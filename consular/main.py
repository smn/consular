import json

from consular.clients import ConsulClient, MarathonClient, HTTPError

from twisted.internet import reactor
from twisted.web import server
from twisted.internet.defer import succeed, inlineCallbacks, returnValue
from twisted.internet.task import LoopingCall
from twisted.web.http import NOT_FOUND
from twisted.python import log

from klein import Klein


def get_app_name(app_id):
    """
    Get the app name from the marathon app ID. Separators in the ID ('/') are
    replaced with '-'s while the leading separator is removed.
    """
    return app_id.lstrip('/').replace('/', '-')


@inlineCallbacks
def handle_not_found_error(f, *args, **kwargs):
    """
    Perform a request and catch the not found (404) error if one occurs.

    :param: f: The function to call to perform the request. The function may
        return a deferred.
    :param: args: The arguments to call the function with.
    :param: kwargs: The keyword arguments to call the function with.
    :returns: The return value of the function call or None if there was a 404
        response code.
    """
    try:
        response = yield f(*args, **kwargs)
    except HTTPError as e:
        if e.response.code == NOT_FOUND:
            response = None
        else:
            raise e
    returnValue(response)


class ConsularSite(server.Site):

    debug = False

    def log(self, request):
        if self.debug:
            server.Site.log(self, request)


class Consular(object):
    """
    :param str consul_endpoint:
        The HTTP endpoint for Consul (often http://example.org:8500).
    :param str marathon_endpoint:
        The HTTP endpoint for Marathon (often http://example.org:8080).
    :param bool enable_fallback:
        Fallback to the main Consul endpoint for registrations if unable
        to reach Consul running on the machine running a specific Marathon
        task.
    :param str registration_id:
        A unique parameter for this Consul server. It is used for house-keeping
        purposes such as purging tasks that are no longer running in Marathon.
    """

    app = Klein()
    _debug = False
    clock = reactor

    def __init__(self, consul_endpoint, marathon_endpoint, enable_fallback,
                 registration_id):
        self.consul_client = ConsulClient(consul_endpoint, enable_fallback)
        self.marathon_client = MarathonClient(marathon_endpoint)
        self.registration_id = registration_id
        self.event_dispatch = {
            'status_update_event': self.handle_status_update_event,
        }

    def set_debug(self, debug):
        self._debug = debug
        self.consul_client.debug = debug
        self.marathon_client.debug = debug

    def set_timeout(self, timeout):
        self.consul_client.timeout = timeout
        self.marathon_client.timeout = timeout

    def set_requester(self, requester):
        self.consul_client.requester = requester
        self.marathon_client.requester = requester

    def run(self, host, port):
        """
        Starts the HTTP server.

        :param str host:
            The host to bind to (example is ``localhost``)
        :param int port:
            The port to listen on (example is ``7000``)
        """
        site = ConsularSite(self.app.resource())
        site.debug = self._debug
        self.clock.listenTCP(port, site, interface=host)

    def schedule_sync(self, interval, purge=False):
        """
        Schedule a recurring sync of apps, starting after this method is
        called.

        :param float interval:
            The number of seconds between syncs.
        :param bool purge:
            Whether to purge old apps after each sync.
        :return:
            A tuple of the LoopingCall object and the deferred created when it
            was started.
        """
        lc = LoopingCall(self._try_sync_apps, purge)
        lc.clock = self.clock
        return (lc, lc.start(interval, now=True))

    @inlineCallbacks
    def _try_sync_apps(self, purge=False):
        """
        Sync the apps, catching and logging any exception that occurs.
        """
        try:
            yield self.sync_apps(purge)
        except Exception as e:
            # TODO: More specialised exception handling.
            log.msg('Error syncing apps: %s' % e)

    @inlineCallbacks
    def register_marathon_event_callback(self, events_url):
        """
        Register Consular with Marathon to receive HTTP event callbacks.
        To use this ensure that `Marathon is configured`_ to send HTTP event
        callbacks for state changes in tasks.

        :param str events_url:
            The HTTP endpoint to register with Marathon for event callbacks.

        .. _`Marathon is configured`:
            https://mesosphere.github.io/marathon/docs/event-bus.html
            #configuration
        """
        existing_callbacks = (
            yield self.marathon_client.get_event_subscriptions())
        already_registered = any(
            [events_url == url for url in existing_callbacks])
        if already_registered:
            log.msg('Consular event callback already registered.')
            returnValue(True)

        registered = (
            yield self.marathon_client.post_event_subscription(events_url))
        if registered:
            log.msg('Consular event callback registered.')
        else:
            log.err('Consular event callback registration failed.')
        returnValue(registered)

    @app.route('/')
    def index(self, request):
        request.setHeader('Content-Type', 'application/json')
        return json.dumps([])

    @app.route('/events')
    def events(self, request):
        """
        Listens to incoming events from Marathon on ``/events``.

        :param klein.app.KleinRequest request:
            The Klein HTTP request
        """
        request.setHeader('Content-Type', 'application/json')
        event = json.load(request.content)
        handler = self.event_dispatch.get(
            event.get('eventType'), self.handle_unknown_event)
        return handler(request, event)

    def handle_status_update_event(self, request, event):
        """
        Handles status updates from Marathon.

        The various task stages are handled as follows:

        TASK_STAGING: ignored
        TASK_STARTING: ignored
        TASK_RUNNING: task data updated on Consul
        TASK_FINISHED: task data removed from Consul
        TASK_FAILED: task data removed from Consul
        TASK_KILLED: task data removed from Consul
        TASK_LOST: task data removed from Consul

        :param klein.app.KleinRequest request:
            The Klein HTTP request
        :param dict event:
            The Marathon event
        """
        dispatch = {
            'TASK_STAGING': self.noop,
            'TASK_STARTING': self.noop,
            'TASK_RUNNING': self.update_task_running,
            'TASK_FINISHED': self.update_task_killed,
            'TASK_FAILED': self.update_task_killed,
            'TASK_KILLED': self.update_task_killed,
            'TASK_LOST': self.update_task_killed,
        }
        handler = dispatch.get(event['taskStatus'])
        return handler(request, event)

    def noop(self, request, event):
        return succeed(json.dumps({
            'status': 'ok'
        }))

    @inlineCallbacks
    def update_task_running(self, request, event):
        """ Use a running event to register a new Consul service. """
        # Register the task as a service
        yield self.register_task_service(
            event['appId'], event['taskId'], event['host'], event['ports'])

        # Sync the app labels in case they've changed or aren't stored yet
        app = yield handle_not_found_error(
            self.marathon_client.get_app, event['appId'])

        # The app could have disappeared in this time if it was destroyed. If
        # it has been destroyed, do nothing and wait for the TASK_KILLED event
        # to clear it.
        if app is not None:
            yield self.sync_app_labels(app)
        else:
            log.msg('Warning. App with ID "%s" could not be found for new '
                    'task with ID "%s"' % (event['appId'], event['taskId'],))

        returnValue(json.dumps({'status': 'ok'}))

    def update_task_killed(self, request, event):
        d = self.deregister_task_service(event['taskId'], event['host'])
        d.addCallback(lambda _: json.dumps({'status': 'ok'}))
        return d

    def handle_unknown_event(self, request, event):
        event_type = event.get('eventType')
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(400)  # bad request
        log.msg('Not handling event type: %s' % (event_type,))
        return json.dumps({
            'error': 'Event type %s not supported.' % (event_type,)
        })

    def reg_id_tag(self):
        """ Get the registration ID tag for this instance of Consular. """
        return self._consular_tag('reg-id', self.registration_id)

    def app_id_tag(self, app_id):
        """ Get the app ID tag for the given app ID. """
        return self._consular_tag('app-id', app_id)

    def _consular_tag(self, tag_name, value):
        return self._consular_tag_key(tag_name) + value

    def get_app_id_from_tags(self, tags):
        """
        Get the app ID from the app ID tag in the given tags, or None if the
        tag could not be found.
        """
        return self._find_consular_tag(tags, 'app-id')

    def _find_consular_tag(self, tags, tag_name):
        pseudo_key = self._consular_tag_key(tag_name)
        matches = [tag for tag in tags if tag.startswith(pseudo_key)]

        if not matches:
            return None
        if len(matches) > 1:
            raise RuntimeError('Multiple (%d) Consular tags found for key '
                               '"%s": %s'
                               % (len(matches), pseudo_key, matches,))

        return matches[0].lstrip(pseudo_key)

    def _consular_tag_key(self, tag_name):
        return 'consular-%s=' % (tag_name,)

    def _create_service_registration(self, app_id, service_id, address, port):
        """
        Create the request body for registering a service with Consul.
        """
        registration = {
            'Name': get_app_name(app_id),
            'ID': service_id,
            'Address': address,
            'Tags': [
                self.reg_id_tag(),
                self.app_id_tag(app_id),
            ]
        }
        if port is not None:
            registration['Port'] = port

        return registration

    def register_task_service(self, app_id, task_id, host, ports):
        """
        Register a Marathon task as a service in Consul.

        :param str app_id:
            The ID of the Marathon app that the task belongs to.
        :param str task_id:
            The ID of the task, this will be used as the Consul service ID.
        :param str host:
            The host address of the machine the task is running on.
        :param list ports:
            The port numbers the task can be accessed on on the host machine.
        """
        if not ports:
            port = None
        elif len(ports) == 1:
            [port] = ports
        else:
            # TODO: Support multiple ports (issue #29)
            port = min(ports)
            log.msg('Warning. %d ports found for app "%s". Consular currently '
                    'only supports a single port. Only the lowest port (%s) '
                    'will be used.' % (len(ports), app_id, port,))

        log.msg('Registering %s at %s with %s at %s:%s.' % (
            app_id, host, task_id, host, port))
        registration = self._create_service_registration(app_id, task_id,
                                                         host, port)

        return self.consul_client.register_agent_service(
            host, registration)

    def deregister_task_service(self, task_id, host):
        """
        Deregister a Marathon task's service from Consul.

        :param str task_id:
            The ID of the task, this will be used as the Consul service ID.
        :param str host:
            The host address of the machine the task is running on.
        """
        return self.deregister_consul_service(host, task_id)

    def deregister_consul_service(self, agent_endpoint, service_id):
        """
        Deregister a service from a Consul agent.

        :param str agent_endpoint:
            The HTTP endpoint of the Consul agent.
        :param str service_id:
            The ID of the Consul service to be deregistered.
        """
        log.msg('Deregistering service with ID "%s" at Consul endpoint %s ' % (
            service_id, agent_endpoint,))
        return self.consul_client.deregister_agent_service(
            agent_endpoint, service_id)

    @inlineCallbacks
    def sync_apps(self, purge=False):
        """
        Ensure all the apps in Marathon are registered as services
        in Consul.

        Set ``purge`` to ``True`` if you automatically want services in Consul
        that aren't registered in Marathon to be purged. Consular only purges
        services that have been registered with the same ``registration-id``.

        :param bool purge:
            To purge or not to purge.
        """
        log.msg('Syncing apps')
        apps = yield self.marathon_client.get_apps()

        self.check_apps_namespace_clash(apps)

        for app in apps:
            yield self.sync_app(app)

        if purge:
            log.msg('Purging dead apps')
            yield self.purge_dead_apps(apps)

    def check_apps_namespace_clash(self, apps):
        """
        Checks if app names in Marathon will cause a namespace clash in Consul.
        Throws an exception if there is a collision, else returns the apps.

        :param: apps:
            The JSON list of apps from Marathon's API.
        """
        # Collect the app name to app id(s) mapping.
        name_ids = {}
        for app in apps:
            app_id = app['id']
            app_name = get_app_name(app_id)
            name_ids.setdefault(app_name, []).append(app_id)

        # Check if any app names map to more than one app id.
        collisions = {name: ids
                      for name, ids in name_ids.items() if len(ids) > 1}

        if collisions:
            collisions_string = '\n'.join(sorted(
                ['%s => %s' % (name, ', '.join(ids),)
                 for name, ids in collisions.items()]))
            raise RuntimeError(
                'The following Consul service name(s) will resolve to '
                'multiple Marathon app names: \n%s' % (collisions_string,))

        return apps

    @inlineCallbacks
    def sync_app(self, app):
        yield self.sync_app_labels(app)
        yield self.sync_app_tasks(app)

    @inlineCallbacks
    def purge_dead_apps(self, apps):
        yield self.purge_dead_services()
        yield self.purge_dead_app_labels(apps)

    @inlineCallbacks
    def sync_app_labels(self, app):
        """
        Sync the app labels for the given app by pushing its labels to the
        Consul k/v store and cleaning any labels there that are no longer
        present.

        :param: app:
            The app JSON as return by the Marathon HTTP API.
        """
        # NOTE: KV requests can go straight to the consul registry
        #       we're already connected to, they're not local to the agents.
        app_name = get_app_name(app['id'])
        labels = app.get('labels', {})
        yield self.put_consul_app_labels(app_name, labels)
        yield self.clean_consul_app_labels(app_name, labels)

    def put_consul_app_labels(self, app_name, labels):
        """
        Store the given set of labels under the given app name in the Consul
        k/v store.
        """
        return self.put_consul_kvs({'consular/%s/%s' % (app_name, key,): value
                                    for key, value in labels.items()})

    @inlineCallbacks
    def put_consul_kvs(self, key_values):
        """ Store the given key/value set in the Consul k/v store. """
        for key, value in key_values.items():
            yield self.consul_client.put_kv(key, value)

    @inlineCallbacks
    def clean_consul_app_labels(self, app_name, labels):
        """
        Delete app labels stored in the Consul k/v store under the given app
        name that aren't present in the given set of labels.
        """
        # Get the existing labels from Consul
        if self._debug:
            log.msg('Cleaning labels no longer in use by app "%s"' % app_name)

        keys = yield handle_not_found_error(self.get_consul_app_keys, app_name)
        if keys is None:
            log.msg('No keys found in Consul for service "%s"' % app_name)
            return

        if self._debug:
            log.msg('%d labels stored in Marathon, %d keys found in Consul '
                    'for app "%s"' % (len(labels), len(keys), app_name))

        # Filter out the Marathon labels
        keys = self._filter_marathon_labels(keys, labels)

        if self._debug:
            log.msg('%d keys to be deleted from Consul for app %s' % (
                len(keys), app_name))

        # Delete the non-existant keys
        for key in keys:
            yield self.consul_client.delete_kv_keys(key)

    def get_consul_app_keys(self, app_name):
        """ Get the Consul k/v keys for the app with the given name. """
        return self.consul_client.get_kv_keys('consular/%s' % (app_name,))

    def get_consul_consular_keys(self):
        """
        Get the next level of Consul k/v keys at 'consular/', i.e. will
        return 'consular/my-app' but not 'consular/my-app/my-label'.
        """
        return self.consul_client.get_kv_keys('consular/', separator='/')

    def _filter_marathon_labels(self, consul_keys, marathon_labels):
        """
        Takes a list of Consul keys and removes those with keys not found in
        the given dict of Marathon labels.

        :param: consul_keys:
            The list of Consul keys as returned by the Consul API.
        :param: marathon_labels:
            The dict of Marathon labels as returned by the Marathon API.
        """
        label_key_set = set(marathon_labels.keys())
        return [key for key in consul_keys
                if (self._consul_key_to_marathon_label_key(key)
                    not in label_key_set)]

    def _consul_key_to_marathon_label_key(self, consul_key):
        """
        Trims the 'consular/<app_name>/' from the front of the key path to get
        the Marathon label key.
        """
        return consul_key.split('/', 2)[-1]

    @inlineCallbacks
    def sync_app_tasks(self, app):
        tasks = yield self.marathon_client.get_app_tasks(app['id'])
        for task in tasks:
            yield self.register_task_service(
                app['id'], task['id'], task['host'], task['ports'])

    @inlineCallbacks
    def purge_dead_app_labels(self, apps):
        """
        Delete any keys stored in the Consul k/v store that belong to apps that
        no longer exist.

        :param: apps:
            The list of apps as returned by the Marathon API.
        """
        log.msg('Purging dead app labels')
        # Get the existing keys
        keys = yield handle_not_found_error(self.get_consul_consular_keys)
        if keys is None:
            log.msg('No Consular keys found in Consul')
            return

        if self._debug:
            log.msg('Got %d keys from Consul' % len(keys))

        # Filter the present apps out
        keys = self._filter_marathon_apps(keys, apps)

        if self._debug:
            log.msg('After filtering out running apps, %d Consul keys remain '
                    'to be purged' % len(keys))

        # Delete the remaining keys
        for key in keys:
            yield self.consul_client.delete_kv_keys(key, recurse=True)

    def _filter_marathon_apps(self, consul_keys, marathon_apps):
        """
        Takes a list of Consul keys and removes those with keys not found in
        the given list of Marathon apps.

        :param: consul_keys:
            The list of Consul keys as returned by the Consul API.
        :param: marathon_apps:
            The list of apps as returned by the Marathon API.
        """
        app_name_set = set([get_app_name(app['id']) for app in marathon_apps])
        return [key for key in consul_keys
                if (self._consul_key_to_marathon_app_name(key)
                    not in app_name_set)]

    def _consul_key_to_marathon_app_name(self, consul_key):
        """
        Trims the 'consular/' from the front of the key path to get the
        Marathon app name.
        """
        return consul_key.split('/', 1)[-1].strip('/')

    @inlineCallbacks
    def purge_dead_services(self):
        nodes = yield self.consul_client.get_catalog_nodes()
        for node in nodes:
            self.purge_dead_agent_services(node['Address'])

    @inlineCallbacks
    def purge_dead_agent_services(self, agent_endpoint):
        data = yield self.consul_client.get_agent_services(agent_endpoint)

        # collect the task ids for the service name
        services = {}
        for service_id, service in data.items():
            # Check the service for a tag that matches our registration ID
            tags = service['Tags']
            if tags and self.reg_id_tag() in tags:
                app_id = self.get_app_id_from_tags(tags)
                if app_id:
                    services.setdefault(app_id, set()).add(service_id)
                else:
                    log.msg('Service "%s" does not have an app ID in its '
                            'tags, it cannot be purged.'
                            % (service['Service'],))
            elif self._debug:
                log.msg('Service "%s" is not tagged with our registration ID, '
                        'not touching it.' % (service['Service'],))

        for app_id, task_ids in services.items():
            yield self.purge_service_if_dead(agent_endpoint, app_id, task_ids)

    @inlineCallbacks
    def purge_service_if_dead(self, agent_endpoint, app_id, consul_task_ids):
        # Get the running tasks for the app (don't raise an error if the tasks
        # are not found)
        tasks = yield handle_not_found_error(
            self.marathon_client.get_app_tasks, app_id)
        if tasks is None:
            log.msg('No tasks found in Marathon for app ID "%s"' % app_id)
            tasks = []

        # Remove the running tasks from the set of Consul services
        service_ids = self._filter_marathon_tasks(tasks, consul_task_ids)

        # Deregister the remaining old services
        for service_id in service_ids:
            yield self.deregister_consul_service(agent_endpoint, service_id)

    def _filter_marathon_tasks(self, marathon_tasks, consul_service_ids):
        if not marathon_tasks:
            return consul_service_ids

        task_id_set = set([task['id'] for task in marathon_tasks])
        return [service_id for service_id in consul_service_ids
                if service_id not in task_id_set]
