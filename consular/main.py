import json

from urllib import urlencode
from twisted.internet import reactor
from twisted.web.client import HTTPConnectionPool
from twisted.internet.defer import (
    succeed, inlineCallbacks, returnValue, gatherResults)
from twisted.python import log

import treq
from klein import Klein


def get_appid(app_id_string):
    return app_id_string.rsplit('/', 1)[1]


class Consular(object):

    app = Klein()

    def __init__(self, consul_endpoint, marathon_endpoint):
        self.consul_endpoint = consul_endpoint
        self.marathon_endpoint = marathon_endpoint
        self.pool = HTTPConnectionPool(reactor, persistent=False)
        self.event_dispatch = {
            'status_update_event': self.handle_status_update_event,
        }

    def get_marathon_event_callbacks(self):
        d = self.marathon_request('GET', '/v2/eventSubscriptions')
        d.addErrback(log.err)
        d.addCallback(lambda response: response.json())
        d.addCallback(lambda data: data['callbackUrls'])
        return d

    def create_marathon_event_callback(self, url):
        d = self.marathon_request(
            'POST', '/v2/eventSubscriptions?%s' % urlencode({
                'callbackUrl': url,
            }))
        d.addErrback(log.err)
        d.addCallback(lambda response: response.code == 200)
        return d

    @inlineCallbacks
    def register_marathon_event_callback(self, events_url):
        existing_callbacks = yield self.get_marathon_event_callbacks()
        already_registered = any(
            [events_url == url for url in existing_callbacks])
        if already_registered:
            log.msg('Consular event callback already registered.')
            returnValue(True)

        registered = yield self.create_marathon_event_callback(events_url)
        if registered:
            log.msg('Consular event callback registered.')
        else:
            log.err('Consular event callback registration failed.')
        returnValue(registered)

    def marathon_request(self, method, path, data=None):
        return treq.request(
            method, ('%s%s' % (self.marathon_endpoint, path)).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool)

    def consul_request(self, method, path, data=None):
        return treq.request(
            method, ('%s%s' % (self.consul_endpoint, path)).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool)

    @app.route('/')
    def index(self, request):
        request.setHeader('Content-Type', 'application/json')
        return json.dumps([])

    @app.route('/events')
    def events(self, request):
        request.setHeader('Content-Type', 'application/json')
        event = json.load(request.content)
        handler = self.event_dispatch.get(
            event.get('eventType'), self.handle_unknown_event)
        return handler(request, event)

    def handle_status_update_event(self, request, event):
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

    def update_task_running(self, request, event):
        # NOTE: Marathon sends a list of ports, I don't know yet when & if
        #       there are multiple values in that list.
        d = self.get_app(event['appId'])
        d.addCallback(lambda app: self.sync_app(app))
        d.addCallback(lambda _: json.dumps({'status': 'ok'}))
        return d

    def update_task_killed(self, request, event):
        d = self.consul_request('PUT', '/v1/agent/service/deregister/%s' % (
            event['taskId'],))
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

    def sync_apps(self):
        d = self.marathon_request('GET', '/v2/apps')
        d.addCallback(lambda response: response.json())
        d.addCallback(
            lambda data: gatherResults(
                [self.sync_app(app) for app in data['apps']]))
        return d

    def get_app(self, app_id):
        d = self.marathon_request('GET', '/v2/apps%s' % (app_id,))
        d.addCallback(lambda response: response.json())
        d.addCallback(lambda data: data['app'])
        return d

    def sync_app(self, app):
        app_id = app['id']
        d = self.marathon_request('GET', '/v2/apps%s/tasks' % (app_id,))
        d.addCallback(lambda response: response.json())
        d.addCallback(lambda data: gatherResults(
            self.sync_app_task(app, task) for task in data['tasks']))
        return d

    def sync_app_task(self, app, task):
        return self.consul_request('PUT', '/v1/agent/service/register', {
            'Name': get_appid(app['id']),
            'ID': task['id'],
            'Address': task['host'],
            'Port': task['ports'][0]
        })
