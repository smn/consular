import json

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.trial.unittest import TestCase
from twisted.web.server import NOT_DONE_YET

from txfake import FakeHttpServer
from txfake.fake_connection import wait0

from consular.clients import HTTPError, JsonClient, MarathonClient


class JsonClientTestBase(TestCase):
    def setUp(self):
        self.client = self.get_client()
        self.requests = DeferredQueue()
        self.fake_server = FakeHttpServer(self.handle_request)

        self.client.agent = self.fake_server.get_agent()

    def handle_request(self, request):
        self.requests.put(request)
        return NOT_DONE_YET

    def get_client(self):
        """To be implemented by subclass"""
        raise NotImplementedError()

    def write_json_response(self, request, json_data, response_code=200,
                            headers={'Content-Type': 'application/json'}):
        request.setResponseCode(response_code)
        for name, value in headers.items():
            request.setHeader(name, value)
        request.write(json.dumps(json_data))
        request.finish()

    def uri(self, path):
        return '%s%s' % (self.client.endpoint, path,)


class JsonClientTest(JsonClientTestBase):

    def get_client(self):
        return JsonClient('http://localhost:8000')

    @inlineCallbacks
    def test_request(self):
        """
        When a request is made, it should be made with the correct method,
        address and headers, and should contain an empty body. The response
        should be returned.
        """
        d = self.client.request('GET', '/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))
        self.assertEqual(request.getHeader('content-type'), 'application/json')
        self.assertEqual(request.getHeader('accept'), 'application/json')
        self.assertEqual(request.content.read(), '')

        request.setResponseCode(200)
        request.write('hi\n')
        request.finish()

        response = yield d
        text = yield response.text()
        self.assertEqual(text, 'hi\n')

    @inlineCallbacks
    def test_request_json_data(self):
        """
        When a request is made with the json_data parameter set, that data
        should be sent as JSON.
        """
        d = self.client.request('GET', '/hello', json_data={'test': 'hello'})

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))
        self.assertEqual(json.load(request.content), {'test': 'hello'})

        request.setResponseCode(200)
        request.finish()

        yield d

    @inlineCallbacks
    def test_request_endpoint(self):
        """
        When a request is made with the endpoint parameter set, that parameter
        should be used as the endpoint.
        """
        d = self.client.request('GET', '/hello',
                                endpoint='http://localhost:9000')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, 'http://localhost:9000/hello')

        request.setResponseCode(200)
        request.finish()

        yield d

    @inlineCallbacks
    def test_get_json(self):
        """
        When the get_json method is called, a GET request should be made and
        the response should be deserialized from JSON.
        """
        d = self.client.get_json('/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))

        request.setResponseCode(200)
        request.write(json.dumps({'test': 'hello'}))
        request.finish()

        res = yield d
        self.assertEqual(res, {'test': 'hello'})

    @inlineCallbacks
    def test_client_error_response(self):
        """
        When a request is made and a 4xx response code is returned, a HTTPError
        should be raised to indicate a client error.
        """
        d = self.client.request('GET', '/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))

        request.setResponseCode(403)
        request.write('Unauthorized\n')
        request.finish()

        yield wait0()
        failure = self.failureResultOf(d, HTTPError)
        self.assertEqual(
            failure.getErrorMessage(),
            '403 Client Error for url: %s' % self.uri('/hello'))

    @inlineCallbacks
    def test_server_error_response(self):
        """
        When a request is made and a 5xx response code is returned, a HTTPError
        should be raised to indicate a server error.
        """
        d = self.client.request('GET', '/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))

        request.setResponseCode(502)
        request.write('Bad gateway\n')
        request.finish()

        yield wait0()
        failure = self.failureResultOf(d, HTTPError)
        self.assertEqual(
            failure.getErrorMessage(),
            '502 Server Error for url: %s' % self.uri('/hello'))


class MarathonClientTest(JsonClientTestBase):
    def get_client(self):
        return MarathonClient('http://localhost:8080')

    @inlineCallbacks
    def test_get_json_field(self):
        """
        When get_json_field is used to make a request, the response is
        deserialized from JSON and the value of the specified field is
        returned.
        """
        d = self.client.get_json_field('/my-path', 'field-key')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/my-path'))

        self.write_json_response(request, {
            'field-key': 'field-value',
            'other-field-key': 'do-not-care'
        })

        res = yield d
        self.assertEqual(res, 'field-value')

    @inlineCallbacks
    def test_get_json_field_missing(self):
        """
        When get_json_field is used to make a request, the response is
        deserialized from JSON and if the specified field is missing, an error
        is raised.
        """
        d = self.client.get_json_field('/my-path', 'field-key')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/my-path'))

        self.write_json_response(request, {'other-field-key': 'do-not-care'})

        yield wait0()
        failure = self.failureResultOf(d, KeyError)
        self.assertEqual(
            failure.getErrorMessage(),
            '\'Unable to get value for "field-key" from Marathon response: '
            '"{"other-field-key": "do-not-care"}"\'')

    @inlineCallbacks
    def test_get_event_subscription(self):
        """
        When we request event subscriptions from Marathon, we should receive a
        list of callback URLs.
        """
        d = self.client.get_event_subscriptions()

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/v2/eventSubscriptions'))

        self.write_json_response(request, {
            'callbackUrls': [
                'http://localhost:7000/events?registration=localhost'
            ]
        })

        res = yield d
        self.assertEqual(res, [
            'http://localhost:7000/events?registration=localhost'
        ])

    @inlineCallbacks
    def test_post_event_subscription(self):
        """
        When we post an event subscription with a callback URL, we should
        return True for a 200/OK response from Marathon.
        """
        d = self.client.post_event_subscription(
            'http://localhost:7000/events?registration=localhost')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'POST')
        self.assertEqual(request.path, self.uri('/v2/eventSubscriptions'))
        self.assertEqual(request.args, {
            'callbackUrl': [
                'http://localhost:7000/events?registration=localhost'
            ]
        })

        self.write_json_response(request, {
            # TODO: Add check that callbackUrl is correct
            'callbackUrl':
                'http://localhost:7000/events?registration=localhost',
            'clientIp': '0:0:0:0:0:0:0:1',
            'eventType': 'subscribe_event'
        })

        res = yield d
        self.assertEqual(res, True)

    @inlineCallbacks
    def test_post_event_subscription_not_ok(self):
        """
        When we post an event subscription with a callback URL, we should
        return False for a non-200/OK response from Marathon.
        """
        d = self.client.post_event_subscription(
            'http://localhost:7000/events?registration=localhost')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'POST')
        self.assertEqual(request.path, self.uri('/v2/eventSubscriptions'))
        self.assertEqual(request.args, {
            'callbackUrl': [
                'http://localhost:7000/events?registration=localhost'
            ]
        })

        self.write_json_response(request, {}, response_code=201)

        res = yield d
        self.assertEqual(res, False)

    @inlineCallbacks
    def test_get_apps(self):
        """
        When we request the list of apps from Marathon, we should receive the
        list of apps with some information.
        """
        d = self.client.get_apps()

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/v2/apps'))

        apps = {
            'apps': [
                {
                    'id': '/product/us-east/service/myapp',
                    'cmd': 'env && sleep 60',
                    'constraints': [
                        [
                            'hostname',
                            'UNIQUE',
                            ''
                        ]
                    ],
                    'container': None,
                    'cpus': 0.1,
                    'env': {
                        'LD_LIBRARY_PATH': '/usr/local/lib/myLib'
                    },
                    'executor': '',
                    'instances': 3,
                    'mem': 5.0,
                    'ports': [
                        15092,
                        14566
                    ],
                    'tasksRunning': 0,
                    'tasksStaged': 1,
                    'uris': [
                        'https://raw.github.com/mesosphere/marathon/master/'
                        'README.md'
                    ],
                    'version': '2014-03-01T23:42:20.938Z'
                }
            ]
        }
        self.write_json_response(request, apps)

        res = yield d
        self.assertEqual(res, apps['apps'])

    @inlineCallbacks
    def test_get_app(self):
        """
        When we request information on a specific app from Marathon, we should
        receive information on that app.
        """
        d = self.client.get_app('/my-app')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/v2/apps/my-app'))

        app = {
            'app': {
                'args': None,
                'backoffFactor': 1.15,
                'backoffSeconds': 1,
                'maxLaunchDelaySeconds': 3600,
                'cmd': 'python toggle.py $PORT0',
                'constraints': [],
                'container': None,
                'cpus': 0.2,
                'dependencies': [],
                'deployments': [
                    {
                        'id': '44c4ed48-ee53-4e0f-82dc-4df8b2a69057'
                    }
                ],
                'disk': 0.0,
                'env': {},
                'executor': '',
                'healthChecks': [
                    {
                        'command': None,
                        'gracePeriodSeconds': 5,
                        'intervalSeconds': 10,
                        'maxConsecutiveFailures': 3,
                        'path': '/health',
                        'portIndex': 0,
                        'protocol': 'HTTP',
                        'timeoutSeconds': 10
                    },
                    {
                        'command': None,
                        'gracePeriodSeconds': 5,
                        'intervalSeconds': 10,
                        'maxConsecutiveFailures': 6,
                        'path': '/machinehealth',
                        'overridePort': 3333,
                        'protocol': 'HTTP',
                        'timeoutSeconds': 10
                    }
                ],
                'id': '/my-app',
                'instances': 2,
                'mem': 32.0,
                'ports': [
                    10000
                ],
                'requirePorts': False,
                'storeUrls': [],
                'upgradeStrategy': {
                    'minimumHealthCapacity': 1.0
                },
                'uris': [
                    'http://downloads.mesosphere.com/misc/toggle.tgz'
                ],
                'user': None,
                'version': '2014-09-12T23:28:21.737Z',
                'versionInfo': {
                    'lastConfigChangeAt': '2014-09-11T02:26:01.135Z',
                    'lastScalingAt': '2014-09-12T23:28:21.737Z'
                }
            }
        }
        self.write_json_response(request, app)

        res = yield d
        self.assertEqual(res, app['app'])

    @inlineCallbacks
    def test_get_app_tasks(self):
        """
        When we request the list of tasks for an app from Marathon, we should
        receive a list of app tasks.
        """
        d = self.client.get_app_tasks('/my-app')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/v2/apps/my-app/tasks'))

        tasks = {
            'tasks': [
                {
                    'host': 'agouti.local',
                    'id': 'my-app_1-1396592790353',
                    'ports': [
                        31336,
                        31337
                    ],
                    'stagedAt': '2014-04-04T06:26:30.355Z',
                    'startedAt': '2014-04-04T06:26:30.860Z',
                    'version': '2014-04-04T06:26:23.051Z'
                },
                {
                    'host': 'agouti.local',
                    'id': 'my-app_0-1396592784349',
                    'ports': [
                        31382,
                        31383
                    ],
                    'stagedAt': '2014-04-04T06:26:24.351Z',
                    'startedAt': '2014-04-04T06:26:24.919Z',
                    'version': '2014-04-04T06:26:23.051Z'
                }
            ]
        }
        self.write_json_response(request, tasks)

        res = yield d
        self.assertEqual(res, tasks['tasks'])
