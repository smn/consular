import click
import sys

from urllib import urlencode


@click.command()
@click.option('--scheme', default='http',
              help='The scheme to use. (HTTP/HTTPS)')
@click.option('--host', default='localhost',
              help='The host to bind to.')
@click.option('--port', default='7000', type=int,
              help='The port to listen to.')
@click.option('--consul', default='http://localhost:8500',
              help='The Consul HTTP API. NOTE: Other agents in the cluster '
                   'must use the same scheme and port.')
@click.option('--marathon', default='http://localhost:8080',
              help='The Marathon HTTP API')
@click.option('--registration-id',
              help=('Name used to register for event callbacks in Marathon as '
                    'well as to register services in Consul. Must be unique '
                    'for each consular process.'),
              type=str, default='consular')
@click.option('--sync-interval',
              help=('Automatically sync the apps in Marathon with what\'s '
                    'in Consul every _n_ seconds. Defaults to 0 (disabled).'),
              type=int)
@click.option('--purge/--no-purge',
              help=('Automatically purge dead services from Consul if they '
                    'are not known in Marathon '
                    '(needs sync-interval enabled).'),
              default=False)
@click.option('--logfile',
              help='Where to log output to',
              type=click.File('a'),
              default=sys.stdout)
@click.option('--debug/--no-debug',
              help='Log debug output or not',
              default=False)
@click.option('--timeout',
              help='HTTP API client timeout',
              default=5, type=int)
@click.option('--fallback/--no-fallback',
              help=('Fallback to the default Consul agent for service '
                    'registration if the host running the mesos tasks '
                    'is not running a consul agent. '
                    'ONLY USE IF YOU KNOW WHAT YOU ARE DOING.'),
              default=False)
@click.option('--fallback-timeout',
              help=('How long to wait until assuming there is no consul '
                    'agent running on a mesos-slave machine'),
              default=2, type=int)
def main(scheme, host, port,
         consul, marathon, registration_id,
         sync_interval, purge, logfile, debug, timeout,
         fallback, fallback_timeout):  # pragma: no cover
    from consular.main import Consular
    from twisted.internet import reactor
    from twisted.python import log

    log.startLogging(logfile)

    consular = Consular(consul, marathon, fallback, registration_id)
    consular.set_debug(debug)
    consular.set_timeout(timeout)
    consular.fallback_timeout = fallback_timeout
    events_url = "%s://%s:%s/events?%s" % (
        scheme, host, port,
        urlencode({
            'registration': registration_id,
        }))
    consular.register_marathon_event_callback(events_url)

    if sync_interval > 0:
        consular.schedule_sync(sync_interval, purge)

    consular.run(host, port)
    reactor.run()
