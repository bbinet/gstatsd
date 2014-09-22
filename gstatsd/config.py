import optparse
import re

from gstatsd.core import __version__


class ProxyConfig(object):

    def __init__(self, cfg, statscfg):
        self.name = cfg['name']
        self.regex = re.compile('^%s$' % self.name)
        self.allow = cfg.get('allow', True)
        self.interval = float(cfg.get('interval', statscfg.flush_interval))
        self.min_hour = int(cfg.get('min_hour', 0))
        self.max_hour = int(cfg.get('max_hour', 24))
        self.aggregate = cfg.get('aggregate', 'average')
        self.key = cfg.get('key', '%(uuid)s.%(hostname)s.%(name)s')
        assert self.allow in (True, False)
        assert self.aggregate in (
            'average', 'sum', 'min', 'max', 'last', 'gust')


class StatsConfig(object):
    props = ['uuid', 'host', 'port', 'verbose', 'flush_interval', 'prefix',
             'threshold', 'daemonize', 'numstats']
    # default config
    uuid = 'stats'
    host = 'localhost'
    port = 8125
    verbose = False
    flush_interval = 10
    prefix = None
    threshold = 90
    daemonize = False
    numstats = False

    def __init__(self, *args):
        self.sinks = []
        self.proxy = []
        for arg in args:
            if isinstance(arg, basestring):
                self.parse_yml(arg)
            elif isinstance(arg, dict):
                self.parse_dict(arg)
            elif isinstance(arg, optparse.Values):
                self.parse_options(arg)
            else:
                raise Exception('wrong type: %r' % arg)

    @classmethod
    def get_optionparser(cls):
        parser = optparse.OptionParser(
            description="A statsd service in Python + gevent.",
            usage="Usage: gstatsd [options] [config files]",
            version=__version__,
            add_help_option=False)
        parser.add_option(
            '-l', '--listen-host', dest='host',
            help="host to listen to (default '%s')" % cls.host)
        parser.add_option(
            '-p', '--listen-port', dest='port',
            help="port to listen to (default %d)" % cls.port)
        parser.add_option(
            '-s', '--sink', dest='sinks', action='append', default=[],
            help="a service to which stats are sent ([[host:]port:]type"
            "[,backend options]). Supported types are \"graphite\" and "
            "\"influxdb\".\nInfluxDB backend needs database, user, and "
            "password options, for example:\n-s influxdb,mydb,myuser,mypass")
        parser.add_option(
            '-f', '--flush-interval', dest='flush_interval',
            help="flush interval, in seconds (default %d)"
            % cls.flush_interval)
        parser.add_option(
            '-u', '--uuid', dest='uuid',
            help="uuid for this gstatsd instance (default %r)" % cls.uuid)
        parser.add_option(
            '-x', '--prefix', dest='prefix',
            help="key prefix added to all keys (default %r)" % cls.prefix)
        parser.add_option(
            '-t', '--threshold', dest='threshold',
            help="percent threshold (default %d)" % cls.threshold)
        parser.add_option(
            '-D', '--daemonize', dest='daemonize', action='store_true',
            help='daemonize the service')
        parser.add_option(
            '-n', '--numstats', dest='numstats', action='store_true',
            help='send statsd numstats value to all sinks')
        parser.add_option(
            '-v', '--verbose', dest='verbose', action='count',
            help="increase verbosity (currently used for debugging)")
        parser.add_option(
            '-h', '--help', dest='usage', action='store_true',
            help="display this message")
        return parser

    def parse_options(self, options):
        sinks = getattr(options, 'sinks', None)
        if sinks is not None:
            self.sinks += sinks
        for prop in self.props:
            val = getattr(options, prop, None)
            if val is not None:
                setattr(self, prop, val)

    def parse_yml(self, path):
        import yaml
        with open(path, 'r') as f:
            self.parse_dict(yaml.load(f))

    def parse_dict(self, d):
        if 'sinks' in d:
            self.sinks += d['sinks']
        if 'proxy' in d:
            self.proxy = [ProxyConfig(p, self) for p in d['proxy']]
        for prop in self.props:
            val = d.get(prop, None)
            if val is not None:
                setattr(self, prop, val)

    def dump_yml(self):
        import yaml
        d = {
            'sinks': self.sinks,
            }
        for prop in self.props:
            d[prop] = getattr(self, prop)
        return yaml.dump(d)
