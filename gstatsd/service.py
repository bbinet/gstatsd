
# standard
import os
import resource
import signal
import string
import sys
import time
import traceback
from collections import defaultdict

# local
from gstatsd.sink import SinkManager
from gstatsd.config import StatsConfig

# vendor
import gevent
import gevent.socket
socket = gevent.socket
# protect stats
from gevent.thread import allocate_lock as Lock
stats_lock = Lock()

# constants
MAX_PACKET = 2048

# table to remove invalid characters from keys
ALL_ASCII = set(chr(c) for c in range(256))
KEY_VALID = string.ascii_letters + string.digits + '_-.'
KEY_TABLE = string.maketrans(KEY_VALID + '/', KEY_VALID + '_')
KEY_DELETIONS = ''.join(ALL_ASCII.difference(KEY_VALID + '/'))

# error messages
E_BADADDR = 'invalid bind address specified %r'
E_NOSINKS = 'you must specify at least one stats sink'


class Stats(object):

    def __init__(self):
        self.timers = defaultdict(list)
        self.timers_stats = defaultdict(dict)
        self.counts = defaultdict(float)
        self.gauges = defaultdict(float)
        self.proxy_values = defaultdict(list)
        self.percent = None
        self.interval = None


def daemonize(umask=0027):
    if gevent.fork():
        os._exit(0)
    os.setsid()
    if gevent.fork():
        os._exit(0)
    os.umask(umask)
    fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if fd_limit == resource.RLIM_INFINITY:
        fd_limit = 1024
    for fd in xrange(0, fd_limit):
        try:
            os.close(fd)
        except:
            pass
    os.open(os.devnull, os.O_RDWR)
    os.dup2(0, 1)
    os.dup2(0, 2)
    gevent.reinit()


class StatsDaemon(object):

    """
    A statsd service implementation in Python + gevent.
    """

    def __init__(self, cfg, debug=False):
        self.load_config(cfg, debug)

    def load_config(self, cfg, debug=False):
        if not cfg.sinks:
            self.exit(E_NOSINKS)
        self._bindaddr = (cfg.host, int(cfg.port))
        self._sink = SinkManager(cfg.sinks)
        self._percent = float(cfg.threshold)
        self._interval = float(cfg.flush_interval)
        self._debug = debug
        self._sock = None
        self._flush_task = None
        self._key_prefix = cfg.prefix
        if self._debug:
            print cfg.dump_yml()

        self._reset_stats()

    def _reset_stats(self):
        with stats_lock:
            self._stats = Stats()
            self._stats.percent = self._percent
            self._stats.interval = self._interval

    def exit(self, msg, code=1):
        self.error(msg)
        sys.exit(code)

    def error(self, msg):
        sys.stderr.write(msg + '\n')

    def start(self):
        "Start the service"
        # register signals
        gevent.signal(signal.SIGINT, self._shutdown)

        # spawn the flush trigger
        def _flush_impl():
            while 1:
                gevent.sleep(self._stats.interval)

                # rotate stats
                stats = self._stats
                self._reset_stats()

                # send the stats to the sink which in turn broadcasts
                # the stats packet to one or more hosts.
                try:
                    self._sink.send(stats)
                except Exception:
                    trace = traceback.format_tb(sys.exc_info()[-1])
                    self.error(''.join(trace))

        self._flush_task = gevent.spawn(_flush_impl)

        # start accepting connections
        self._sock = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._sock.bind(self._bindaddr)
        while 1:
            try:
                data, _ = self._sock.recvfrom(MAX_PACKET)
                for p in data.split('\n'):
                    if p:
                        self._process(p)
            except Exception, ex:
                self.error(str(ex))

    def _shutdown(self):
        "Shutdown the server"
        self.exit("service exiting", code=0)

    def _process(self, data):
        "Process a single packet and update the internal tables."
        now = int(time.time() * 1000)
        parts = data.split(':')
        if self._debug:
            self.error('packet: %r' % data)
        if not parts:
            return

        # interpret the packet and update stats
        stats = self._stats
        key = parts[0].translate(KEY_TABLE, KEY_DELETIONS)
        if self._key_prefix:
            key = '.'.join([self._key_prefix, key])
        for part in parts[1:]:
            srate = 1.0
            fields = part.split('|')
            length = len(fields)
            if length < 2:
                continue
            value = fields[0]
            stype = fields[1].strip()

            with stats_lock:
                # timer (milliseconds)
                if stype == 'ms':
                    stats.timers[key].append(float(value if value else 0))
                # counter with optional sample rate
                elif stype == 'c':
                    if length == 3 and fields[2].startswith('@'):
                        srate = float(fields[2][1:])
                    value = float(value if value else 1) * (1 / srate)
                    stats.counts[key] += value
                # gauge
                elif stype == 'g':
                    value = float(value if value else 1)
                    stats.gauges[key] = value
                # proxy
                elif stype == 'p':
                    stats.proxy_values[key].append((now, value))


def main():
    parser = StatsConfig.get_optionparser()
    options, args = parser.parse_args()
    if options.usage:
        print(parser.format_help())
        sys.exit()

    cfg = StatsConfig(*(args + [options]))

    if cfg.daemonize:
        daemonize()

    sd = StatsDaemon(cfg)
    sd.start()


if __name__ == '__main__':
    main()
