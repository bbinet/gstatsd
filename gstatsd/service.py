# standard
import os
import resource
import signal
import string
import sys
import time
import traceback
from bisect import bisect_left
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

    def __init__(self, interval, percent):
        self.timers = defaultdict(list)
        self.timers_stats = defaultdict(dict)
        self.counts = defaultdict(float)
        self.gauges = defaultdict(float)
        self.proxies = defaultdict(list)
        self.interval = interval
        self.percent = percent


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
        self._stats = None
        self._sock = None
        self._flush_task = None
        self._start_time = None
        self.load_config(cfg, debug)
        self._proxies = defaultdict(lambda: ([], []))

    def load_config(self, cfg, debug=False):
        if not cfg.sinks:
            self.exit(E_NOSINKS)
        self._bindaddr = (cfg.host, int(cfg.port))
        self._sink = SinkManager(cfg.sinks)
        self._percent = float(cfg.threshold)
        self._interval = float(cfg.flush_interval)
        self._debug = debug
        self._key_prefix = cfg.prefix
        self._proxycfg = cfg.proxy
        self._proxycfg_cache = {}
        if self._debug:
            print cfg.dump_yml()

    def _reset_stats(self):
        now = time.time()
        with stats_lock:
            stats = self._stats
            self._stats = Stats(self._interval, self._percent)
            if not stats:
                return
            del_keys = []
            for key, (times, values) in self._proxies.iteritems():
                cfg = self._proxycfg_cache[key]
                i = cfg.interval
                if i <= 0:
                    stats.proxies[key] = zip(times, values)
                    del_keys.append(key)
                    continue
                t = self._start_time + \
                    ((times[0] - self._start_time) // i) * i + i
                while t <= now:
                    idx = bisect_left(times, t)
                    t += i
                    if idx <= 0:
                        continue
                    if cfg.aggregate == 'last':
                        stats.proxies[key].append((t, values[-1]))
                        del values[:idx]
                        del times[:idx]
                        continue
                    bucket = values[:idx]
                    if cfg.aggregate == 'average':
                        stats.proxies[key].append(
                            (t, sum(bucket) / len(bucket)))
                    elif cfg.aggregate == 'sum':
                        stats.proxies[key].append((t, sum(bucket)))
                    elif cfg.aggregate == 'min':
                        stats.proxies[key].append((t, sorted(bucket)[0]))
                    elif cfg.aggregate == 'max':
                        stats.proxies[key].append((t, sorted(bucket)[-1]))
                    del values[:idx]
                    del times[:idx]
                if len(times) == 0:
                    del_keys.append(key)
            for key in del_keys:
                del self._proxies[key]
        return stats

    def exit(self, msg, code=1):
        self.error(msg)
        sys.exit(code)

    def error(self, msg):
        sys.stderr.write(msg + '\n')

    def start(self):
        "Start the service"

        # set start time in microsecs
        self._start_time = time.time()

        # starts with fresh stats
        self._reset_stats()

        # register signals
        gevent.signal(signal.SIGINT, self._shutdown)

        # spawn the flush trigger
        def _flush_impl():
            while 1:
                gevent.sleep(self._stats.interval)

                # rotate stats
                stats = self._reset_stats()

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
                now = time.time()
                for p in data.split('\n'):
                    if p:
                        self._process(p, now)
            except Exception, ex:
                self.error(str(ex))

    def _shutdown(self):
        "Shutdown the server"
        self.exit("service exiting", code=0)

    def _process(self, data, now):
        "Process a single packet and update the internal tables."
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
                    cfg = self._proxycfg_cache.get(key)
                    if cfg is None:
                        cfg = self._get_proxycfg(key)
                    if cfg:
                        # if cfg is not allowed (False), just ignore it
                        self._proxies[key][0].append(now)
                        self._proxies[key][1].append(float(value))

    def _get_proxycfg(self, key):
        for cfg in self._proxycfg:
            if cfg.regex.match(key):
                if not cfg.allow:
                    break  # this var is not allowed --> return False
                self._proxycfg_cache[key] = cfg
                return cfg
        self._proxycfg_cache[key] = False
        return False


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
