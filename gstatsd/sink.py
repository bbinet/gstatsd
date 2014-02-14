
# standard
import cStringIO
import sys
import time

# vendor
from gevent import socket

E_BADSINKPORT = 'bad sink port: %s\n(should be an integer)'
E_BADSINKTYPE = 'bad sink type: %s\n(should be one of graphite|influxdb)'
E_SENDFAIL = 'failed to send stats to %s %s: %s'


class Sink(object):

    """
    A resource to which stats will be sent.
    """

    _default_host = 'localhost'
    _hosts = set()

    def error(self, msg):
        sys.stderr.write(msg + '\n')

    def add(self, spec):
        if isinstance(spec, basestring):
            spec = spec.split(':')
        port = self._default_port
        host = self._default_host
        try:
            port = spec.pop(-1)
            port = int(port)
            host = spec.pop(-1)
        except IndexError:
            pass  # port and host are optional: keep default values
        except ValueError:
            raise ValueError(E_BADSINKPORT % port)
        self._hosts.add((host, port))


class GraphiteSink(Sink):

    """
    Sends stats to one or more Graphite servers.
    """

    _default_port = 2003

    def send(self, stats):
        "Format stats and send to one or more Graphite hosts"
        buf = cStringIO.StringIO()
        now = int(time.time())
        num_stats = 0

        # timer stats
        pct = stats.percent
        timers = stats.timers
        for key, vals in timers.iteritems():
            if not vals:
                continue

            # compute statistics
            num = len(vals)
            vals = sorted(vals)
            vmin = vals[0]
            vmax = vals[-1]
            mean = vmin
            max_at_thresh = vmax
            if num > 1:
                idx = round((pct / 100.0) * num)
                tmp = vals[:int(idx)]
                if tmp:
                    max_at_thresh = tmp[-1]
                    mean = sum(tmp) / idx

            key = 'stats.timers.%s' % key
            buf.write('%s.mean %f %d\n' % (key, mean, now))
            buf.write('%s.upper %f %d\n' % (key, vmax, now))
            buf.write('%s.upper_%d %f %d\n' % (key, pct, max_at_thresh, now))
            buf.write('%s.lower %f %d\n' % (key, vmin, now))
            buf.write('%s.count %d %d\n' % (key, num, now))
            num_stats += 1

        # counter stats
        counts = stats.counts
        for key, val in counts.iteritems():
            buf.write('stats.%s %f %d\n' % (key, val / stats.interval, now))
            buf.write('stats_counts.%s %f %d\n' % (key, val, now))
            num_stats += 1

        # counter stats
        gauges = stats.gauges
        for key, val in gauges.iteritems():
            buf.write('stats.%s %f %d\n' % (key, val, now))
            buf.write('stats_counts.%s %f %d\n' % (key, val, now))
            num_stats += 1

        buf.write('statsd.numStats %d %d\n' % (num_stats, now))

        # TODO: add support for N retries

        for host in self._hosts:
            # flush stats to graphite
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(host)
                sock.sendall(buf.getvalue())
                sock.close()
            except Exception, ex:
                self.error(E_SENDFAIL % ('graphite', host, ex))


class InfluxDBSink(Sink):

    """
    Sends stats to one or more InfluxDB servers.
    """

    _default_port = 8086

    def send(self, stats):
        "Format stats and send to one or more InfluxDB hosts"
        raise NotImplementedError()


class SinkManager(object):

    """
    A manager of sinks to which stats will be sent.
    """

    # TODO: support more sink types. Currently only graphite and influxdb
    # backends are supported, but we may want to write stats to hbase, redis...
    _sink_class_map = {
        'graphite': GraphiteSink,
        'influxdb': InfluxDBSink,
        }

    def __init__(self, sinkspecs):
        # construct the sink and add hosts to it
        self._sinks = {}
        for spec in sinkspecs:
            spec = spec.split(':')
            sink_type = spec.pop(-1)
            try:
                if sink_type not in self._sinks:
                    self._sinks[sink_type] = self._sink_class_map[sink_type]()
                self._sinks[sink_type].add(spec)
            except KeyError:
                raise ValueError(E_BADSINKTYPE % sink_type)

    def send(self, stats):
        "Send stats to one or more services"
        for sink in self._sinks.itervalues():
            sink.send(stats)
