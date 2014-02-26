
# standard
import cStringIO
import sys
import time
import json
import urllib2

# vendor
from gevent import socket

E_BADSINKPORT = 'bad sink port: %s\n(should be an integer)'
E_BADSINKTYPE = 'bad sink type: %s\n(should be one of graphite|influxdb)'
E_BADSTATUSCODE = 'bad status code: %d\n(expected 200)'
E_SENDFAIL = 'failed to send stats to %s %s: %s'


class Sink(object):

    """
    A resource to which stats will be sent.
    """

    _default_host = 'localhost'

    def error(self, msg):
        sys.stderr.write(msg + '\n')

    def _compute_timer_stats(self, vals, percent):
        "Compute statistics from pending metrics"
        num = len(vals)
        vals = sorted(vals)
        vmin = vals[0]
        vmax = vals[-1]
        mean = vmin
        max_at_thresh = vmax
        if num > 1:
            idx = round((percent / 100.0) * num)
            tmp = vals[:int(idx)]
            if tmp:
                max_at_thresh = tmp[-1]
                mean = sum(tmp) / idx
        return {
            'mean': mean,
            'upper': vmax,
            'max_at_thresh': max_at_thresh,
            'lower': vmin,
            'count': num,
            }


class GraphiteSink(Sink):

    """
    Sends stats to one or more Graphite servers.
    """

    _default_port = 2003

    def __init__(self):
        self._hosts = set()

    def add(self, options):
        if isinstance(options, tuple):
            host = options[0] or self._default_host
            port = options[1] or self._default_port
        elif isinstance(options, dict):
            host = options.get('host', self._default_host)
            port = options.get('port', self._default_port)
        else:
            raise Exception('bad sink config object type: %r' % options)
        self._hosts.add((host, port))

    def send(self, stats, now):
        "Format stats and send to one or more Graphite hosts"
        buf = cStringIO.StringIO()
        num_stats = 0
        now = int(now)  # time precision = second

        # timer stats
        pct = stats.percent
        for key, vals in stats.timers.iteritems():
            if not vals:
                continue
            if key not in stats.timers_stats:
                stats.timers_stats[key] = self._compute_timer_stats(vals, pct)
            values = {
                'key': 'stats.timers.%s' % key,
                'now': now,
                'percent': pct,
                }
            values.update(stats.timers_stats[key])
            buf.write('%(key)s.mean %(mean)f %(now)d\n'
                      '%(key)s.upper %(upper)f %(now)d\n'
                      '%(key)s.upper_%(percent)d %(max_at_thresh)f %(now)d\n'
                      '%(key)s.lower %(lower)f %(now)d\n'
                      '%(key)s.count %(count)d %(now)d\n' % values)
            num_stats += 1

        # counter stats
        for key, val in stats.counts.iteritems():
            buf.write('stats.%(key)s %(count_interval)f %(now)d\n'
                      'stats_counts.%(key)s %(count)f %(now)d\n' % {
                          'key': key,
                          'count': val,
                          'count_interval': val / stats.interval,
                          'now': now
                          })
            num_stats += 1

        # gauges stats
        for key, val in stats.gauges.iteritems():
            buf.write('stats.%(key)s %(gauge)f %(now)d\n' % {
                'key': key,
                'gauge': val,
                'now': now
                })
            num_stats += 1

        buf.write('statsd.numStats %(num_stats)d %(now)d\n' % {
            'num_stats': num_stats,
            'now': now
            })

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
        buf.close()


class InfluxDBSink(Sink):

    """
    Sends stats to one or more InfluxDB servers.
    """

    def __init__(self):
        self._urls = set()

    _default_port = 8086

    def add(self, options):
        if isinstance(options, tuple):
            host = options[0] or self._default_host
            port = options[1] or self._default_port
            db, user, password = options[3]
        elif isinstance(options, dict):
            host = options.get('host', self._default_host)
            port = options.get('port', self._default_port)
            db = options['database']
            user = options['user']
            password = options['password']
        else:
            raise Exception('bad sink config object type: %r' % options)
        self._urls.add(
            'http://{0}:{1}/db/{2}/series?u={3}&p={4}&time_precision=m'
            .format(host, port, db, user, password))

    def send(self, stats, now):
        "Format stats and send to one or more InfluxDB hosts"
        # timer stats
        now = int(now * 1000)  # time precision = millisecond
        pct = stats.percent
        body = []
        for key, vals in stats.timers.iteritems():
            if not vals:
                continue
            if key not in stats.timers_stats:
                stats.timers_stats[key] = self._compute_timer_stats(vals, pct)
            v = stats.timers_stats[key]
            body.append({
                "name": "stats.%s.timer" % key,
                "columns": ["time", "mean", "upper", "upper_%d" % pct,
                            "lower", "count"],
                "points": [[now, v['mean'], v['upper'], v['max_at_thresh'],
                        v['lower'], v['count']]]
                })
        # counter stats
        for key, val in stats.counts.iteritems():
            body.append({
                "name": "stats.%s.count" % key,
                "columns": ["time", "value", "count_interval"],
                "points": [[now, val, val / stats.interval]]
                })
        # gauges stats
        for key, val in stats.gauges.iteritems():
            body.append({
                "name": "stats.%s.gauge" % key,
                "columns": ["time", "value"],
                "points": [[now, val]]
                })

        if not body:
            return
        for url in self._urls:
            # flush stats to influxdb
            try:
                req = urllib2.Request(url)
                req.add_header('Content-Type', 'application/json')
                resp = urllib2.urlopen(req, json.dumps(body), 3)
                status = resp.getcode()
                if status != 200:
                    raise ValueError(E_BADSTATUSCODE % status)
            except Exception, ex:
                self.error(E_SENDFAIL % ('influxdb', url, ex))


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

    def _parse_string(self, s):
        # parse the sink string config (coming from optparse)
        s = s.split(':')
        port = None
        host = None
        sink_options = s.pop(-1).split(',')
        sink_type = sink_options.pop(0)
        try:
            port = s.pop(-1)
            port = int(port)
            host = s.pop(-1)
        except IndexError:
            pass  # port and host are optional: keep default values
        except ValueError:
            raise ValueError(E_BADSINKPORT % port)
        return (host, port, sink_type, sink_options)

    def __init__(self, sinks):
        # construct the sink and add hosts to it
        self._sinks = {}
        for s in sinks:
            if isinstance(s, basestring):
                s = self._parse_string(s)
                sink_type = s[2]
            elif isinstance(s, dict):
                sink_type = s['type']
            else:
                raise Exception('bad sink config object type: %r' % s)
            try:
                if sink_type not in self._sinks:
                    self._sinks[sink_type] = self._sink_class_map[sink_type]()
                self._sinks[sink_type].add(s)
            except KeyError:
                raise ValueError(E_BADSINKTYPE % sink_type)

    def send(self, stats):
        "Send stats to one or more services"
        now = time.time()
        for sink in self._sinks.itervalues():
            sink.send(stats, now)
