import sys
import time


E_BADSINKPORT = 'bad sink port: %s\n(should be an integer)'
E_BADSINKTYPE = 'bad sink type: %s\n(should be one of graphite|influxdb)'
E_BADSTATUSCODE = 'bad status code: %d\n(expected 200)'
E_SENDFAIL = 'failed to send stats to %s %s: %s'


def compute_timer_stats(self, vals, percent):
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


class Sink(object):

    """
    A resource to which stats will be sent.
    """

    _default_host = 'localhost'

    def error(self, msg):
        sys.stderr.write(msg + '\n')


class SinkManager(object):

    """
    A manager of sinks to which stats will be sent.
    """

    def _get_or_create_sink(self, name):
        if name not in self._sinks:
            if name == 'graphite':
                from gstatsd.graphitesink import GraphiteSink
                self._sinks[name] = GraphiteSink()
            elif name == 'influxdb':
                from gstatsd.influxdbsink import InfluxDBSink
                self._sinks[name] = InfluxDBSink()
            elif name == 'file':
                from gstatsd.filesink import FileSink
                self._sinks[name] = FileSink()
            elif name == 'lastvaluefile':
                from gstatsd.lastvaluefilesink import LastValueFileSink
                self._sinks[name] = LastValueFileSink()
            else:
                raise ValueError(E_BADSINKTYPE % name)
        return self._sinks[name]

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
            sink = self._get_or_create_sink(sink_type)
            sink.add(s)

    def send(self, stats, numstats=False):
        "Send stats to one or more services"
        now = time.time()
        for sink in self._sinks.itervalues():
            sink.send(stats, now, numstats=numstats)
