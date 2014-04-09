from gevent import socket

from gstatsd.sink import Sink, E_SENDFAIL, compute_timer_stats


class GraphiteSink(Sink):

    """
    Sends stats to one or more Graphite servers.
    """

    _default_port = 2003

    def __init__(self):
        self._hosts = set()

    @classmethod
    def encode(cls, stats, now, numstats=True):
        now = int(now)  # time precision = second
        num_stats = 0
        lines = []

        # timer stats
        pct = stats.percent
        for key, vals in stats.timers.iteritems():
            if not vals:
                continue
            if key not in stats.timers_stats:
                stats.timers_stats[key] = compute_timer_stats(vals, pct)
            values = {
                'key': 'stats.timers.%s' % key,
                'now': now,
                'percent': pct,
                }
            values.update(stats.timers_stats[key])
            lines.append(
                '%(key)s.mean %(mean)f %(now)d\n'
                '%(key)s.upper %(upper)f %(now)d\n'
                '%(key)s.upper_%(percent)d %(max_at_thresh)f %(now)d\n'
                '%(key)s.lower %(lower)f %(now)d\n'
                '%(key)s.count %(count)d %(now)d\n' % values)
            num_stats += 1

        # counter stats
        for key, val in stats.counts.iteritems():
            lines.append(
                'stats.%(key)s %(count_interval)f %(now)d\n'
                'stats_counts.%(key)s %(count)f %(now)d\n' % {
                    'key': key,
                    'count': val,
                    'count_interval': val / stats.interval,
                    'now': now
                    })
            num_stats += 1

        # gauges stats
        for key, val in stats.gauges.iteritems():
            lines.append('stats.%(key)s %(gauge)f %(now)d\n' % {
                'key': key,
                'gauge': val,
                'now': now
                })
            num_stats += 1

        # proxies stats
        for key, vals in stats.proxies.iteritems():
            for t, val in vals:
                lines.append('stats.%(key)s %(val)s %(t)d\n' % {
                    'key': key,
                    'val': val,
                    't': int(t)
                    })
            num_stats += len(vals)

        if numstats:
            lines.append('statsd.numStats %(num_stats)d %(now)d\n' % {
                'num_stats': num_stats,
                'now': now
                })

        if lines:
            return ''.join(lines)

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

        data = self.__class__.encode(stats, now)
        if not data:
            return

        # TODO: add support for N retries

        for host in self._hosts:
            # flush stats to graphite
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(host)
                sock.sendall(data)
                sock.close()
            except Exception, ex:
                self.error(E_SENDFAIL % ('graphite', host, ex))
