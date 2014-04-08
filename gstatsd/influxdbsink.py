import json
import urllib2

from gstatsd.sink import Sink, E_SENDFAIL, E_BADSTATUSCODE


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
            'http://{0}:{1}/db/{2}/series?u={3}&p={4}&time_precision=u'
            .format(host, port, db, user, password))

    def send(self, stats, now):
        "Format stats and send to one or more InfluxDB hosts"
        now = int(now * 1000 * 1000)  # time precision = microsecond
        pct = stats.percent
        body = []
        # timer stats
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
        # proxy values stats
        for key, vals in stats.proxies.iteritems():
            body.append({
                "name": "stats.%s.proxy" % key,
                "columns": ["time", "value"],
                "points": [[int(t * 1000 * 1000), val] for t, val in vals]
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
