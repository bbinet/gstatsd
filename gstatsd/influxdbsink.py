import json
import urllib2

from gstatsd.sink import Sink, E_SENDFAIL, E_BADSTATUSCODE, compute_timer_stats


class InfluxDBSink(Sink):

    """
    Sends stats to one or more InfluxDB servers.
    """

    def __init__(self):
        self._urls = set()

    _default_port = 8086

    @classmethod
    def encode(cls, stats, now, numstats=False):
        now = int(now * 1000 * 1000)  # time precision = microsecond
        num_stats = 0
        pct = stats.percent
        body = []
        # timer stats
        for key, vals in stats.timers.iteritems():
            if not vals:
                continue
            if key not in stats.timers_stats:
                stats.timers_stats[key] = compute_timer_stats(vals, pct)
            v = stats.timers_stats[key]
            body.append({
                "name": "stats.%s.timer" % key,
                "columns": ["time", "mean", "upper", "upper_%d" % pct,
                            "lower", "count"],
                "points": [[now, v['mean'], v['upper'], v['max_at_thresh'],
                        v['lower'], v['count']]]
                })
            num_stats += 1
        # counter stats
        for key, val in stats.counts.iteritems():
            body.append({
                "name": "stats.%s.count" % key,
                "columns": ["time", "value", "count_interval"],
                "points": [[now, val, val / stats.interval]]
                })
            num_stats += 1
        # gauges stats
        for key, val in stats.gauges.iteritems():
            body.append({
                "name": "stats.%s.gauge" % key,
                "columns": ["time", "value"],
                "points": [[now, val]]
                })
            num_stats += 1
        # proxy values stats
        for key, vals in stats.proxies.iteritems():
            body.append({
                "name": "%s" % key,
                "columns": ["time", "value"],
                "points": [[int(t * 1000 * 1000), val] for t, val in vals]
                })
            num_stats += len(vals)

        if numstats:
            body.append({
                "name": "statsd.numStats",
                "columns": ["time", "value"],
                "points": [[now, num_stats]]
                })

        if body:
            return json.dumps(body)

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

    def send(self, stats, now, numstats=False):
        "Format stats and send to one or more InfluxDB hosts"

        body = self.__class__.encode(stats, now, numstats=numstats)
        if not body:
            return

        for url in self._urls:
            # flush stats to influxdb
            try:
                req = urllib2.Request(url)
                req.add_header('Content-Type', 'application/json')
                resp = urllib2.urlopen(req, body, 3)
                status = resp.getcode()
                if status != 200:
                    raise ValueError(E_BADSTATUSCODE % status)
            except Exception, ex:
                self.error(E_SENDFAIL % ('influxdb', url, ex))
