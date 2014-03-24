import time
import unittest

from gstatsd import service, config


class StatsServiceTest(unittest.TestCase):

    def setUp(self):
        self.svc = service.StatsDaemon(
            config.StatsConfig({
                'sinks': ['2003:graphite'],
                'flush_interval': 5,
                }),
            False)
        self.svc._reset_stats()
        self.stats = self.svc._stats

    def test_construct(self):
        svc = service.StatsDaemon(
            config.StatsConfig({
                'sinks': ['2003:graphite'],
                }),
            False)
        svc._reset_stats()
        stats = svc._stats
        self.assertEquals(svc._bindaddr, ('localhost', 8125))
        self.assertEquals(svc._interval, 10.0)
        self.assertEquals(svc._debug, False)
        self.assertEquals(stats.percent, 90.0)
        self.assertEquals(
            svc._sink._sinks['graphite']._hosts, set([('localhost', 2003)]))

        svc = service.StatsDaemon(
            config.StatsConfig({
                'host': 'bar',
                'port': 8125,
                'sinks': ['foo:2004:graphite'],
                'flush_interval': 5,
                'threshold': 80,
                }),
            True)
        svc._reset_stats()
        stats = svc._stats
        self.assertEquals(svc._bindaddr, ('bar', 8125))
        self.assertEquals(
            svc._sink._sinks['graphite']._hosts, set([('foo', 2004)]))
        self.assertEquals(svc._interval, 5.0)
        self.assertEquals(stats.percent, 80.0)
        self.assertEquals(svc._debug, True)

    def test_counters(self):
        pkt = 'foo:1|c'
        self.svc._process(pkt, time.time())
        self.assertEquals(self.stats.counts, {'foo': 1})
        self.svc._process(pkt, time.time())
        self.assertEquals(self.stats.counts, {'foo': 2})
        pkt = 'foo:-1|c'
        self.svc._process(pkt, time.time())
        self.assertEquals(self.stats.counts, {'foo': 1})

    def test_counters_sampled(self):
        pkt = 'foo:1|c|@.5'
        self.svc._process(pkt, time.time())
        self.assertEquals(self.stats.counts, {'foo': 2})

    def test_timers(self):
        pkt = 'foo:20|ms'
        self.svc._process(pkt, time.time())
        self.assertEquals(self.stats.timers, {'foo': [20.0]})
        pkt = 'foo:10|ms'
        self.svc._process(pkt, time.time())
        self.assertEquals(self.stats.timers, {'foo': [20.0, 10.0]})

    def test_key_sanitize(self):
        pkt = '\t\n#! foo . bar \0 ^:1|c'
        self.svc._process(pkt, time.time())
        self.assertEquals(self.stats.counts, {'foo.bar': 1})

    def test_key_prefix(self):
        svc = service.StatsDaemon(
            config.StatsConfig({
                'sinks': ['2003:graphite'],
                'flush_interval': 5,
                'prefix': 'pfx',
                }))
        svc._reset_stats()
        pkt = 'foo:1|c'
        svc._process(pkt, time.time())
        self.assertEquals(svc._stats.counts, {'pfx.foo': 1})


def main():
    unittest.main()


if __name__ == '__main__':
    main()
