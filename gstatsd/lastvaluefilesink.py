import os
from gstatsd.sink import Sink


class LastValueFileSink(Sink):

    def __init__(self):
        self._vars = set()

    def add(self, options):
        if isinstance(options, tuple):
            opts = options[3]
            filename = opts[0]
            metric = opts[1]
        elif isinstance(options, dict):
            filename = options['filename']
            metric = options['metric']
        else:
            raise Exception('bad sink config object type: %r' % options)
        self._vars.add((filename, metric))

    def send(self, stats, now, numstats=False):
        for filename, metric in self._vars:
            if metric in stats.proxies and len(stats.proxies[metric]) > 0:
                if not os.path.exists(os.path.dirname(filename)):
                    os.makedirs(os.path.dirname(filename))
                with open(filename, 'w') as f:
                    f.write('%.2f' % stats.proxies[metric][-1][1])
