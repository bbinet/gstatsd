import os
import sys
import traceback
import errno

from gstatsd.sink import Sink, E_SENDFAIL


class RotatingFileHandler:

    # Thanks to the Supervisor project from which most of this code has been
    # copied. See the original source at:
    # http://github.com/Supervisor/supervisor/blob/master/supervisor/loggers.py
    def __init__(self, filename, mode='a', maxBytes=512*1024*1024,
                 backupCount=10):
        """
        Open the specified file and use it as the stream for logging.

        By default, the file grows indefinitely. You can specify particular
        values of maxBytes and backupCount to allow the file to rollover at
        a predetermined size.

        Rollover occurs whenever the current log file is nearly maxBytes in
        length. If backupCount is >= 1, the system will successively create
        new files with the same pathname as the base file, but with extensions
        ".1", ".2" etc. appended to it. For example, with a backupCount of 5
        and a base file name of "app.log", you would get "app.log",
        "app.log.1", "app.log.2", ... through to "app.log.5". The file being
        written to is always "app.log" - when it gets filled up, it is closed
        and renamed to "app.log.1", and if files "app.log.1", "app.log.2" etc.
        exist, then they are renamed to "app.log.2", "app.log.3" etc.
        respectively.

        If maxBytes is zero, rollover never occurs.
        """
        if maxBytes > 0:
            mode = 'a'  # doesn't make sense otherwise!
        self.stream = open(filename, mode)
        self.baseFilename = filename
        self.mode = mode
        self.maxBytes = maxBytes
        self.backupCount = backupCount

    def flush(self):
        try:
            self.stream.flush()
        except IOError, why:
            # if output is piped, EPIPE can be raised at exit
            if why.args[0] != errno.EPIPE:
                raise

    def close(self):
        if hasattr(self.stream, 'fileno'):
            fd = self.stream.fileno()
            if fd < 3:  # don't ever close stdout or stderr
                return
        self.stream.close()

    def handleError(self):
        ei = sys.exc_info()
        traceback.print_exception(ei[0], ei[1], ei[2], None, sys.stderr)
        del ei

    def reopen(self):
        self.close()
        self.stream = open(self.baseFilename, self.mode)

    def remove(self):
        try:
            os.remove(self.baseFilename)
        except OSError, why:
            if why.args[0] != errno.ENOENT:
                raise

    def write(self, msg):
        """
        Write message to the stream.

        Output the message to the file, catering for rollover as described
        in doRollover().
        """
        try:
            try:
                self.stream.write(msg)
            except UnicodeError:
                self.stream.write(msg.encode("UTF-8"))
            self.flush()
        except:
            self.handleError()
        self.doRollover()

    def removeAndRename(self, sfn, dfn):
        if os.path.exists(dfn):
            try:
                os.remove(dfn)
            except OSError, why:
                # catch race condition (destination already deleted)
                if why.args[0] != errno.ENOENT:
                    raise
        try:
            os.rename(sfn, dfn)
        except OSError, why:
            # catch exceptional condition (source deleted)
            # E.g. cleanup script removes active log.
            if why.args[0] != errno.ENOENT:
                raise

    def doRollover(self):
        """
        Do a rollover, as described in __init__().
        """
        if self.maxBytes <= 0:
            return

        if not (self.stream.tell() >= self.maxBytes):
            return

        self.stream.close()
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d" % (self.baseFilename, i)
                dfn = "%s.%d" % (self.baseFilename, i + 1)
                if os.path.exists(sfn):
                    self.removeAndRename(sfn, dfn)
            dfn = self.baseFilename + ".1"
            self.removeAndRename(self.baseFilename, dfn)
        self.stream = open(self.baseFilename, 'w')


class FileSink(Sink):

    """
    Sends stats to one or more files
    """

    def __init__(self):
        self._files = set()

    def add(self, options):
        maxbytes = 512*1024*1024
        backupcount = 10
        if isinstance(options, tuple):
            opts = options[3]
            filename = opts[0]
            try:
                maxbytes = opts[1]
                backupcount = opts[2]
            except IndexError:
                pass  # maxbytes is optional
        elif isinstance(options, dict):
            filename = options['filename']
            maxbytes = options.get('maxbytes', maxbytes)
            backupcount = options.get('backupcount', backupcount)
        else:
            raise Exception('bad sink config object type: %r' % options)
        self._files.add(RotatingFileHandler(filename, maxBytes=maxbytes,
                                            backupCount=backupcount))

    def send(self, stats, now):
        "Format stats and send to one or more files"
        pct = stats.percent
        lines = []
        # timer stats
        for key, vals in stats.timers.iteritems():
            if not vals:
                continue
            if key not in stats.timers_stats:
                stats.timers_stats[key] = self._compute_timer_stats(vals, pct)
            values = {
                'key': key,
                'now': now,
                'percent': pct,
                }
            values.update(stats.timers_stats[key])
            lines.append('stats.%(key)s.timer %(now)d %(mean)f %(upper)f '
                         '%(max_at_thresh)f %(lower)f %(count)d' % values)

        # counter stats
        for key, val in stats.counts.iteritems():
            lines.append('stats.%(key)s.count %(now)d %(count)f '
                         '%(count_interval)f' % {
                             'key': key,
                             'count': val,
                             'count_interval': val / stats.interval,
                             'now': now
                             })

        # gauges stats
        for key, val in stats.gauges.iteritems():
            lines.append('stats.%(key)s.gauge %(now)d %(gauge)f' % {
                'key': key,
                'gauge': val,
                'now': now
                })

        # proxies stats
        for key, vals in stats.proxies.iteritems():
            for t, val in vals:
                lines.append('stats.%(key)s.proxy %(t)d %(val)s' % {
                    'key': key,
                    'val': val,
                    't': int(t)
                    })

        if not lines:
            return

        for f in self._files:
            # flush stats to file
            try:
                f.write('\n'.join(lines + ['']))
            except Exception, ex:
                self.error(E_SENDFAIL % ('file', f.baseFilename, ex))
