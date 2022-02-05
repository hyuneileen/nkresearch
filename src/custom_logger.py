import logging
import sys

# colors
# pretty

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    cyan = "\x1b[36m"
    blue = "\x1b[34m;20m"
    yellow = "\x1b[33m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = " [%(asctime)s] [%(levelname)s] %(message)s"
    FORMATS = {
        logging.DEBUG: yellow + format + reset,
        logging.INFO: cyan + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def progressbar(it, prefix="", size=60, file=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        file.write("\033[0;32m %s[%s%s] %i/%i\r\033[0;0m" % (prefix, "#"*x, "."*(size-x), j, count))
        file.flush()        
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    file.write("\n")
    file.flush()
