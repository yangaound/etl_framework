import datetime
import re
from importlib import import_module


DT_PATTERN = re.compile(
    r"^(?P<year>\d\d\d\d)-(?P<month>\d\d)-(?P<day>\d\d)[ T](?P<hour>\d\d):(?P<minute>\d\d):(?P<second>\d\d)\.(?P<microsecond>\d\d\d)"
)


def datetime2str(dt):
    str_dt = "%04d-%02d-%02d %02d:%02d:%02d.%06d" % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)
    return str_dt[:-3]


def str2datetime(str_dt):
    try:
        Y, m, d, H, M, S, ms = DT_PATTERN.match(str_dt).groups()
        return datetime.datetime(int(Y), int(m), int(d), int(H), int(M), int(S), int(ms))
    except Exception as e:
        raise Exception("'yyyy-mm-ddTHH:MM:SS.uuu' time format error  or " + str(e))


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError:
        msg = "%s doesn't look like a module path" % dotted_path
        raise ImportError(msg)

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        msg = 'Module "%s" does not define a "%s" attribute/class' % (module_path, class_name)
        raise ImportError(msg)


class ETLException(Exception):
    pass


class Error():
    no = 0
    msg = ""
    DropTaskErrno = 1
    ConnectSrcDBErrno = 1 << 1
    ConnectDestDBErrno = 1 << 2
    CorruptedSrcDBErrno = 1 << 3
    CorruptedDestDBErrno = 1 << 4
    ConfigErrorno = 1 << 5
    RumtimeErrno = 1 << 8
