
from importlib import import_module


def import_cls_string(dotted_cls_path):
    try:
        namespace, class_name = dotted_cls_path.rsplit('.', 1)
    except ValueError:
        msg = "%s doesn't look like a namespace path" % dotted_cls_path
        raise ImportError(msg)

    module = import_module(namespace)

    try:
        return getattr(module, class_name)
    except AttributeError:
        msg = 'Object "%s" not in namespace "%s"' % (class_name, namespace)
        raise ImportError(msg)
