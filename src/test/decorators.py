import pytest

def _safe_import(mod_name, attr):
    try:
        mod = __import__(mod_name)
    except ImportError:
        return False

    if attr is None:
        return mod

    try:
        import sys
        getattr(sys.modules[mod_name], attr)
    except AttributeError:
        return False
    return mod


def features():
    import os
    features = os.getenv("RBD_FEATURES")
    features = int(features) if features is not None else 125
    return features


def _missing_features(required_features):
    fs = features()
    if fs is None:
        return True
    for f in required_features:
        if f & fs != f:
            return True
    return False


def require_new_format():
    return pytest.mark.skipif(
        features() is None,
        reason='Old format deprecated'
    )


def require_features(required_features):
    return pytest.mark.skipif(
        _missing_features(required_features),
        reason='Missing required features'
    )


def require_ceph_conf(conf):
    import os
    return pytest.mark.skipif(
        not os.path.exists(conf),
        reason='Missing ceph conf: {0}'.format(conf)
    )


def require_module(mod, attr=None):
    return pytest.mark.skipif(
        not _safe_import(mod, attr),
        reason='Missing {0}{1} dependency'.format(mod, ('.' + attr) if attr is not None else '')
    )


def require_radosx(attr=None):
    return require_module('radosx', attr)


def require_rbdx(attr=None):
    return require_module('rbdx', attr)
