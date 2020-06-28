import pytest
import decorators as td

import six

IMG_ORDER = 22  # 4 MiB objects
IMG_SIZE = 8 << 20  # 8 MiB
pool_idx = 0


try:
    TimeoutError
except:
    class TimeoutError(OSError):
        pass


def pool_name():
    import os
    global pool_idx
    pool_idx += 1
    return "test-rbdx-" + '-' + str(os.getpid()) + '-' + str(pool_idx)


# Autouse fixtures instantiated before explicitly used fixtures, but no determined
# ordering in the same scope
# https://docs.pytest.org/en/stable/fixture.html#order-higher-scoped-fixtures-are-instantiated-first
# https://github.com/pytest-dev/pytest/issues/5460
@pytest.fixture(autouse=True)
def cleanup_modules(monkeypatch):
    with monkeypatch.context() as m:
        import sys
        # del loadded modules to build a clean environment
        m.delitem(sys.modules, 'radosx', raising=False)
        m.delitem(sys.modules, 'rbdx', raising=False)
        m.delitem(sys.modules, 'cephrbdx', raising=False)
        yield


@pytest.fixture
def mock_import_error(mocker):
    import sys
    # mocker.patch.dict returns nothing (i.e., None) before Python 3.8
    # m = mocker.patch.dict('sys.modules')
    mocker.patch.dict('sys.modules')
    sys.modules['radosx'] = None

    mocker.patch.dict('sys.modules', {'rbdx': None})


@pytest.fixture
def mock_import_rbdx_v1(mocker, monkeypatch):
    radosx = mocker.MagicMock()
    mocker.patch.dict('sys.modules', {'radosx': radosx})

    import sys
    rbdx = mocker.MagicMock()
    type(rbdx).IoCtx = mocker.PropertyMock(side_effect=ImportError)
    monkeypatch.setitem(sys.modules, 'rbdx', rbdx)


@pytest.fixture
def mock_import_rbdx_v2(mocker):
    import sys
    rbdx = mocker.MagicMock()
    mocker.patch.dict(sys.modules, rbdx=rbdx)


@pytest.fixture
def rados_client():
    import rados
    with rados.Rados(conffile='',
                     conf={'client_mount_timeout': '5'}) as rados_client:
        yield rados_client


@pytest.fixture
def features():
    return td.features()


def wait_for_pool_active(client, pool, timeout=3, tries=30, tick=1):
    import errno
    import json
    import os
    import time

    client.conf_set('rados_mon_op_timeout', str(timeout))

    def get_pg_num(client, pool):
        cmddict = {'prefix': 'osd pool get'}
        argdict = {
            'format': 'json',
            'pool': pool,
            'var': 'pg_num'
        }
        cmddict.update(argdict)
        cmd = [json.dumps(cmddict)]
        (r, outbuf, outs) = client.mon_command(cmd, inbuf=b'')
        if r != 0:
            return None
        d = json.loads(outbuf.decode('utf-8'))
        return int(d['pg_num'])

    def get_active_pg_num(client, pool):
        cmddict = {'prefix': 'pg ls-by-pool'}
        argdict = {
            'format': 'json',
            'poolstr': pool,
            'states': ['active']
        }
        cmddict.update(argdict)
        cmd = [json.dumps(cmddict)]
        (r, outbuf, outs) = client.mgr_command(cmd, inbuf=b'')
        # outbuf may be empty if all pgs are non-active
        if r != 0 or not outbuf:
            return None
        d = json.loads(outbuf.decode('utf-8'))
        # Nautilus added 'pg_ready' field
        if 'pg_ready' in d:
            pg_stats = d.get('pg_stats', [])
            return len(pg_stats)
        return len(d)

    pg_num = None
    while tries > 0:
        tries -= 1
        time.sleep(tick)

        if pg_num is None:
            pg_num = get_pg_num(client, pool)
        if pg_num is None:
            continue

        active_pg_num = get_active_pg_num(client, pool)
        if active_pg_num is None:
            continue
        if active_pg_num == pg_num:
            return

    raise TimeoutError(errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))


def test_mock_import_error(mock_import_error):
    import cephrbdx
    assert cephrbdx.rbdx_v2 == False
    assert cephrbdx.IMPORT_RBDX == False


def test_mock_import_rbdx_v1(mock_import_rbdx_v1):
    import cephrbdx
    assert cephrbdx.rbdx_v2 == False
    assert cephrbdx.IMPORT_RBDX == True


def test_mock_import_rbdx_v2(mock_import_rbdx_v2):
    import cephrbdx
    assert cephrbdx.rbdx_v2 == True
    assert cephrbdx.IMPORT_RBDX == True


def test_mock_rbdx_v1_list_images(mocker, mock_import_rbdx_v1):
    import cephrbdx
    assert cephrbdx.rbdx_v2 == False

    m = mocker.patch('cephrbdx.RbdRados')
    m.return_value.__enter__.return_value = m
    m.list_images.return_value = (mocker.Mock(), mocker.Mock())
    cephrbdx.get_images('cluster_name', [1, 2])

    assert m.called
    assert m.list_images.called
    assert m.list_images.call_args_list == [((1,),), ((2,),)]


def test_mock_rbdx_v2_list_images(mocker, mock_import_rbdx_v2):
    import cephrbdx
    assert cephrbdx.rbdx_v2 == True

    m = mocker.patch('cephrbdx.rbdx_list_images')
    cephrbdx.get_images('cluster_name', [1, 2])

    assert m.called
    # note: call_args.args/kwargs were introduced in Python 3.8
    # call_args is a tuple<args, kwargs> in previous versions
    # assert m.call_args.args == ('client.admin', 'cluster_name', [1, 2])
    assert m.call_args[0] == ('client.admin', 'cluster_name', [1, 2])


@td.require_module('rados')
def test_mock_rbdx_v2_list_images_with_data(mocker, mock_import_rbdx_v2):
    import cephrbdx
    assert cephrbdx.rbdx_v2 == True

    from munch import munchify

    expected = {
        'aceebe99a3d1': {
            'Size': 1073741824,
            'Capacity': 123
        },
        'faa94050cdfd': {
            'Size': 2073741824,
            'Capacity': 456
        },
        'fab53c8ce559': {
            'Size': 3073741824,
            'Capacity': 789
        },
    }

    info1 = munchify({
        "access_timestamp": 1592454820,
        "create_timestamp": 1592454820,
        "data_pool_id": -1,
        "dirty": 0,
        "du": 123,
        "features": 61,
        "flags": 0,
        "id": "aceebe99a3d1",
        "metas": {},
        "modify_timestamp": 1592454820,
        "name": "i1",
        "op_features": 0,
        "order": 22,
        "parent": {
            "image_id": "",
            "pool_id": -1,
            "pool_namespace": "",
            "snap_id": -2
        },
        "size": 1073741824,
        "snaps": {},
        "watchers": []
    })
    info2 = munchify({
        "access_timestamp": 1592909041,
        "create_timestamp": 1592909041,
        "data_pool_id": -1,
        "dirty": 0,
        "du": 456,
        "features": 317,
        "flags": 0,
        "id": "faa94050cdfd",
        "metas": {},
        "modify_timestamp": 1592909041,
        "name": "i2",
        "op_features": 1,
        "order": 22,
        "parent": {
            "image_id": "",
            "pool_id": -1,
            "pool_namespace": "",
            "snap_id": -2
        },
        "size": 2073741824,
        "snaps": {
            "2": {
                "children": [
                    {
                        "image_id": "fab53c8ce559",
                        "pool_id": 253,
                        "pool_namespace": ""
                    }
                ],
                "dirty": 0,
                "du": 789,
                "flags": 0,
                "id": 2,
                "name": "s1",
                "size": 1073741824,
                "snap_type": "user",
                "timestamp": 1592909050
            }
        },
        "watchers": []
    })
    info3 = munchify({
        "access_timestamp": 1592909082,
        "create_timestamp": 1592909082,
        "data_pool_id": -1,
        "dirty": 0,
        "du": 789,
        "features": 317,
        "flags": 0,
        "id": "fab53c8ce559",
        "metas": {},
        "modify_timestamp": 1592909082,
        "name": "c1",
        "op_features": 2,
        "order": 22,
        "parent": {
            "image_id": "faa94050cdfd",
            "pool_id": 253,
            "pool_namespace": "",
            "snap_id": 2
        },
        "size": 3073741824,
        "snaps": {},
        "watchers": []
    })

    mocker.patch('rados.Rados')

    m = mocker.patch('rbdx.list_info')
    m.return_value = ({
                          "aceebe99a3d1": [
                              info1,
                              0
                          ],
                          "faa94050cdfd": [
                              info2,
                              0
                          ],
                          "fab53c8ce559": [
                              info3,
                              0
                          ]
                      }, 0)

    images = cephrbdx.get_images('cluster_name', [1, 2])
    assert images['1'] == expected
    assert images['2'] == expected


def _test_list_images(rados_client, features):
    import rbd
    import cephrbdx

    cluster_name = 'ceph'

    pool_name1 = pool_name()
    rados_client.create_pool(pool_name1)
    ioctx1 = rados_client.open_ioctx(pool_name1)
    pool_id1 = rados_client.pool_lookup(pool_name1)

    wait_for_pool_active(rados_client, pool_name1)

    pool_name2 = pool_name()
    rados_client.create_pool(pool_name2)
    ioctx2 = rados_client.open_ioctx(pool_name2)
    pool_id2 = rados_client.pool_lookup(pool_name2)

    wait_for_pool_active(rados_client, pool_name2)

    # empty pool
    images = cephrbdx.get_images(cluster_name, [pool_id1])
    assert images == {
        str(pool_id1): {}
    }
    images = cephrbdx.get_images(cluster_name, [pool_id1, pool_id2])
    assert images == {
        str(pool_id1): {},
        str(pool_id2): {}
    }

    # pool1/image1
    rbd.RBD().create(ioctx1, 'p1i1', IMG_SIZE / 4, IMG_ORDER, old_format=False,
                     features=int(features))
    p1id1 = None
    with rbd.Image(ioctx1, 'p1i1') as p1i1:
        p1id1 = p1i1.id()

    # pool1/image2
    rbd.RBD().create(ioctx1, 'p1i2', IMG_SIZE / 2, IMG_ORDER, old_format=False,
                     features=int(features))
    p1id2 = None
    with rbd.Image(ioctx1, 'p1i2') as p1i2:
        p1id2 = p1i2.id()

    images = cephrbdx.get_images(cluster_name, [pool_id1, pool_id2])
    assert images == {
        str(pool_id1): {
            p1id1: {
                'Size': IMG_SIZE / 4,
                'Capacity': 0
            },
            p1id2: {
                'Size': IMG_SIZE / 2,
                'Capacity': 0
            },
        },
        str(pool_id2): {}
    }

    # pool2/image1
    rbd.RBD().create(ioctx2, 'p2i1', IMG_SIZE, IMG_ORDER, old_format=False,
                     features=int(features))
    p2id1 = None
    with rbd.Image(ioctx2, 'p2i1') as p2i1:
        p2id1 = p2i1.id()

    # pool2/image2
    rbd.RBD().create(ioctx2, 'p2i2', IMG_SIZE * 2, IMG_ORDER, old_format=False,
                     features=int(features))
    p2id2 = None
    with rbd.Image(ioctx2, 'p2i2') as p2i2:
        p2id2 = p2i2.id()

    images = cephrbdx.get_images(cluster_name, [pool_id2])
    assert images == {
        str(pool_id2): {
            p2id1: {
                'Size': IMG_SIZE,
                'Capacity': 0
            },
            p2id2: {
                'Size': IMG_SIZE * 2,
                'Capacity': 0
            }
        },
    }

    images = cephrbdx.get_images(cluster_name, [pool_id1, pool_id2])
    assert images == {
        str(pool_id1): {
            p1id1: {
                'Size': IMG_SIZE / 4,
                'Capacity': 0
            },
            p1id2: {
                'Size': IMG_SIZE / 2,
                'Capacity': 0
            }
        },
        str(pool_id2): {
            p2id1: {
                'Size': IMG_SIZE,
                'Capacity': 0
            },
            p2id2: {
                'Size': IMG_SIZE * 2,
                'Capacity': 0
            }
        },
    }

    # write data
    with rbd.Image(ioctx1, 'p1i1') as p1i1:
        p1i1.write(six.b('abc'), 0)
    with rbd.Image(ioctx1, 'p1i2') as p1i2:
        p1i2.write(six.b('abc'), (1 << IMG_ORDER) / 2)
    with rbd.Image(ioctx2, 'p2i1') as p2i1:
        p2i1.write(six.b('abc'), (1 << IMG_ORDER))
    with rbd.Image(ioctx2, 'p2i2') as p2i2:
        p2i2.write(six.b('abc'), (1 << IMG_ORDER))
        p2i2.write(six.b('abc'), (1 << IMG_ORDER) * 2)

    images = cephrbdx.get_images(cluster_name, [pool_id1, pool_id2])
    assert images == {
        str(pool_id1): {
            p1id1: {
                'Size': IMG_SIZE / 4,
                'Capacity': IMG_SIZE / 4
            },
            p1id2: {
                'Size': IMG_SIZE / 2,
                'Capacity': (1 << IMG_ORDER)
            }
        },
        str(pool_id2): {
            p2id1: {
                'Size': IMG_SIZE,
                'Capacity': (1 << IMG_ORDER)
            },
            p2id2: {
                'Size': IMG_SIZE * 2,
                'Capacity': (1 << IMG_ORDER) * 2
            }
        },
    }

    ioctx2.close()
    rados_client.delete_pool(pool_name2)

    ioctx1.close()
    rados_client.delete_pool(pool_name1)


@td.require_radosx()
@td.require_rbdx()
@td.require_ceph_conf('/etc/ceph/ceph.conf')
@td.require_new_format()
@pytest.mark.real
def test_rbdx_v1_list_images(monkeypatch, rados_client, features):
    with monkeypatch.context() as m:
        m.delattr('rbdx.IoCtx', raising=False)

        import cephrbdx
        assert cephrbdx.rbdx_v2 == False

        _test_list_images(rados_client, features)


@td.require_rbdx('IoCtx')
@td.require_new_format()
@pytest.mark.real
def test_rbdx_v2_list_images(monkeypatch, rados_client, features):
    import cephrbdx
    assert cephrbdx.rbdx_v2 == True

    _test_list_images(rados_client, features)
