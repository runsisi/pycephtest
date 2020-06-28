import logging

CLIENT_NAME = 'client.admin'
IMPORT_RBDX = True

# default to v2
rbdx_v2 = True
try:
    from rbdx import IoCtx
    import rbdx
except:
    rbdx_v2 = False
    try:
        import radosx
        import rbdx
    except:
        IMPORT_RBDX = False

log = logging.getLogger('cephrbdx')


class RbdxError(Exception):
    pass


def rbdx_list_images(client_name, cluster_name, pools):
    import rados

    def list_images_per_pool(client, pool):
        images = {}
        with client.open_ioctx2(pool) as ioctx:
            with rbdx.IoCtx(ioctx.ioctx()) as iox:
                (infos, r) = rbdx.list_info(iox, rbdx.INFO_F_IMAGE_DU)
                if r != 0:
                    log.error("rbdx: list info {0} failed: {1}".format(pool, r))
                    # None on failure
                    return None

                for iid, (info, r) in infos.items():
                    if r != 0:
                        log.error("rbdx: get image info {0}/{1} failed: {2}".format(pool, iid, r))
                        continue
                    images[iid] = {
                        'Size': info.size,
                        'Capacity': info.du
                    }
                return images

    images = {}
    try:
        with rados.Rados(name=client_name, clustername=cluster_name,
                         conffile='',
                         conf={'client_mount_timeout': '5',
                               'rados_osd_op_timeout': '3'}) as client:
            for p in pools:
                images[str(p)] = list_images_per_pool(client, p)
    except Exception as e:
        log.error("rbdx: list images failed: {0}".format(e))
    finally:
        return images


def get_images(cluster_name, pools):
    images = {}

    if rbdx_v2:
        return rbdx_list_images(CLIENT_NAME, cluster_name, pools)

    # rbdx v1
    try:
        with RbdRados(CLIENT_NAME, cluster_name) as client:
            for pool_id in pools:
                r, image_list = client.list_images(int(pool_id))
                if r == 0:
                    images[str(pool_id)] = image_list
                else:
                    images[str(pool_id)] = None
    except Exception as ex:
        log.error("Get images got exception %s", ex)
    finally:
        return images


class RbdRados(object):
    def __init__(self, client_name, cluster_name):
        self.client = radosx.Rados()
        r = self.client.init2(client_name, cluster_name, 0)
        if r != 0:
            raise RbdxError(r)
        r = self.client.conf_read_file('/etc/ceph/%s.conf' % cluster_name)
        if r != 0:
            raise RbdxError(r)
        r = self.client.conf_set('client_mount_timeout', '5')
        if r != 0:
            raise RbdxError(r)
        r = self.client.connect()
        if r != 0:
            raise RbdxError(r)
        self.rbdx = rbdx.xRBD()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client:
            self.client.shutdown()

    def list_images(self, pool_id):
        images = {}
        ioctx = radosx.IoCtx()
        r = self.client.ioctx_create2(int(pool_id), ioctx)
        if r != 0:
            return r, images

        image_list = rbdx.Map_string_2_pair_du_info_t_int()
        r = self.rbdx.list_du(ioctx, image_list)
        if r != 0:
            return r, images

        for iid, (info, r) in image_list.items():
            if r != 0:
                continue
            images[iid] = {
                'Size': info.size,
                'Capacity': info.du
            }
        return 0, images
