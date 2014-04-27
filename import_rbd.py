#!/usr/bin/env python
"""
Pool mapping file should have the form:
<poolid>\t<pool_name>

Object mapping file should have the form:
<host>\t<fullpath>
"""
import argparse
import getpass
import logging
import os.path
import paramiko
import rados
import rbd
import struct
import tempfile

def read_remote_file(connections, user, hostname, fullpath):
    """
    :param hostname: the host from which to read
    :type hostname: string
    :param fullpath: path to file to read
    :type fullpath: string
    :returns: string - contents of file
    """
    conn = get_or_create_host_connection(connections, user, hostname)
    sftp_client = conn.open_sftp()
    with tempfile.NamedTemporaryFile() as temp:
        sftp_client.get(fullpath, temp.name)
        sftp_client.close()
        temp.seek(0)
        return temp.read()

def restore_rbd_image(log, connections, pool_name, user, paths,
                      image_name, block_name_prefix, order, size):
    with rados.Rados(conffile='') as cluster:
        with cluster.open_ioctx(pool_name) as ioctx:
            log.info('Creating image %s', image_name)
            rbd.RBD().create(ioctx, image_name, size, order=order)
            with rbd.Image(ioctx, image_name) as image:
                num_objs = len(paths)
                for i, (host, path) in enumerate(paths):
                    log.info('restoring object %d/%d', i, num_objs)
                    data = read_remote_file(connections, user, host, path)
                    offset = rbd_block_offset(block_name_prefix, order, path)
                    image.write(data, offset)

def parse_rbd_header(header_contents):
    """
    :param header_contents: the rbd header as stored on disk
    :type header_contents: string
    :returns: (block name prefix, order, size) where prefix is string
    and order and size are int
    """
    fmt = '<40s24s4s8sbbbbQ'
    unpacked = struct.unpack(fmt, header_contents[:struct.calcsize(fmt)])
    prefix = unpacked[1].rstrip('\x00')
    order = unpacked[4]
    size = unpacked[8]
    return prefix, order, size

def rbd_block_offset(block_name_prefix, order, fullpath):
    """
    :param fullpath: path to file to read
    :type fullpath: string
    :returns: offset where offset is the offset of the object into the image in bytes
    Names are <prefix>.<offset>
    """
    basename = os.path.basename(fullpath)
    obj_num = int(basename[len(block_name_prefix) + 1:len(block_name_prefix) + 13])
    return obj_num * 2**order

def image_name_from_header_path(fullpath):
    basename = os.path.basename(fullpath)
    return basename[:basename.find('.rbd')]

def get_pool_paths(pool_id, object_list_file):
    paths = {}
    for line in object_list_file.readlines():
        host, _, fullpath = line.strip().split(' ', 2)
        pool_prefix = 'current/' + pool_id
        # ignore snapshots
        if pool_prefix in fullpath and '__head' in fullpath:
            paths[os.path.basename(fullpath)] = (host[:host.find('.osd')], fullpath)
    return paths.values()

def get_rbd_header_paths(paths):
    return [(host, path) for host, path in paths if '.rbd' in path]

def get_rbd_data_paths(paths, block_name_prefix):
    return [(host, path) for host, path in paths if block_name_prefix in path]

def connect_to_host(user, hostname):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(hostname=hostname,
                username=user,
                timeout=60)
    ssh.get_transport().set_keepalive(True)
    return ssh

def get_or_create_host_connection(connections, user, hostname):
    if hostname not in connections or not connections[hostname].get_transport().is_alive():
        connections[hostname] = connect_to_host(user, hostname)
    return connections[hostname]

def get_pool_id(pool_file, pool_name):
    for line in pool_file.readlines():
        cur_id, cur_name = line.strip().split(' ', 1)
        logging.getLogger().debug('pool id, name = %r, %r', cur_id, cur_name)
        if cur_name == pool_name:
            return cur_id
    raise Exception('could not find pool ' + pool_name + ' in pools file')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Restore rbd images from osd filestores',
    )
    verbosity = parser.add_mutually_exclusive_group(required=False)
    verbosity.add_argument(
        '-v', '--verbose',
        action='store_true', dest='verbose',
        help='be more verbose',
    )
    verbosity.add_argument(
        '-q', '--quiet',
        action='store_true', dest='quiet',
        help='be less verbose',
    )
    parser.add_argument(
        '--user',
        required=False,
        help='ssh username for accessing osds',
    )
    parser.add_argument(
        '--pool-file',
        required=False,
        type=file,
        help='text file with lines in the format pool_id pool_name',
        )
    parser.add_argument(
        '--object-list',
        required=True,
        type=file,
        help='text file with lines in the format <host> <ignored> <absolute_path>',
        )
    parser.add_argument(
        '--log-file',
        help='where to store log output',
        )
    parser.add_argument(
        'pool',
        nargs=1,
        help='which pool to restore',
    )
    return parser.parse_args()

def main():
    args = parse_args()
    log = logging.getLogger()
    log_level = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level)
    if args.log_file is not None:
        handler = logging.handlers.FileHandler(
            filename=args.log_file,
            )
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d %(process)d:%(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S',
            )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
    if args.user is None:
        args.user = getpass.getuser()

    connections = {}
    pool_name = args.pool[0]
    pool_id = get_pool_id(args.pool_file, pool_name)
    log.info('Restoring rbd images from pool %s (id %s)', pool_name, pool_id)

    paths_for_pool = get_pool_paths(pool_id, args.object_list)
    headers_in_pool = get_rbd_header_paths(paths_for_pool)
    num_images = len(headers_in_pool)
    for i, (host, fullpath) in enumerate(headers_in_pool):
        image_name = image_name_from_header_path(fullpath)
        log.info('Restoring image %s (%d/%d)', image_name, i, num_images)
        header = read_remote_file(connections, args.user, host, fullpath)
        block_name_prefix, order, size = parse_rbd_header(header)
        log.info('\tblock_name_prefix: %s', block_name_prefix)
        log.info('\torder: %d', order)
        log.info('\tsize: %d bytes', size)

        data_paths = get_rbd_data_paths(paths_for_pool, block_name_prefix)
        restore_rbd_image(log, connections, pool_name, args.user,
                          data_paths, image_name, block_name_prefix,
                          order, size)
    log.info('Finished restoring pool %s', pool_name)

if __name__ == '__main__':
    main()
