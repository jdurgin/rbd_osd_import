#!/usr/bin/env python
"""
Pool mapping file should have the form:
<poolid> <pool_name>

Object mapping file should have the form:
<host> <ignored> <fullpath>
"""
import argparse
import getpass
import logging
import logging.handlers
import os.path
import paramiko
import Queue
import rados
import random
import rbd
import struct
import tempfile
import threading
import time

def read_remote_file(log, lock, connections, user, host_paths):
    """
    :param host_paths: tuples of hostname, fullpath
    :type fullpath: list of tuples
    :returns: string - contents of file
    """
    tries = 0
    while True:
        try:
            hostname, fullpath = random.choice(host_paths)
            conn = get_or_create_host_connection(lock, connections, user, hostname)
            sftp_client = conn.open_sftp()
            with tempfile.NamedTemporaryFile() as temp:
                sftp_client.get(fullpath, temp.name)
                sftp_client.close()
                temp.seek(0)
                return temp.read()
        except IOError:
            if len(host_paths) == 1:
                log.error("all locations for %s returned an IOError", os.path.basename(fullpath))
                raise
            log.debug('error reading %s:%s, trying another path',
                      hostname, fullpath, exc_info=True)
            host_paths = [(host, path) for host, path in host_paths if path != fullpath or host != hostname]
        except Exception:
            if tries > 10:
                raise
            tries += 1
            time.sleep(0.1)

def delete_remote_file(log, lock, connections, user, host_paths):
    """
    :param host_paths: tuples of hostname, fullpath
    :type fullpath: list of tuples
    :returns: string - contents of file
    """
    tries = 0
    while True:
        try:
            for hostname, fullpath in host_paths:
                log.info('deleting %s:%s', hostname, fullpath)
                try:
                    conn = get_or_create_host_connection(lock, connections, user, hostname)
                    sftp_client = conn.open_sftp()
                    sftp_client.remove(fullpath)
                    sftp_client.close()
                except IOError:
                    log.exception('could not delete %s:%s', hostname, fullpath)
                    continue
        except Exception:
            if tries > 10:
                raise
            tries += 1
            time.sleep(0.1)

class RestoreImage(threading.Thread):
    def __init__(self, log, lock, connections, pool_name, user, q, result_q):
        super(RestoreImage, self).__init__()
        self.log = log
        self.lock = lock
        self.connections = connections
        self.pool_name = pool_name
        self.user = user
        self.q = q
        self.result_q = result_q

    def run(self):
        while True:
            try:
                item = self.q.get()
                if item is None:
                    self.result_q.put(None)
                    return
                paths, image_name, block_name_prefix, order, size = item
                self.restore_image(paths, image_name, block_name_prefix, order, size)
                self.result_q.put(image_name)
            except Exception as e:
                self.result_q.put(e)
                self.log.exception('error restoring image %s', image_name)

    def restore_image(self, paths, image_name, block_name_prefix, order, size):
        with rados.Rados(conffile='') as cluster:
            with cluster.open_ioctx(self.pool_name) as ioctx:
                self.log.info('Creating image %s', image_name)
                try:
                    rbd.RBD().create(ioctx, image_name, size, order=order)
                except rbd.ImageExists:
                    # if the image already existed from an earlier run,
                    # try to restore the rest of it for idempotency
                    pass
                with rbd.Image(ioctx, image_name) as image:
                    num_objs = len(paths)
                    for i, host_paths in enumerate(paths):
                        obj_filename = os.path.basename(host_paths[0][1])
                        self.log.info('restoring object %s, %d/%d in image %s', obj_filename, i, num_objs, image_name)
                        data = read_remote_file(self.log,
                                                self.lock,
                                                self.connections,
                                                self.user,
                                                host_paths)
                        offset = rbd_block_offset(block_name_prefix, order, obj_filename)
                        image.write(data, offset)

class DeleteOldImage(threading.Thread):
    def __init__(self, log, lock, connections, pool_name, user, q, result_q):
        super(DeleteOldImage, self).__init__()
        self.log = log
        self.lock = lock
        self.connections = connections
        self.pool_name = pool_name
        self.user = user
        self.q = q
        self.result_q = result_q

    def run(self):
        while True:
            try:
                item = self.q.get()
                if item is None:
                    self.result_q.put(None)
                    return
                paths, image_name, block_name_prefix, order, size = item
                self.delete_image(paths, image_name, block_name_prefix, order, size)
                self.result_q.put(image_name)
            except Exception as e:
                self.result_q.put(e)
                self.log.exception('error deleting image %s', image_name)

    def delete_image(self, paths, image_name, block_name_prefix, order, size):
        error = None
        num_objs = len(paths)
        for i, host_paths in enumerate(paths):
            obj_filename = os.path.basename(host_paths[0][1])
            self.log.info('deleting object %s, %d/%d in image %s', obj_filename, i, num_objs, image_name)
            try:
                delete_remote_file(self.log,
                                   self.lock,
                                   self.connections,
                                   self.user,
                                   host_paths)
            except Exception as e:
                self.log.exception('error deleting object %s', obj_filename)
                error = e
        if error:
            raise error

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

def rbd_block_offset(block_name_prefix, order, basename):
    """
    :param basename: path to file to read
    :type basename: string
    :returns: offset where offset is the offset of the object into the image in bytes
    Names are <prefix>.<offset>
    """
    obj_num = int(basename[len(block_name_prefix) + 1:len(block_name_prefix) + 13], 16)
    return obj_num * 2**order

def image_name_from_header_path(fullpath):
    basename = os.path.basename(fullpath)
    return basename[:basename.find('.rbd__head')]

def get_pool_paths(pool_id, object_list_file, hosts, ignore_snapshots=True):
    paths = {}
    for line in object_list_file.readlines():
        host, _, fullpath = line.strip().split(' ', 2)
        pool_prefix = 'current/' + pool_id + '.'
        # ignore snapshots
        if pool_prefix in fullpath:
            if ignore_snapshots and '__head' not in fullpath:
                continue
            end = host.find('.osd')
            if end != -1:
                host = host[:end]
            paths.setdefault(os.path.basename(fullpath), [])
            # possibly restrict to specified hosts
            if hosts and host not in hosts:
                continue
            paths[os.path.basename(fullpath)].append((host, fullpath))
    return paths.values()

def get_rbd_header_paths(paths):
    return [host_paths for host_paths in paths if '.rbd' in host_paths[0][1]]

def get_rbd_data_paths(paths, block_name_prefix):
    return [host_paths for host_paths in paths if block_name_prefix in host_paths[0][1]]

def connect_to_host(user, hostname):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(hostname=hostname,
                username=user,
                timeout=60)
    ssh.get_transport().set_keepalive(True)
    return ssh

def get_or_create_host_connection(lock, connections, user, hostname):
    with lock:
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
        '--num-workers',
        type=int,
        default=1,
        help='how many images to restore in parallel threads',
        )
    parser.add_argument(
        'pool',
        nargs=1,
        help='which pool to restore',
    )
    parser.add_argument(
        'images',
        nargs='*',
        help='if specified, only restore the given images',
        )
    parser.add_argument(
        '--delete-old-images',
        type=bool,
        default=False,
        help='if specified, only delete the given images, don\'t restore anything',
        )
    parser.add_argument(
        '--yes-i-really-mean-it',
        type=bool,
        default=False,
        help='allow deleting old files',
        )
    parser.add_argument(
        '--dry-run',
        type=bool,
        default=False,
        help='print out image files that would be deleted and exit',
        )
    parser.add_argument(
        '--restrict-to-hosts',
        nargs='*',
        help='only delete/restore from specific hosts',
        )
    return parser.parse_args()

def main():
    args = parse_args()
    log = logging.getLogger()
    log_level = logging.INFO
    lib_log_level = logging.WARN
    if args.verbose:
        log_level = logging.DEBUG
        lib_log_level = logging.DEBUG
    logging.basicConfig(level=log_level)
    logging.getLogger('paramiko').setLevel(lib_log_level)
    if args.log_file is not None:
        handler = logging.handlers.WatchedFileHandler(
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

    if args.delete_old_images and not args.yes_i_really_mean_it and not args.dry_run:
        print "You must specify the --yes-i-really-mean-it flag to delete old files"
        return

    connections = {}
    pool_name = args.pool[0]
    pool_id = get_pool_id(args.pool_file, pool_name)
    if args.delete_old_images:
        action = 'deleting'
    else:
        action = 'restoring'
    log.info('%s rbd images from pool %s (id %s)', action, pool_name, pool_id)

    paths_for_pool = get_pool_paths(pool_id, args.object_list, args.restrict_to_hosts, not args.delete_old_images)
    headers_in_pool = get_rbd_header_paths(paths_for_pool)
    num_images = len(headers_in_pool)
    q = Queue.Queue()
    result_q = Queue.Queue()
    lock = threading.Lock()
    for i, host_paths in enumerate(headers_in_pool):
        image_name = image_name_from_header_path(host_paths[0][1])
        if args.images and image_name not in args.images:
            continue
        header = read_remote_file(log, lock, connections, args.user, host_paths)
        block_name_prefix, order, size = parse_rbd_header(header)
        log.info('RBD image header for %s:', image_name)
        log.info('\tblock_name_prefix: %s', block_name_prefix)
        log.info('\torder: %d', order)
        log.info('\tsize: %d bytes', size)
        data_paths = get_rbd_data_paths(paths_for_pool, block_name_prefix)
        if args.dry_run:
            for host, path in host_paths:
                log.info('would be %s %s:%s', action, host, path)
            for paths in data_paths:
                for host, path in paths:
                    log.info('would be %s %s:%s', action, host, path)
        else:
            q.put([data_paths, image_name, block_name_prefix, order, size])

    if args.dry_run:
        return

    for i in xrange(args.num_workers):
        q.put(None)

    if args.delete_old_images:
        threads = [DeleteOldImage(log, lock, connections, pool_name, args.user, q, result_q) for i in xrange(args.num_workers)]
    else:
        threads = [RestoreImage(log, lock, connections, pool_name, args.user, q, result_q) for i in xrange(args.num_workers)]
    for thread in threads:
        thread.start()

    if args.delete_old_images:
        action = 'deleting'
    else:
        action = 'restoring'
    i = 0
    finished = 0
    errors = 0
    while finished < args.num_workers:
        item = result_q.get()
        if item is None:
            finished += 1
            continue
        if isinstance(item, Exception):
            errors += 1
            continue
        i += 1
        log.info('Finished %s image %s (%d/%d)',
                 action, item, i, num_images)

    if args.delete_old_images:
        for i, host_paths in enumerate(headers_in_pool):
            image_name = image_name_from_header_path(host_paths[0][1])
            if args.images and image_name not in args.images:
                continue
            log.info('Removing header %d/%d', i, len(headers_in_pool))
            delete_remote_file(log, lock, connections, args.user, host_paths)

    if errors > 0:
        log.error('Failed to %s %d images (see log for details)', action, errors)
    else:
        log.info('Finished %s all images in pool %s', action, pool_name)

if __name__ == '__main__':
    main()
