"""
Pool mapping file should have the form:
<poolid>\t<pool_name>

Object mapping file should have the form:
<host>\t<fullpath>
"""

def read_remote_file(hostname, fullpath):
    """
    :param hostname: the host from which to read
    :type hostname: string
    :param fullpath: path to file to read
    :type fullpath: string
    :returns: string - contents of file
    """
    pass

def parse_rbd_header_fullpath(fullpath):
    """
    :param fullpath: path to file to read
    :type fullpath: string
    :returns: (prefix, offset) where prefix is string and offset is int

    <imagename>.rbd
    """


def parse_rbd_block_fullpath(fullpath):
    """
    :param fullpath: path to file to read
    :type fullpath: string
    :returns: (prefix, offset) where prefix is string and offset is int

    <prefix>.<offset>
    """
    pass
