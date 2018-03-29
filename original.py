import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str




class node(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
        self.metadata = {}
        self.data = defaultdict(bytes)
        self.directories = {}
        self.fd = 0
        now = time()
        self.metadata['/'] = dict(st_mode=(S_IFDIR | 0777), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)

    def chmod(self, path, mode):
        c = search(path)
        self.metadata[path]['st_mode'] &= 0770000
        self.metadata[path]['st_mode'] |= mode
        c.metadata[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        c=search(path)
        self.metadata[path]['st_uid'] = uid
        self.metadata[path]['st_gid'] = gid
        c.metadata[path]['st_uid'] = uid
        c.metadata[path]['st_gid'] = gid


def search(path) :
    count = path.count('/')
    start = self.root
    if count == 1 :
        start = self.root
    else :
        list = path.split('/')
        for i in list[:-1] :
            if (i in start.directories) :
                start = start.directories[i]
                
            

    return start


def mkdir(self,path,mode) :
    a = search(path)
    b = node()
    a.directories[name] = b
    self.metadata[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
    a.metadata[name] = self.metadata[path]
    
    a.metadata['st_nlink'] += 1


def create(self, path, mode):
    self.metadata[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
    c = search(path)
    c.metadata[path] = self.metadata[path]
    self.fd += 1
    return self.fd

def rename(self, old, new):
        self.files[new] = self.files.pop(old)
        d  = search(old)
        d.metadata[new] = d.metadata.pop[old]


def open(self, path, flags):
    self.fd += 1
    return self.fd

def read(self, path, size, offset, fh):
    return self.data[path][offset:offset + size]


def rmdir(self,path) :
    self.metadata.pop(path)
    e = search(path)
    e.metadata.pop(path)
    e.metadata['st_nlink'] -= 1

def setxattr(self, path, name, value, options, position=0):
     # Ignore options
     f = search(path)
     attrs = self.files[path].setdefault('attrs', {})
     attrs[name] = value
     attrs1 = f.metadata[path].setdefault ('attrs',{})
     attrs1[name] = value

def getattr(self, path, fh=None):
    if path not in self.metadata:
        raise FuseOSError(ENOENT)

    return self.files[path]

    def getxattr(self, path, name, position=0):
        attrs = self.metadata[path].get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR


    def listxattr(self, path):
        attrs = self.metadata[path].get('attrs', {})
        return attrs.keys()       



    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def chown(self, path, uid, gid):
        f = search(path)
        self.metadata[path]['st_uid'] = uid
        self.metadata[path]['st_gid'] = gid
        f.metadata[path]['st_uid'] = uid
        f.metadata[path]['st_gid'] = gid

    def removexattr(self, path, name):
        c = search(path)
        attrs = self.metadata[path].get('attrs', {})
        attrs1 = c.metadata[path].get('attrs', {})
        try:
            del attrs[name]
            del attrs1[name]
        except KeyError:
            pass        # Should return ENOATTR

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        c = search(target)
        self.metadata[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))
        c.metadata[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))
        self.data[target] = source

    def truncate(self, path, length, fh=None):
        c = search(path)
        self.data[path] = self.data[path][:length]
        self.metadata[path]['st_size'] = length
        c.metadata[path]['st_size'] = length
    def unlink(self, path):
        c = search(path)
        self.metadata.pop(path)
        c.metadata.pop(path)

    def utimens(self, path, times=None):
        c = search(path)
        now = time()
        atime, mtime = times if times else (now, now)
        self.metadata[path]['st_atime'] = atime
        self.metadata[path]['st_mtime'] = mtime
        c.metadata[path]['st_atime'] = atime
        c.metadata[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        c = search(path)
        self.data[path] = self.data[path][:offset] + data
        self.metadata[path]['st_size'] = len(self.data[path])
        c.metadata[path]['st_size'] = len(self.data[path])
        return len(data)
if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(node(), argv[1], foreground=True,debug=True)

