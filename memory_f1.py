#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

############################
### RV: custom functions ###
############################

### split path to keys####

def splitPath(spath):
 
  print "split path"
  if spath == '/':  
      return list('/')
  else:
      mapList = spath.split('/')
      mapList=['/' + x for x in mapList]
      print mapList
      return mapList

### make path from list of keys ###
def makePath(plist):    
    print "make path"
    nlist  = []
    
    for x in plist: nlist.append(x[1:])        
      
    nlist =  '/'.join(nlist)
    
    print nlist
    
    return nlist

### get file from dictionary ###


def getFileDict(fileDict, fpath): ## rv check path       
     
     if not isinstance(fpath,list):
         mapList = splitPath(fpath)
     else:
         mapList = fpath
     
     print "get file"
     print mapList    
         
     return reduce(lambda d, k: d[k], mapList, fileDict)


def setFileDict(fileDict, spath, value): ## set path    
    
    # if not isinstance(spath,list):
    #     mapList = splitPath(spath)
    # else:
    #     mapList = spath   
     
     mapList = splitPath(spath)
     
     print "setFile"
     print mapList 
     
     if spath == '/':
          fileDict["/"] = value
     else:
          getFileDict(fileDict, makePath(mapList[:-1]))[mapList[-1]] = value
          getFileDict(fileDict, makePath(mapList[:-1]))['st_nlink'] += 1

def getParentDir(fileDict, spath): ## gets the key to parent directory   
     
     mapList = splitPath(spath)
     
     print "setFile"
     print mapList 
     
     if spath == '/':
          return None
     else:
          return getFileDict(fileDict, makePath(mapList[:-1]))         
          
          
def pathCheck(fileDict,spath):
    
    print "path check"
    print spath
    print fileDict
    try:
      getFileDict(fileDict,spath)
      return True
    except:
      return False

######
######

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)

    def chmod(self, path, mode):
        myfiles = getFileDict(self.files,path) #rv
        myfiles['st_mode'] &= 0770000 #rv
        myfiles['st_mode'] |= mode    #rv
        return 0

    def chown(self, path, uid, gid):
        myfiles = getFileDict(self.files,path) #rv
        myfiles['st_uid'] = uid
        myfiles['st_gid'] = gid

    def create(self, path, mode):
    
    
        value = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        
        setFileDict(self.files,path,value)       
               
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        
        if not pathCheck(self.files,path):#rv
            raise FuseOSError(ENOENT)
      
        return getFileDict(self.files,path) #rv

    def getxattr(self, path, name, position=0):
        
        print "attr name"
        print name
        print "xattr position"
        print position
        
        myfiles = getFileDict(self.files,path)
        attrs = myfiles.get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        myfiles = getFileDict(self.files,path) #rv
        attrs = myfiles.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        value = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        
        setFileDict(self.files,path,value)
               
        # self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        return ['.', '..'] + [x[1:] for x in self.files if x != '/'] ## rv: DOUBT ##

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        myfiles=getFileDict(self.files,path) #rv
        attrs = myfiles.get('attrs', {})  #rv

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        #myfiles=getFileDict(self.files,path) #rv
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        myfiles=getFileDict(self.files,path) #rv
        
        attrs = myfiles.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
        self.data[path] = self.data[path][:length]
        myfiles=getFileDict(self.files,path) #rv
     
        myfiles['st_size'] = length

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)

        myfiles=getFileDict(self.files,path) #rv
        myfiles['st_atime'] = atime
        myfiles['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        self.data[path] = self.data[path][:offset] + data
        myfiles = getFileDict(self.files,path)    ## rv
        myfiles['st_size'] = len(self.data[path]) ## rv
        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True,debug=True)
