#!/usr/bin/env python

import xmlrpclib
from xmlrpclib import Binary
import pickle

import logging
from collections import defaultdict
import errno  #For Error number, ENOENT, ENOTEMPTY and ECONNREFUSED
from errno import ENOENT
from errno import ENOTEMPTY
from errno import ECONNREFUSED
from errno import EAGAIN

from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


if not hasattr(__builtins__, 'bytes'):
    bytes = str
    
class ServerHandler():
    
    def __init__(self,Qr,Qw,metaServer,dataServer):
        
        self.Qr = Qr
        self.Qw = Qw
        self.numServer = len(dataServer)
                
        #handlers to manage communication with remote XML-RPC servers
        self.portAddress = []
        self.rpcHandler = []
        self.serverStatus = []

        addr = 'http://localhost:' + metaServer
        self.portAddress.append(addr)
        self.rpcHandler.append(xmlrpclib.ServerProxy(addr))
        
        for temp in dataServer:
            addr = 'http://localhost:' + temp
            self.portAddress.append(addr)
            self.rpcHandler.append(xmlrpclib.ServerProxy(addr))
        
        self.initializeRoot()
        self.connectDataServers(True)
        
        self.underRestart = False
            
    def connectDataServers(self,start):    
        
        self.serverStatus = []
        for i in range(1,self.numServer+1):
            status = self.tryConnection(i)
            self.serverStatus.append(status)
            
            #if the servers are in R state for the first time the system is started => initialize restart flag on servers
            if status == 'R' and start == True:
                self.putSingleDataServer('/',"restart flag", i)
            
                
        print("Server status: %s" %str(self.serverStatus))
        
    
    def initializeRoot(self):
        now = time()
        tempdict = {}
        
        #Try to get root info if already present
        #server may be still running, so do not overwrite root if it is already there
        tempdict = self.tryConnection(0)
        if tempdict == 'N':
            exit(1)
                            
        if  tempdict == 'R':        
            #root not found, set root as "/"
            #using two arrays files and dirs inside the meta dictionary 
            #to track contents created below a directory.
            meta_dict = dict(st_mode = (S_IFDIR | 0755), st_ctime = now, st_mtime = now, 
            st_atime = now, st_nlink = 2, files = [], dirs = [])
            
            self.putMetaServer('/',meta_dict)
            self.putMetaServer('fd',0)
            
            print("Root was not found, root initialized : %s" %str(meta_dict))
            
        else:
            #root is already found on server
            #dont overwrite, just modify access time
            tempdict['st_atime'] = now
            print("Root was found, root updated : %s" %str(tempdict))
            self.putMetaServer('/',tempdict)
  
    def tryConnection(self, num):
        
        try:
            if(num == 0):
                tempdict = self.getMetaServer('/')
            else:
                tempdict = self.getDataServer('/',num)
        except KeyError:
            return 'R'  
        except EnvironmentError as msg:        
            if msg.errno == errno.ECONNREFUSED:
                if(num == 0): name = "Meta"
                else: name = "Data " + str(num)
                print("%s Server connection refused" % name)
                return 'N'
            else:
                raise msg
        
        if(num == 0): return tempdict  #Return root info for meta server
        else: return 'Y'               #Return Y (active) for data server
        
    
    def findIndex(self,mylist,value):
        return [i for i, x in enumerate(mylist) if x == value]
        
    def putMetaServer(self, key, value, ttl=3000):
        self.rpcHandler[0].put(Binary(key),Binary(pickle.dumps(value)),ttl)
    
    def getMetaServer(self, key):
        return pickle.loads(self.rpcHandler[0].get(Binary(key))["value"].data)
    
    def removeServer(self, key, i):
        return self.rpcHandler[i].remove(Binary(key))
    
    def putSingleDataServer(self, key, value, num, ttl=3000):
        
        # write to specific server indicated by num
        self.rpcHandler[num].put(Binary(key),Binary(pickle.dumps(value)),ttl) 

        
    def putDataServers(self, key, value):
        
        self.connectDataServers(False)
        #### Check if any server restarted from blank state ####
        ### using self.underRestart flag so that restart handler does not get called during recursrive read
        if self.serverStatus.count("R") > 0 and self.underRestart == False:
            print "Calling HandleRestart from put data"
            self.underRestart = True
            self.HandleRestart()
            
        activeServers = self.serverStatus.count("Y")
        
        #if Qw servers are not active upon a write request return with error
        if  activeServers < self.Qw:
            return False
        
        #Iterate in order to write to Qw servers
        for i in range(1,self.Qw+1):
            print("Putting key %s - value %s in server %d" %(key,value,i))
            self.putSingleDataServer(key,value,i)
                
    def getDataServer(self, key, i):
        return pickle.loads(self.rpcHandler[i].get(Binary(key))["value"].data)
        
    def HandleRestart(self):
        print "At handle restart"
        #get array of servers that were restarted
        restartList = self.findIndex(self.serverStatus,"R")
        print("serverstatus %s - restartList %s " %(self.serverStatus,restartList))
        
        activeServer = self.serverStatus.index('Y')
        keyList = self.rpcHandler[activeServer+1].list_content()
        print("activeServer+1 %d - keyList %s " %(activeServer+1,keyList))
                
        for restartedServer in restartList:
            rs = int(restartedServer)+1
            print("rs : %d " %rs)
            
            for key in keyList:
                print("For each key %s" %key)
                if key != "/":
                    data = self.readDataServer(key,"read")
                    if data == False:
                        print("Unable to restore %s key for %d data server as \
                              read quorum for remaining servers (for restoration) was not met" %(key,restartedServer+1))
                    else:
                        self.putSingleDataServer(key, data, restartedServer+1)
                        print("%s key for %d data server is successfully restored with data %s" %(key,restartedServer+1,data))
            
            #reset the restart flag for the restarted server
            self.putSingleDataServer('/',"restart flag", restartedServer+1)
            
        #Check for connection status after restart
        self.underRestart = False
        self.connectDataServers(False) 
    
    def insertIntoParent(self, path, item):
        x = path[:path.rfind("/")]
        y = path[path.rfind("/"):]
        if not x: x = '/'
        tempdict1 = self.getMetaServer(x)
        
        #add file/directory created to its parent's list
        tempdict1[item].append(y)
        
        #increment st_nlink of parent if the item created is a directory
        if item == 'dirs':
            tempdict1['st_nlink'] += 1
            
        self.putMetaServer(x,tempdict1)
        
    def removeFromParent(self, path, item):
        x = path[:path.rfind("/")]
        y = path[path.rfind("/"):]
        if not x: x = '/'
        tempdict1 = self.getMetaServer(x)
        
        #remove file/directory created to its parent's list
        tempdict1[item].remove(y)
        
        #decrement st_nlink of parent if the item removed is a directory
        if item == 'dirs':
            tempdict1['st_nlink'] -= 1
        
        self.putMetaServer(x,tempdict1)
    
    def incrementFD(self):
        tempfd = self.getMetaServer('fd')
        tempfd += 1
        self.putMetaServer('fd',tempfd)
        return tempfd
        
    def readDataServer(self,key,request):
    
        self.connectDataServers(False)
        
        #### Check if any server restarted from blank state ####
        ### using self.underRestart flag so that restart handler does not get called during recursrive read
        if self.serverStatus.count("R") > 0 and self.underRestart == False:
            print "Calling HandleRestart"
            self.underRestart = True
            self.HandleRestart()
            
        activeServers = self.serverStatus.count("Y")
        
        if request == "read": count = self.Qr
        else: count = self.Qw
           
        #if quorum servers are not active upon a read/write request return with error
        
        print("activeServers : %d count : %d" %(activeServers,count))
        if  activeServers < count:
            return False
            
        quorum = self.Qr
        
        i = quorum   #To track read quorom
        j = 0        #To tracker data server id
        k = 0        #Loop counter to track number of accessed servers
        
        data = ['-'] * self.numServer
        matchCount = [0] * self.numServer
        
        print("Start of while: data - %s matchcount - %s - i %d " %(str(data), str(matchCount), i))
                
        while i>0:
            
            if self.serverStatus[j] == 'Y':         
                tempData = self.getDataServer(key, j+1)
                print("ServerID %d Accessedserver k %d tempData %s" %(j+1,k,str(tempData)))
                
                #First server read - no comparisons to make
                if k == 0:
                    i -= 1
                    data[j] = tempData
                    matchCount[j] += 1
                
                #Iterate through data array to match
                else:
                    l = 1  #To track data match count
                    h = 1  #To iterate through data array
                    atleast1Match = "N"
                    
                    for h in range(0,self.numServer):
                        if data[h] != '-':
                            if data[h] == tempData:
                                matchCount[h] += 1
                                l += 1
                                atleast1Match = "Y"
                                
                    
                    #If atleast one match was found decrement 
                    if atleast1Match == "Y":
                        i -= 1      
                    data[j] = tempData
                    matchCount[j] += l
                k += 1
                    
            #Goto to next server
            j += 1

            print("End of while: data - %s matchcount - %s - i %d " %(str(data), str(matchCount), i))
            
            #Break out of while loop if Qr was met or all servers were checked
            print("At if condition: i = %d - j = %d " %(i,j))
            if i == 0 or j == self.numServer:
                break          
       
        index = matchCount.index(max(matchCount))
        print("Finally Out index %d data at index data[index] %s" %(index,data[index]))
        
        #if number of servers accessed match the maximum match count 
        #=> Data read from all accessed servers match => #No data restorations are required
        #=> Return data from any server with the correct data
        if k == matchCount.count(max(matchCount)):  
            print("Data of all servers accessed so far match and quorum is met k is %d index is %d" %(k,index))
            return data[index]
            
        else:
           
            #Qr was met but before returning TRY to check for corruption and restore if necessary
            if i == 0:
                try:
                    #Restore data on corrupted server if there are any
                    corruptIndex = matchCount.index(1)+1
                    print("corruptIndex : %d" %corruptIndex)
                    if corruptIndex > 0:      
                        print("corrpution Handled Bad data %s on server %d restored to %s" %(data[corruptIndex-1],corruptIndex,data[index])) 
                        self.putSingleDataServer(key,data[index],corruptIndex)
                    #if index 1 is not found ignore value error - there are no corrupt servers
                except ValueError:
                    pass
                
                #return correct data
                return data[index]
            else:
                return False
                
class Memory(LoggingMixIn, Operations):

    def __init__(self,Qr,Qw,metaServer,dataServer):
        
        #SH stands for server handler
        self.SH = ServerHandler(Qr,Qw,metaServer,dataServer)
        
    def chmod(self, path, mode):
        
        print("<<<<<<<<<<< Function chmod - %s >>>>>>>>>>>>>" %path)
        
        tempdict = self.SH.getMetaServer(path)
        tempdict['st_mode'] &= 0770000
        tempdict['st_mode'] |= mode
        self.SH.putMetaServer(path,tempdict) 
        return 0

    def chown(self, path, uid, gid):
        
        print("<<<<<<<<<<< Function chown - %s >>>>>>>>>>>>>" %path)
        
        tempdict = self.SH.getMetaServer(path)
        tempdict['st_uid'] = uid
        tempdict['st_gid'] = gid
        self.SH.putMetaServer(path,tempdict) 

    def create(self, path, mode):
        
        print("<<<<<<<<<<< Function create %s >>>>>>>>>>>>>" %path)
        
        tempdict = dict(st_mode=(S_IFREG | mode), st_nlink = 1, st_size = 0, st_ctime = time(), 
                        st_mtime = time(), st_atime = time(),files = [],dirs = [], empty = "Y")
        self.SH.putMetaServer(path,tempdict)
        
        #insert file name as a content in its parent directory files array
        self.SH.insertIntoParent(path,'files')
        return self.SH.incrementFD()

    def getattr(self, path, fh=None):
        
        print("<<<<<<<<<<< Function getattr - %s >>>>>>>>>>>>>" %path)
        
        try:
            tempdict = self.SH.getMetaServer(path)
            if not tempdict:
                raise FuseOSError(ENOENT)
            return tempdict  
        except KeyError:
            raise FuseOSError(ENOENT)

    def getxattr(self, path, name, position=0):
        
        print("<<<<<<<<<<< Function getxattr - %s >>>>>>>>>>>>>" %path)
        
        try:
            tempdict = self.SH.getMetaServer(path) 
            attrs = tempdict.get('attrs', {})
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        
        print("<<<<<<<<<<< Function listxattr - %s >>>>>>>>>>>>>" %path)
        
        try:
            tempdict = self.SH.getMetaServer(path) 
            attrs = tempdict.get('attrs', {})
            return attrs.keys()
        except KeyError:
            return ''       # Should return ENOATTR

    def mkdir(self, path, mode):
        
        print("<<<<<<<<<<< Function mkdir - %s >>>>>>>>>>>>>" %path)
        
        tempdict = dict(st_mode = (S_IFDIR | mode), st_nlink = 2,st_size = 0, st_ctime = time(), 
                        st_mtime = time(), st_atime = time(),files = [],dirs = [])
                                                       
        self.SH.putMetaServer(path,tempdict)
        
        #insert directory name as a content in its parent directory dirs array
        #increment st_nlink of parent directory
        self.SH.insertIntoParent(path,'dirs')

    def open(self, path, flags):
        
        print("<<<<<<<<<<< Function open - %s >>>>>>>>>>>>>" %path)
        return self.SH.incrementFD()

    def read(self, path, size, offset, fh):
        
        print("<<<<<<<<<<< Function read - %s >>>>>>>>>>>>>" %path)
        tempdict = self.SH.getMetaServer(path)
        
        if tempdict['empty'] == 'Y':
            return ''
        else:
            tempdata = self.SH.readDataServer(path,'read')
            
        if tempdata == False:
            raise FuseOSError(EAGAIN)
        else:
            return tempdata[offset:offset + size]

    def readdir(self, path, fh):
        
        print("<<<<<<<<<<< Function readdir - %s >>>>>>>>>>>>>" %path)
        
        #return all the contents tracked by arrays files and dirs
        tempdict = self.SH.getMetaServer(path)
        return ['.', '..'] + [x[1:] for x in tempdict['files']] + [x[1:] for x in tempdict['dirs']]

    def readlink(self, path):
        
        print("<<<<<<<<<<< Function readlink - %s >>>>>>>>>>>>>" %path)
        
        try:
            tempdata = self.SH.getDataServer(path)
            return tempdata
        except KeyError:
            raise FuseOSError(ENOENT)


    def removexattr(self, path, name):
        
        print("<<<<<<<<<<< Function removexattr - %s >>>>>>>>>>>>>" %path)
        
        #getattr gets called first to check for path keyerror so we dont have to check it again here
        try:
        
            tempdict = self.SH.getMetaServer(path) 
            attrs = tempdict.get('attrs', {})
            del attrs[name]
            tempdict['attrs'] = attrs
            self.SH.putMetaServer(path,tempdict)
            
        except KeyError:
            pass        # Should return ENOATTR
    
    def rename(self, old, new):
        
        print("<<<<<<<<<<< Function rename - %s >>>>>>>>>>>>>" %path)
        
        olddict = self.getMetaServer(old)
        link = olddict['st_nlink']
        if link == 1: item = 'files'
        else: item = 'dirs'
        
        #TODO: Change TTL to remove
        newdict = olddict
        olddict = {}     
        self.putMetaServer(old,olddict)
        self.putMetaServer(new,newdict)
        
        #because the file may or may not be empty, so "try" to copy data
        try:                        
            olddata = self.getDataServer(old)
            newdata = olddata
            olddata = {}
            self.putDataServers(old,olddata)
            self.putDataServers(new,newdata)
        except KeyError:
            pass    # Should return ENOATTR

        #Remove old entry from its parent
        self.removeFromParent(old,item)
        
        #Add new entry to its parent
        self.insertIntoParent(new,item)
      
        #If a directory is renamed, change the keys recursively for all items inside it
        if link != 1: self.changepath(old, new)
    
    def changepath(self, old, new):
        
        tempdict = self.SH.getMetaServer(new)      
        
        for eachitem in tempdict['files']:
            olditem = old+eachitem
            newitem = new+eachitem
            
            olddict = self.SH.getMetaServer(olditem) 
            newdict = olddict
            olddict = {}
            self.SH.putMetaServer(olditem,olddict)
            self.SH.putMetaServer(newitem,newdict)
            
            #because the file may or may not be empty, so "try" to copy data
            try:                        
                
                olddata = self.SH.readDataServer(olditem) 
                newdata = olddata
                olddata = {}
                self.putDataServers(olditem,olddata)
                self.putDataServers(newitem,newdata)
            except KeyError:
                pass
            
        for eachitem in tempdict['dirs']:
            olditem = old+eachitem
            newitem = new+eachitem
            
            olddict = self.SH.getMetaServer(olditem)
            newdict = olddict
            olddict = {}
            self.SH.putMetaServer(olditem,olddict)
            self.SH.putMetaServer(newitem,newdict)
           
            #if this is a directory do this recursively for items inside it
            if newdict['st_nlink'] != 1:
                self.changepath(olditem, newitem)
    
    
    def rmdir(self, path):
    
        print("<<<<<<<<<<< Function rmdir - %s >>>>>>>>>>>>>" %path)
        
        tempdict = self.SH.getMetaServer(path)
        length = len(tempdict['files'])+len(tempdict['dirs'])
        
        #if there are no contents under this folder, only then remove it
        if length == 0: 
            
            self.SH.removeServer(path, 0)

            #remove directory name as a content in its parent directory dirs array
            #decrement st_nlink of parent directory
            self.SH.removeFromParent(path,'dirs')
            
        else:
            raise FuseOSError(ENOTEMPTY) #Should return ENOTEMPTY when dir is not empty, rmdir fails
        

    def setxattr(self, path, name, value, options, position=0):
        
        print("<<<<<<<<<<< Function setxattr - %s >>>>>>>>>>>>>" %path)
        
        # Ignore options
        try:
            tempdict = self.SH.getMetaServer(path)  
            attrs = tempdict.get('attrs', {})
            attrs[name] = value
            tempdict['attrs'] = attrs
            self.SH.putMetaServer(path,tempdict)
        except KeyError:
            return ''       # Should return ENOATTR

    def statfs(self, path):
    
        print("<<<<<<<<<<< Function statfs - %s >>>>>>>>>>>>>" %path)
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        
        print("<<<<<<<<<<< Function symlink - target: %s source: %s  >>>>>>>>>>>>>" %(target,source))
        
        tempdict = dict(st_mode = (S_IFLNK | 0777), st_nlink = 1,st_size = len(source))
        self.SH.putMetaServer(target,tempdict)
        
        #For symbolic link file (key=target) data points to source file path (value=source)
        self.SH.putDataServers(target,source)
        
        #Add symbolic link entry to its parent
        self.SH.insertIntoParent(target,'files')
                
    def truncate(self, path, length, fh=None):
        
        print("<<<<<<<<<<< Function truncate - %s >>>>>>>>>>>>>" %path)
        
        tempdict = self.SH.getMetaServer(path)
        
        if tempdict['empty'] == 'Y':
            return
        
        #Truncate the length of data
        tempdata = self.SH.readDataServer(path,"write")  
        print("Data returned tempdata : %s" %str(tempdata))
        
        if tempdata == False:
            raise FuseOSError(EAGAIN)
        else:
            tempdata = tempdata[:length]
            self.SH.putDataServers(path,tempdata)
            
            #Update new length in meta information
            tempdict = self.SH.getMetaServer(path)
            tempdict['st_size'] = length
            self.SH.putMetaServer(path,tempdict)
                
    def unlink(self, path):
        
        print("<<<<<<<<<<< Function unlink - %s >>>>>>>>>>>>>" %path)
        
        #delete data from server by setting ttl = 0
        tempdict = self.SH.getMetaServer(path)
        self.SH.removeServer(path, 0)
        
        #Remove file entry from parent
        self.SH.removeFromParent(path,'files')
        
        if tempdict['empty'] == 'Y':
            return
        
        for i in range(1,self.SH.numServer+1):    
            try:                       
                print("Putting ttl 0 for server %d" %i)
                self.SH.removeServer(path, i)  
            except KeyError:
                pass

        
    def utimens(self, path, times=None):
        
        print("<<<<<<<<<<< Function utimens - %s >>>>>>>>>>>>>" %path)
        
        now = time()
        atime, mtime = times if times else (now, now)
        tempdict = self.SH.getMetaServer(path)
        tempdict['st_atime'] = atime
        tempdict['st_mtime'] = mtime
        self.SH.putMetaServer(path,tempdict)
        
    def write(self, path, data, offset, fh):
        
        print("<<<<<<<<<<< Function write - %s >>>>>>>>>>>>>" %path)
        
        
        tempdict = self.SH.getMetaServer(path)
        
        if tempdict['empty'] == 'Y':
            tempdata = ''
        else:
            tempdata = self.SH.readDataServer(path,'write')
        
        if tempdata == False:
            raise FuseOSError(EAGAIN)
        else:
            
            #Write Data to file
            tempdata = tempdata[:offset] + data
            length = len(tempdata)
            self.SH.putDataServers(path,tempdata)
            
            #Update length and empty flag in Meta information
            tempdict['st_size'] = length
            tempdict['empty'] = 'N'
            self.SH.putMetaServer(path,tempdict) 
            return length


if __name__ == '__main__':
    if len(argv) <6:
        print('usage: %s <mountpoint> <QuorumRead - Qr> <QuorumWrite - Qw> <Meta-Sever> <Data-Server-1> <Data-Server-2> ... <<Data-Server-n>' % argv[0])
        exit(1)
        
    #Parse parameters
    mountPoint = argv[1]
    Qr = int(argv[2])
    Qw = int(argv[3])
    metaServer = argv[4]
    dataServer = argv[5:]
    numServers = len(dataServer)
    errorCheck = 'N'
    
    #Check Parameters
    if Qr < 1 or Qr > numServers:
        print('Error: Qr - %d is out of range' % Qr)
        errorCheck = 'Y'
    
    if Qw != numServers:
        print('Error: Qw - %d is not equal to the number of Replica Servers - %d in the system' % (Qw,numServers))
        errorCheck = 'Y'
    
    if errorCheck == 'Y':
        exit(1)
    
    
    print('\n***************************** FUSE FileSystem Information ******************************\n')
    print('                         Mount Point : %s' %mountPoint )
    print('                         Read Quorum : %d' % Qr)
    print('                        Write Quorum : %d' % Qw)
    print('                    Meta Server Port : %s' % metaServer)
    print('                 Data Server Port(s) : %s' % dataServer)
    print('\n****************************************************************************************\n')

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(Qr,Qw,metaServer,dataServer), mountPoint, foreground=True, debug=False)
