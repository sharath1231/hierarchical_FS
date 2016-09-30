import xmlrpclib
from xmlrpclib import Binary
import pickle
import termios
import os

import sys, tty
from sys import argv, exit
import errno
from errno import ECONNREFUSED

class ServerManager():    
   
    def __init__(self,serverList):
    
        self.serverList = serverList
        print serverList
        self.numServers  = len(serverList)
        self.portNum     = []
        self.portAddress = []
        self.rpcHandler  = []

        for temp in self.serverList:
            self.portNum.append(temp)
            addr = 'http://localhost:' + temp
            self.portAddress.append(addr)
            self.rpcHandler.append(xmlrpclib.ServerProxy(addr))
        
        print str(self.portAddress)
        print str(self.rpcHandler)
            
    def ServerInfo(self):
        
        print('\n***************************** Server Information ******************************\n')
        
        for i in range(0,self.numServers):
            if i == 0: name = "Meta"
            else: name = "Data"
            
            ret = self.tryConnection(i)
            if ret == True: status = "Active"
            else: status = "Inactive"
            
            print('                    %s Server Port : %s - ServerID : %d - Status : %s' % (name,self.serverList[i], i,status))
            
        print('\n*******************************************************************************\n')
    
    def ListMethods(self):
        print 'Option - m : List available methods on any data server \n'
        i=1
        flag='N'
        
        while i<= self.numServers-1:
            
            if self.tryConnection(i) == True:
                print 'Available methods on the data server system are: '
                print self.rpcHandler[i].system.listMethods()
                flag='Y'
                break
            i += 1
                
        if flag == 'N':
            print("All data servers are currently inactive")
    
    def GetandValidate(self):
        print('Enter Data Server ID \n ')
        ch = obj.getch()
        print ch
        
        i = int(ch)
        
        if i < 1 or i > self.numServers-1:
            print "Invalid Data Server ID"
            return False
        
        if self.tryConnection(i) == False:
            print("Data Server %d currently inactive" %i)
            return False
        
        return i
    
    def StartTermValidate(self):
        print('Enter Server ID \n ')
        ch = obj.getch()
        print ch
        
        i = int(ch)
        
        if i < 0 or i > self.numServers-1:
            print "Invalid Server ID"
            return -1
        
        return i
    
    def Terminate(self):
        print 'Option - t : Terminate any server (data/meta) \n'
        
        i = self.StartTermValidate()
        
        if i != -1:
            if self.tryConnection(i) == False:
                print("Server %d already inactive" %i)
                return
            
            ret = self.rpcHandler[i].shutdown()           
            print("Server %d terminated" %i)
    
    def ListContents(self):
        
        print 'Option - l : List keys present on data server \n'
        
        i = self.GetandValidate()
        if i != False:
            print("Keys of Data Server %d : " %i)
            ret = self.rpcHandler[i].list_content()
            print ret
    
    def PrintContents(self):
        
        print 'Option - p :  Print contents present on data server \n'
        
        i = self.GetandValidate()
        if i != False:
            print("Content of Data Server %d : " %i)
            ret = self.rpcHandler[i].print_content()
            print ret
            
    def QuitProgram(self):
        print 'Option - q: Exiting! \n'
        exit(0)
        
    def CorruptData(self):
        print 'Option - c: Corrupt Data on Server! \n'
        i = self.GetandValidate()
        if i != False:
            ret = self.rpcHandler[i].list_content()
            print ret
            
            if len(ret) == 0:
                print("Server is empty. No key to delete")
                return
                
            print("Enter any key from the list above")
            key = raw_input()
            if key not in ret:
                print("Invalid Key")
                return
            
            value = "?$#&^\n"
            self.rpcHandler[i].corrupt(key, value, 3000)
    
    def tryConnection(self,i):
        
        try:
            tempdict = self.getServer('/',i)
        except KeyError:
            return True 
        except EnvironmentError as msg: 
            if msg.errno == errno.ECONNREFUSED:
                return False
            else:
                raise msg
        return True
    
    def getServer(self,key,i):
        return pickle.loads(self.rpcHandler[i].get(Binary(key))["value"].data)  
    
    def getch(self):
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch
    
    def Start(self):
        
        print 'Option - s : Start any server (data/meta) \n'
        
        i = self.StartTermValidate()
        
        if i != -1:

            if self.tryConnection(i) == True:
                print("Server %d already active" %i)
                return False
                
            syscall = "python simpleht.py --port " + self.portNum[i]
            syscall = "'" + syscall + "'"
            print syscall
            os.system("gnome-terminal -e "+ syscall)
            
        
    
    def PrintMenu(self):
        print('\n***************************** Server Management Options ******************************\n')
        print('                               Keypress           Action')
        print('                               --------           ------')
        print('                                  i               Print Server Information and Status') 
        print('                                  l               List Contents (keys) of server')
        print('                                  p               Print Contents of given server')
        print('                                  m               List Available Methods of any Data Server')
        print('                                  c               Corrupt Data for given key and server')
        print('                                  s               Start a server')
        print('                                  t               Terminate a server')
        print('                                  o               Print Server Options')
        print('                                  q               Quit Server Manager')
        
    
    
if __name__ == '__main__':
    if len(argv) <3:
        print('usage: %s <Meta-Sever-Port> <Data-Server-Port> <Data-Server-Port> ... <<Data-Server-Port>' % argv[0])
        exit(1)
    
    metaServer = []
    metaServer.append(argv[1])
    dataServer = argv[2:]
    
    obj = ServerManager(metaServer+dataServer)
    obj.PrintMenu()
    
    while True:
                
        print('\n Waiting for a keypress !! \n ')
        ch = obj.getch()
        print ("Entered Key : %s" %ch)
        
        try:
            options = {'i' : obj.ServerInfo, 'l' : obj.ListContents, 'p' : obj.PrintContents, 'q' : obj.QuitProgram, 'm' : obj.ListMethods, 't' : obj.Terminate, 'c' : obj.CorruptData, 'o' : obj.PrintMenu, 's' : obj.Start}
            options[ch]()
        except KeyError:
            print("Invalid Option !! ")
            
            
