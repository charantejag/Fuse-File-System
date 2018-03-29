import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from sys import argv, exit
from multiprocessing import Pool

quit = 0



# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)

  def count(self):
    # Remove expired entries
    self.next_check = datetime.now() - timedelta(minutes = 5)
    self.check()
    return len(self.data)

  # Retrieve something from the HT
  def get(self, key):
    # Remove expired entries
    self.check()
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results
    key = key.data
    if key in self.data:
      ent = self.data[key]
      now = datetime.now()
      if ent[1] > now:
        ttl = (ent[1] - now).seconds
        rv = {"value": Binary(ent[0]), "ttl": ttl}
      else:
        del self.data[key]
    return rv

  # Insert something into the HT
  def put(self, key, value, ttl):
    # Remove expired entries
    self.check()
    end = datetime.now() + timedelta(seconds = ttl)
    self.data[key.data] = (value.data, end)
    return True
  
  def list_contents(self) :
    c = {}
    c = self.data
    d = c.keys()
    print c.keys()
    return c.keys()  
    
  def troubleshoot(self,key):
    print "entering troubleshoot function"
    ans = self.get(Binary(key))
    return pickle.loads(ans["value"].data)
  
  def corrupt(self,key):
    print "entering corrupt function"
    res = self.get(Binary(key))
    if "value" in res:
      d = pickle.loads(res["value"].data)
    else:
      print "unable to corrupt the value",key
      return None
    corrupted_value = d + "abc"
    print "corrupted value of ",key, "is ",corrupted_value
    self.put(Binary(key),Binary(corrupted_value), 6000)
    print "corrupted the value successfully"
    return corrupted_value
      
  
  def terminate(self):
    global quit
    quit = 1
    return 1
  
    
  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

  # Remove expired entries
  def check(self):
    now = datetime.now()
    if self.next_check > now:
      return
    self.next_check = datetime.now() + timedelta(minutes = 5)
    to_remove = []
    for key, value in self.data.items():
      if value[1] < now:
        to_remove.append(key)
    for key in to_remove:
      del self.data[key]
       
def main(argv):
  server_list=[]
  for x in argv[1:]:
    server_list.append(int(x))
     
  
  p = Pool(processes=len(sys.argv)-1)
  p.map(serve,server_list)   
   
# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.list_contents)
  file_server.register_function(sht.corrupt)
  file_server.register_function(sht.troubleshoot)
  file_server.register_function(sht.terminate)
  file_server.register_function(sht.write_file)
  print 'server will be starting on '+ 'port:' + str(port)
  while quit == 0 :
    file_server.serve_forever()




if __name__ == "__main__":
  if len(argv) < 2:
     print 'usage: %s <port for local instance> <port for data-server2> ..<port for data-server-n> ' % argv[0]
  main(sys.argv)
