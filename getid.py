from time import *
import mmap
import os
from ctypes import c_ushort, c_ulong
from multiprocessing import Pool,Process, Event, Lock, Value
import cProfile, pstats

#globals used below in a test
glock = Lock()


#used to simulate node_id
global_id =0
'''
'''
class GlobalId:

    def __init__(self):
        #node ID
        self.__node_id = -1
        #semaphore
        self.__lock = Lock()
        #file mem map
        self.__rmap = None

        if not os.path.exists('/tmp'):
            os.makedirs('/tmp')
        fname = "/tmp/count_" + str(self.node_id())
        #make sure the file is set initially with value of 0
        #can't mmap an empty file
        if not os.path.exists(fname):
            with open(fname, "wb+") as f:
                f.write((0).to_bytes(8, byteorder='big'))

    #Maintain a counter between 0-100000 in a file
    #When the method is called return the previous value
    #and write the next one into the file.
    #The access is protected for multi processing
    def get_count(self):
        try:
            fname = "/tmp/count_"+str(self.node_id())
            # protect the critical section
            self.__lock.acquire()
            #use mmap to speed up file I/O
            if not self.__rmap:
                with open(fname,"rb+") as f:
                    self.__rmap = mmap.mmap(f.fileno(),length=8, access=mmap.ACCESS_WRITE)
                    #read 8 bytes from the file
                    count = self.__rmap[0:8]
                    count = int.from_bytes(count, "big")
                    #increment the counter by one
                    #wrap around at 100K
                    next = (count + 1)%100000
                    next=next.to_bytes(8, "big")
                    #write it back to the file
                    self.__rmap[0:8] = next
                    self.__rmap.flush()
            self.__lock.release()
            #as long as we return only after we have updated the file with the new value
            # we are safe in case of the node crash
            return count
        except Exception as e:
            print("Exception: {}".format(e))
            return None

    #helper methods to get 2 and 4 byte sized integers
    def hex16(self, data):
        '''16bit int->hex converter'''
        return '0x%004x' % (c_ushort(data).value)
    def hex32(self, data):
        '''32bit int->hex converter'''
        return '0x%008x' % (c_ulong(data).value)

    def get_id(self):
        #timestamp
        t = self.timestamp()
        #convert to seconds
        t = int(t/1000)
        #take 32 lower bits
        t = int(self.hex32(t), base=16)

        #node id
        n = self.node_id()
        #take 16 bits
        #technically we need 10 bits only
        n = int(self.hex16(n), base=16)

        #current counter
        c = self.get_count()
        #take 16 bits
        #technically we need 17 bits here for 100K
        c = int(self.hex16(c), base=16)

        #concatenate to one 64 bit id
        z = int(f"{t}{n}{c}", base=16)
        #print("z {:x}".format(z))
        return z

    def node_id(self):
        #mock node_id method
        #return single id per object
        global global_id
        if self.__node_id == -1:
            if global_id >= 1023:
                raise Exception("Not allowed more than 1024 nodes")
            self.__node_id = global_id
            global_id += 1
        return self.__node_id

    def timestamp(self):
        #mock timestamp method
        return int(time())*1000




