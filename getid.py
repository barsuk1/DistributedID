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
Basically the best and simplest solution to the question would be to ues SQL to generate unigue id
It can be achieved with CREATE TABLE idtable ( ID INT IDENTITY( @node_id,1024) NOT NULL PRIMARY KEY )
ie generate Ids for the table starting with node_id and using increment of number of nodes. 
However below is my attempt on it without using SQL, simply by storing a counter in a memory mapped file
and appending node id and counter to the timestampe  
    
get_id :
Use timestamp, node ID and counter to create a unique ID:
Algorithm should be:
1. Round timestamp to the number of seconds
2. Shift it 30 bits left 
3. Append node ID ( 10 bits to store 0-1023 number)
4. Append counter ( we need 17 bits to store 100K counter, so 20 bits should be enough)
Explanation:
Timestamp will change every second. Node id is uinque per node, and counter is guaranteed to advance 
with every call and can be more than maximum requests per second we should support.
In plain C it should be this:
uint64_t ts = timestamp();
uint64_t nodeId = node_id();
uint64_t counter = get_counter();
uint64_t id = (ts<<30)|((nodeId& 0x3ff)<<20)|(counter& 0xfffff)

It is difficult to create integer of a specific size in Python
ctype helper methods used in the code show one example how it can be done through the call
to ctype, but int type in Python does not have a size limit.
A better approach would be to writ e a method in a plain C as mentioned above and call it from Python  
Not doing it here due to the time constraints
So the below implementation is an approximation of the algorithm described. Sorry...

Note also that it makes sense to define all the variables as class variables here, but 
I haven't done it here since I thought of creating the tests that will simulate creating all the objects locally 
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

#Test emulates get_id() method
def test(i):
    #create an object an call get_id() on it
    g = GlobalId()
    g.get_id()

def testAll(num, glock,gcount, gevent):
    glock.acquire()
    gcount.value += 1
    glock.release()
    gevent.wait()
    cProfile.runctx('test(num)', globals(), locals(),  'profile-%d.out' %num)

#Test wrapper in order to profile only the get_id()
#running time
def profile_worker(num):
    #print some profiling info for each test to the correspondign file
    cProfile.runctx('test(num)', globals(), locals(),  'profile-%d.out' %num)

#Simple test that runs number of processes requesting get_id
#In a real word scenario communications play an important part,
#So if eg client asks node get_id() via HTPP we should also measure
#the node ability to create all the connections and send responses
def doMPTest(num=os.cpu_count()):
    #run each test in a separate process
    for i in range(num):
        #Each process is started one by one in the loop.
        p = Process(target=profile_worker, args=(i,) )
        p.start()
        p.join()

def doMPAllTest(num=os.cpu_count()):
    gcount = Value('i', 0)
    gevent = Event()
    gevent.clear()
    for i in range(num):
        #In order to test for maximum contention
        #all the process will wait in the testAll
        #on an event upon start, the event will be signaled when
        #process count reaches N (100000), so all of them will go in simultaneuosly.
        print("Startin {}".format(i))
        p = Process(target=testAll, args=(i, glock, gcount, gevent) )
        p.start()
    while gcount.value < num:
        sleep(50 / 1000000.0)
    gevent.set()

if __name__=='__main__':
   doMPTest()
   doMPAllTest()


