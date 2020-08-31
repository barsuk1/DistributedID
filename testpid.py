from getid import *
import  argparse

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
        print("Starting Process {}".format(i))
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
        print("Starting Process {}".format(i))
        p = Process(target=testAll, args=(i, glock, gcount, gevent) )
        p.start()
    while gcount.value < num:
        sleep(50 / 1000000.0)
    gevent.set()

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Test getid functionaliyt')

    parser.add_argument('--num', type=int,
                        default=os.cpu_count(),
                        help='Number of processes to start')
    args = parser.parse_args()
    print("Process test one by one")
    doMPTest(args.num)
    print("Process test all at once")
    doMPAllTest(args.num)
