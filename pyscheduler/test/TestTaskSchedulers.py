'''
Created on 07/02/2013

@author: victor
'''
#TODO: Test coverage must be improved (however it is being indirectly tested in pyProCT)

import unittest
import time
from pyscheduler.serialScheduler import SerialScheduler
#from pyscheduler.processParallelScheduler import ProcessParallelScheduler
#from pyscheduler.MPIParallelScheduler import MPIParallelScheduler


def sleep(this):
    print this, "is going to sleep"
    time.sleep(this)
    print this, "has woken up"
    return this

def echo(this):
    return this

def add_tasks(scheduler, test_function):
    """"
    Feeds a scheduler with tasks so that it builds a dependency structure like this:

        1    7  8
       / \      |
      2  3      9
      | / \
      4 5 6
    """
    scheduler.add_task(task_name = "1", dependencies = ["2","3"], description ="",target_function = test_function ,function_kwargs={"this":1})
    scheduler.add_task(task_name = "2", dependencies = ["4"], description ="",target_function = test_function ,function_kwargs={"this":2})
    scheduler.add_task(task_name = "3", dependencies = ["5","6"], description ="",target_function = test_function ,function_kwargs={"this":3})
    scheduler.add_task(task_name = "4", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":4})
    scheduler.add_task(task_name = "5", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":5})
    scheduler.add_task(task_name = "6", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":6})
    scheduler.add_task(task_name = "7", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":7})
    scheduler.add_task(task_name = "8", dependencies = ["9"], description ="",target_function = test_function ,function_kwargs={"this":8})
    scheduler.add_task(task_name = "9", dependencies = [], description ="",target_function = test_function ,function_kwargs={"this":9})

class TestTaskSchedulers(unittest.TestCase):

    def testSerialScheduling(self):
        serial = SerialScheduler()
        add_tasks(serial, echo)
        self.assertItemsEqual([4, 2, 5, 6, 3, 1, 7, 9, 8], serial.run())

# Uncomment tests to try the other schedulers. Note that to test MPI scheduler it has to be invoked using 'mpirun'.

#     def testParallelScheduling(self):
#         parallel = ProcessParallelScheduler(4)
#         add_tasks(parallel, sleep)
#         results = parallel.run()
#         self.assertItemsEqual([4, 2, 5, 6, 3, 1, 7, 9, 8], results)

#     def testMPIParallelScheduling(self):
#         parallel = MPIParallelScheduler()
#         add_tasks(parallel, sleep)
#         results = parallel.run()
#         print results
#         self.assertItemsEqual([4, 2, 5, 6, 3, 1, 7, 9, 8], results)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_ProcessParallelScheduler']
    unittest.main()