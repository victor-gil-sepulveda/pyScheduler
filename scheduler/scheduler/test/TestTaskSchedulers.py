'''
Created on 07/02/2013

@author: victor
'''
import unittest
import time
from scheduler.serialScheduler import SerialScheduler

def test_without_params():
    time.sleep(1)

def test_with_params(seconds):
    time.sleep(seconds)

def echo(this):
    return this

def add_tasks(scheduler):
    """"
    Builds a dependency structure like this one:
    
        1    7  8
       / \      |
      2  3      9
      | / \
      4 5 6
    """
    scheduler.add_task(task_name = "1", dependencies = [], description ="",target_function = echo ,function_kwargs={"this":1})
    scheduler.add_task(task_name = "2", dependencies = ["1"], description ="",target_function = echo ,function_kwargs={"this":2})
    scheduler.add_task(task_name = "3", dependencies = ["1"], description ="",target_function = echo ,function_kwargs={"this":3})
    scheduler.add_task(task_name = "4", dependencies = ["2"], description ="",target_function = echo ,function_kwargs={"this":4})
    scheduler.add_task(task_name = "5", dependencies = ["3"], description ="",target_function = echo ,function_kwargs={"this":5})
    scheduler.add_task(task_name = "6", dependencies = ["3"], description ="",target_function = echo ,function_kwargs={"this":6})
    scheduler.add_task(task_name = "7", dependencies = [], description ="",target_function = echo ,function_kwargs={"this":7})
    scheduler.add_task(task_name = "8", dependencies = [], description ="",target_function = echo ,function_kwargs={"this":8})
    scheduler.add_task(task_name = "9", dependencies = ["8"], description ="",target_function = echo ,function_kwargs={"this":9})
    
    results = scheduler.run()
    
    
class TestTaskSchedulers(unittest.TestCase):
    
    def testSerialScheduling(self):
        serial = SerialScheduler()
        add_tasks(serial)
#     def test_ProcessParallelScheduler(self):
#          
#         # Without dependencies!
#         pool = ProcessParallelScheduler(2, sleep_time = 1)
#         pool.add_process("Process 1", "First process",  test_without_params, {})
#         pool.add_process("Process 2", "Second process", test_with_params, {"seconds":4})
#         pool.add_process("Process 3", "Third process",  test_with_params, {"seconds":3})
#         pool.consume()
#   
#         expected = [{'action': '[Process Begin]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                                         {'action': '[Process Begin]', 'message': {'process': 'Process 2', 'description': 'Second process'}}, 
#                                         {'action': '[Process End]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                                         {'action': '[Process Begin]', 'message': {'process': 'Process 3', 'description': 'Third process'}}, 
#                                         {'action': '[Process End]', 'message': {'process': 'Process 2', 'description': 'Second process'}}, 
#                                         {'action': '[Process End]', 'message': {'process': 'Process 3', 'description': 'Third process'}}] 
#   
#         self.assertItemsEqual(expected, pool.observer.messages["ProcessLauncher"])
#           
#         # And with dependencies!
#         pool = ProcessParallelScheduler(2, sleep_time = 1)
#         pool.add_process("Process 1", "First process", test_without_params, {})
#         pool.add_process("Process 2", "Second process", test_with_params, {"seconds":3}, ["Process 1"])
#         pool.add_process("Process 3", "Third process", test_with_params, {"seconds":2},  ["Process 1"])
#         # This may oblige p1 to be executed and finished alone even if we have 2 slots
#         pool.consume()
#           
#         expected = [{'action': '[Process Begin]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 2', 'description': 'Second process'}}, 
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 3', 'description': 'Third process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 2', 'description': 'Second process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 3', 'description': 'Third process'}}]
#           
#         self.assertItemsEqual(expected, pool.observer.messages["ProcessLauncher"])
#           
#         # With three slots all have to start
#         pool = ProcessParallelScheduler(3, sleep_time = 1)
#         pool.add_process("Process 1", "First process", test_without_params, {})
#         pool.add_process("Process 2", "Second process", test_with_params, {"seconds":3} )
#         pool.add_process("Process 3", "Third process", test_with_params, {"seconds":2} )
#         # This may oblige p1 to be executed and finished alone even if we have 2 slots
#         pool.consume()
#          
#         expected = [{'action': '[Process Begin]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 2', 'description': 'Second process'}}, 
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 3', 'description': 'Third process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 3', 'description': 'Third process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 2', 'description': 'Second process'}}]
#  
#          
#         self.assertItemsEqual(expected, pool.observer.messages["ProcessLauncher"])
#          
#         # With three slots and dependencies, the behavior is exactly equal than the one having 2 slots
#         pool = ProcessParallelScheduler(3, sleep_time = 1)
#         pool.add_process("Process 1", "First process", test_without_params, {})
#         pool.add_process("Process 2", "Second process", test_with_params, {"seconds":3}, ["Process 1"])
#         pool.add_process("Process 3", "Third process", test_with_params, {"seconds":2},  ["Process 1"])
#         # This may oblige p1 to be executed and finished alone even if we have 2 slots
#         pool.consume()
#           
#         expected = [{'action': '[Process Begin]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 1', 'description': 'First process'}}, 
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 2', 'description': 'Second process'}}, 
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 3', 'description': 'Third process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 2', 'description': 'Second process'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 3', 'description': 'Third process'}}]
#   
#         self.assertItemsEqual(expected, pool.observer.messages["ProcessLauncher"])
# 
#     def test_SerialScheduler(self):
#         pool = SerialScheduler(sleep_time = 1)
#         pool.add_process("Process 1", "First process",  test_without_params, {})
#         pool.add_process("Process 2", "Second process", test_with_params, {"seconds":4})
#         pool.add_process("Process 3", "Third process",  test_with_params, {"seconds":3})
#         pool.consume()
# 
#         expected = [{'action': '[Process Begin]', 'message': {'process': 'Process 1'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 1'}}, 
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 2'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 2'}},
#                     {'action': '[Process Begin]', 'message': {'process': 'Process 3'}}, 
#                     {'action': '[Process End]', 'message': {'process': 'Process 3'}}]
#         self.assertItemsEqual(expected, pool.observer.messages["SerialProcess"])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_ProcessParallelScheduler']
    unittest.main()