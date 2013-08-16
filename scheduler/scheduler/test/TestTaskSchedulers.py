'''
Created on 07/02/2013

@author: victor
'''
import unittest
import time

def test_without_params():
    time.sleep(1)

def test_with_params(seconds):
    time.sleep(seconds)
    
def add_processes(scheduler):
    """"
    Builds a dependency structure like this one:
    
        1    7  8
       / \      |
      2  3      9
      | / \
      4 5 6
    """"
    
    scheduler.add_task(task_name = string(1), target_function, function_kwargs, description)
    
class TestTaskSchedulers(unittest.TestCase):
    
    
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