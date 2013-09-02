'''
Created on 16/05/2012

@author: victor
'''
from multiprocessing import Process, Lock
from scheduler.scheduler.serialScheduler import SerialScheduler
import multiprocessing

# class TaskTree(object):
#     
#     def __init__(self, task, father = None):
#         self.children = []
#         self.father = father
#         self.task = task
#         
#     def get_root(self):
#         if self.father== None:
#             return self
#         else:
#             return self.get_root(self.father)
#     
#     def get_tree_tasks(self):
#         tasks = [self.task]
#         for child in self.children:
#             tasks.extend(self.get_tree_tasks(child))
#         return tasks
#     
#     def is_leaf(self):
#         return len(self.children) == 0
#     
# class BottomUpVisitor:
#     def __init__(self, action):
#         self.action_on_visit = action
#         
#     def visit(self, tree):
#         results = []
#         if tree.is_leaf(): # then is a leave
#             return [self.action_on_visit()]
#         else:
#             for child in self.children:
#                 results.extend(self.visit(child))
#         return results
#     
class ProcessParallelScheduler(SerialScheduler):  
    
    def __init__(self, max_processes, wait_time = 30):
        SerialScheduler.__init__(self)
        self.max_processes = max_processes
    
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        pool = multiprocessing.Pool(processes = self.max_processes)
        async_results = []
        
        # Apply the function 
        while len( self.not_completed) > 0:
            #Choose an available process
            task_name = self.choose_runnable_task()
            
            if task_name is None :
                print "It was impossible to pick a suitable task for running. Check dependencies."
                break;
            else:
                # Run a process
                async_results.append(pool.apply_async(self.run_task, kwds = task_name))
                self.remove_task(task_name)
                # List tasks
                #self.list_tasks()
        pool.close()
        pool.join()
        self.ended()
        return self.results
  
    def run_task(self, task_name):
        """
        """
        return self.tasks[task_name].run()

