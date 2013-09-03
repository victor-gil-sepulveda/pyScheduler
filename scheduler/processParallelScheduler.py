'''
Created on 16/05/2012

@author: victor
'''
from scheduler.serialScheduler import SerialScheduler
import multiprocessing


def run_task(task):
    """
    """
    return task.run()

class ProcessParallelScheduler(SerialScheduler):  
    
    def __init__(self, max_processes):
        SerialScheduler.__init__(self)
        self.max_processes = max_processes
        
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        pool = multiprocessing.Pool(processes = self.max_processes)
        ordered_tasks = self.get_ordered_tasks()
        if ordered_tasks != []:
            self.results = pool.map(run_task, ordered_tasks)
        pool.close()
        pool.join()
        self.ended()
        return self.results
  
