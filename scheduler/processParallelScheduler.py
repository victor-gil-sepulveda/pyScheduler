'''
Created on 16/05/2012

@author: victor
'''
from scheduler.serialScheduler import SerialScheduler
import multiprocessing

class ProcessParallelScheduler(SerialScheduler):  
    
    def __init__(self, max_processes):
        SerialScheduler.__init__(self)
        self.max_processes = max_processes
        
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        pool = multiprocessing.Pool(processes = self.max_processes)
        ordered_tasks = []
        print "pool created"
        while len( self.not_completed) > 0:
            #Choose an available process
            task_name = self.choose_runnable_task()
            if task_name is None :
                print "It was impossible to pick a suitable task for running. Check dependencies."
                return []
            else:
                ordered_tasks.append((task_name,))
                self.remove_task(task_name)
        print ordered_tasks
        pool.map(self.run_task, ordered_tasks)
        pool.close()
        pool.join()
        self.ended()
        return self.results
  
