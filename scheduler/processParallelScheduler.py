'''
Created on 16/05/2012

@author: victor
'''
from scheduler.scheduler.serialScheduler import SerialScheduler
import multiprocessing

class ProcessParallelScheduler(SerialScheduler):  
    
    def __init__(self, max_processes):
        SerialScheduler.__init__(self)
        self.max_processes = max_processes
        self.results_queue = multiprocessing.Queue()
        
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        pool = multiprocessing.Pool(processes = self.max_processes)
        lock = multiprocessing.Lock()
        
        # Apply the function 
        while len( self.not_completed) > 0:
            #Choose an available process
            task_name = self.choose_runnable_task()
            
            if task_name is None :
                print "It was impossible to pick a suitable task for running. Check dependencies."
                break;
            else:
                # Run a process
                pool.apply_async(self.run_task, args = (task_name,lock))
                self.remove_task(task_name)
                # List tasks
                #self.list_tasks()
        pool.close()
        pool.join()
        self.ended()
        return self.results
  
    def run_task(self, task_name, lock):
        """
        """
        lock.acquire()
        self.results_queue.put(self.tasks[task_name].run())
        lock.release()
