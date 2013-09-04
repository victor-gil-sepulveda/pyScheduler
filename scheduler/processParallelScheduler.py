'''
Created on 16/05/2012

@author: victor
'''
from scheduler.serialScheduler import SerialScheduler
import multiprocessing

def run_task(task,):
    """
    """
    return task.run()

def add_result(result):
    print "Returned", result

class ProcessParallelScheduler(SerialScheduler):  
    
    def __init__(self, max_processes):
        SerialScheduler.__init__(self)
        self.max_processes = max_processes
    
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        pool = multiprocessing.Pool(processes = self.max_processes)
        deferred_results = []
        # Check that dependencies are OK
        while len(self.not_completed) > 0:
            #Choose an available process
            task_name = self.choose_runnable_task()
            
            if task_name is not None:
                # Run a process
                deferred_results.append(pool.apply_async(run_task, (self.tasks[task_name],), callback = add_result))
       
        pool.close()
        pool.join()
        self.ended()

        for def_result in deferred_results:
            def_result.wait()
            self.results.append(def_result.get())
        
        return self.results
  
        