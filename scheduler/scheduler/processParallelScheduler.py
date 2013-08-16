'''
Created on 16/05/2012

@author: victor
'''
from multiprocessing import Process
import time
from threading import Thread, Lock
from pyproclust.driver.observer.observable import Observable

class ProcessLauncher(Thread, Observable):
    def __init__(self,process, update_guard, processes_dict, running_processes_list, finished_processes_list, observer = None):
        Thread.__init__(self)
        Observable.__init__(self,observer)
        self.process = process
        self.update_guard = update_guard
        self.processes_dict_proxy = processes_dict
        self.currently_running_processes_list_proxy = running_processes_list
        self.finished_processes_proxy = finished_processes_list
        
        
    def run(self):
        # Update lists (process will start to run, and will no longer be available
        # for execution (so we delete it from the tasks dictionary)
        self.update_guard.acquire()
        self.currently_running_processes_list_proxy.append(self.process)
        del self.processes_dict_proxy[self.process.name]
        self.update_guard.release()
        
        # Notify the process starts
        self.update_guard.acquire()
        self.notify("Process Begin", {
                                        "process": self.process.name,
                                        "description": self.process.description                                       
        })
        self.update_guard.release()
        
        # Start the process
        self.process.start()
        # Wait for termination
        self.process.join()
        
        # Notify that the process has ended
        self.update_guard.acquire()
        self.notify("Process End", {
                                        "process": self.process.name,
                                        "description": self.process.description                                       
        })
        self.update_guard.release()
        
        #Update lists
        self.update_guard.acquire()
        self.currently_running_processes_list_proxy.remove(self.process)
        self.finished_processes_proxy.append(self.process.name)
        self.update_guard.release()
    
class ProcessParallelScheduler(Observable):  
    
    def __init__(self, max_processes_at_the_same_time, sleep_time = 30, observer = None):
        super(ProcessParallelScheduler,self).__init__(observer)
        
        self.finished = []
        self.tasks = {}
        self.dependencies = {}
        self.currently_running_processes = []
        self.max_processes = max_processes_at_the_same_time
        self.num_added_processes = 0
        self.sleep_time = sleep_time
        self.thread_guard = Lock()
    
    def consume(self):
        while not len(self.finished) == self.num_added_processes:
            
            # Run a process
            self.run_next_process()
            
            # Listing
            self.list_processes()
            
            # wait
            time.sleep(self.sleep_time) # 1 sec
        
        self.notify("Ended","")
    
    def list_processes(self):
        self.thread_guard.acquire()
        
        process_list = {
                        "Running":[],
                        "Idle":[],
                        "Ended":[]
        }
        
        for p in self.currently_running_processes:
            process_list["Running"].append(p.name)
        
        for p in self.tasks:
            process_list["Idle"].append(self.tasks[p].name)
        
        for p in self.finished:
            process_list["Ended"].append(p)
        
        self.notify("Process List", process_list)
        self.thread_guard.release()
    
    def run_next_process(self):
        self.thread_guard.acquire()
        process = self.get_not_dependant_process()
        self.thread_guard.release()
        
        if process == None:
            self.thread_guard.acquire()
            if len(self.finished) == len(self.tasks.keys()):
                self.notify("No Process Available", "")
            self.thread_guard.release()
        else:
            self.thread_guard.acquire()
            number_of_processes_being_executed = len(self.currently_running_processes)
            self.thread_guard.release()
            
            if number_of_processes_being_executed < self.max_processes:
                ProcessLauncher(process, 
                                self.thread_guard, 
                                self.tasks, 
                                self.currently_running_processes,  
                                self.finished, 
                                self.observer).start()
            
    def get_not_dependant_process(self):
        for process_name in sorted(self.tasks.keys()):
            if len(self.dependencies[process_name]) == 0:
                return self.tasks[process_name]
            else:
#                print "No independent process."
                found_dependency  = False
                for d in self.dependencies[process_name]:
                    if not d in self.finished :
                        found_dependency = True
                        self.notify("Dependency",{"process":process_name,"dependency":d})
                if not found_dependency:
                    return self.tasks[process_name]
#        print "No suitable process."
        return None
    
    def add_process(self, process_name, description, target_function, function_kwargs, dependencies = []):
        if not process_name in self.tasks.keys():
            process = Process(target = target_function, name = process_name, kwargs=function_kwargs)
            process.description = description
            self.tasks[process_name] = process
            self.dependencies[process_name] = dependencies
            self.num_added_processes += 1
        else:
            print "[Error ProcessPool::add_process] process already exists:", process_name
            exit()
        
    def next_process_id (self):
        return self.num_added_processes
