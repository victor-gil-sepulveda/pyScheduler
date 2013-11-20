'''
Created on 16/05/2012

@author: victor
'''
import multiprocessing
from pyscheduler.serialScheduler import SerialScheduler
import sys

def printnflush(*args):
    print args
    sys.stdout.flush()
    
def run_task(process_name, tasks, pipe_end):
    task_ended = False
    try:
        while not task_ended:
            message_type, value = pipe_end.recv()
                
            if message_type == "EXECUTE":
#                 print "Running task",value
                result = tasks[value].run()
#                 print "Sending task finished",value
                pipe_end.send(("TASK FINISHED", (value, result)))
                
            elif message_type == "FINISH":
                printnflush( "Communication successfully closed for",process_name)
                task_ended = True
            else:
                print "Unexpected message:", message_type
                task_ended = True
    
    except EOFError:
        print "Communication closed due to remote closing of the pipe in process", process_name
    
    except Exception, msg:
        print "Communication closed due to unexpected exception", msg
    
    pipe_end.close()
    printnflush( "Task reached end")

class TaskRunner(object):
    
    def __init__(self, process_name, target_function, tasks):
        self.pipe_start, self.pipe_end =  multiprocessing.Pipe()
        print "process started", process_name
        self.process = multiprocessing.Process(group=None, target=target_function, name=process_name, args = (process_name,
                                                                                                              tasks,
                                                                                                              self.pipe_end))
        self.busy = False
    
    def run(self): 
        self.process.start()
    
    def execute_task(self, task_name):
#         print "Executing task", task_name
        self.busy = True
        self.pipe_start.send(("EXECUTE",task_name))
    
    def set_task_finished(self):
        self.busy = False
        
    def finalize(self):
        self.busy = False
        self.pipe_start.send(("FINISH",None))
#         printnflush("joining")
        self.process.join()
        if self.process.is_alive():
            self.process.terminate()
    
    def has_an_incomming_message(self):
        return self.pipe_start.poll(1)
    
    def get_message(self):
        return self.pipe_start.recv()

class ProcessParallelScheduler(SerialScheduler):
    
    def __init__(self, max_processes, functions = {}):
        SerialScheduler.__init__(self,functions)
        self.number_of_processes = max_processes - 1
        self.running = []
        
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        self.function_exec('scheduling_starts', {"number_of_tasks":len(self.not_completed)})
        # Check that dependencies are OK
        
        # Create processes
        available_workers = self.number_of_processes
        task_runners = []
        for i in range(available_workers):
            process_name = "TaskExecutor"+str(i)
            runner = TaskRunner(process_name, run_task, self.tasks)
            runner.run()
            task_runners.append(runner)
        
        # Execute all tasks
        while not len(self.finished) == len(self.tasks):
            cannot_choose_a_task = False
            
            # Choose an available process
            task_name = self.choose_runnable_task()
            
            # Try to execute it
            if task_name is not None:
                # If we can still execute a task we find a free task runner to do it
                for task_runner in task_runners:
                    if not task_runner.busy:
                        self.function_exec('task_started', {"task_name":task_name})
                        task_runner.execute_task(task_name)
                        self.lock_task(task_name)
                        self.running.append(task_name)
                        break
            else:
                cannot_choose_a_task = True
                
            if cannot_choose_a_task or len(self.running) == available_workers:
                # If there is not an available task (so all remaining tasks have dependencies) or
                # we do not have any available worker, it's time to block until we receive results.
                
                # We start polling busy runners pipes to wait for a result
                task_finished = False
                while not task_finished:
                    for task_runner in task_runners:
                        if task_runner.busy and task_runner.has_an_incomming_message():
                            message, value  = task_runner.get_message()
                            if message == "TASK FINISHED":
                                task_name, result = value
                                self.function_exec('task_ended', {"task_name":task_name})
                                self.running.remove(task_name)
                                self.complete_task(task_name)
                                self.remove_from_dependencies(task_name)
                                task_runner.set_task_finished()
                                self.results.append(result)
                            else:
                                print "Unexpected message:",message
                                exit()
                            task_finished = True
        
        print "Sending processes termination message."
        
        for task_runner in task_runners:
            task_runner.finalize()
        
        self.function_exec('scheduling_ended')
        
        return self.results
