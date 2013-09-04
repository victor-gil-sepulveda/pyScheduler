'''
Created on 16/05/2012

@author: victor
'''
import multiprocessing
from scheduler.serialScheduler import SerialScheduler

def run_task(process_name, tasks, pipe_end, pipe_lock, results_queue):
    try:
        while True:
            pipe_lock.acquire()
            message_type, value = pipe_end.recv()
            pipe_lock.release()
            if message_type == "EXECUTE":
                results_queue.put(tasks[value].run())
                #print "Sending task finished",value
                pipe_end.send(("TASK FINISHED", value))
            elif message_type == "FINISH":
                results_queue.close()
                results_queue.join_thread()
                pipe_end.send(("AKCK",None))
                print "Communication successfully closed for",process_name
                break
            else:
                print "Unexpected message:", message_type
                exit()
    except EOFError:
        print "Communication closed due to remote closing of the pipe in process", process_name
    except Exception, msg:
        print "Communication closed due to unexpected exception", msg

class ProcessParallelScheduler(SerialScheduler):  
    
    def __init__(self, max_processes):
        SerialScheduler.__init__(self)
        self.number_of_processes = max_processes -1
        self.running = []
        
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        results_queue = multiprocessing.Queue(len(self.not_completed))

        # Check that dependencies are OK
        
        #Process setup
        # Generate max_processes
        processes= []
        pipe_start, pipe_end =  multiprocessing.Pipe()
        pipe_lock = multiprocessing.Lock()
        for i in range(self.number_of_processes):
            process_name = "TaskExecutor"+str(i)
            process = multiprocessing.Process(group=None, target=run_task, name=process_name, args=(process_name, self.tasks,pipe_end,pipe_lock,results_queue))
            process.start()
            processes.append(process)
        
        # Execute all tasks
        available_workers = self.number_of_processes
        while not len(self.finished) == len(self.tasks):
            cannot_choose_a_task = False
            if len(self.running) < available_workers:
                #Choose an available process
                task_name = self.choose_runnable_task()
                if task_name is not None:
                    # If we can still execute a task we do it (dependency-free task)
                    pipe_start.send(("EXECUTE",task_name)) # We send a global message, the first to get it will be the one to
                    # execute the task
                    self.lock_task(task_name) # from now on this task is not available for choosing
                    self.running.append(task_name)
                else:
                    cannot_choose_a_task = True
                
            if cannot_choose_a_task or len(self.running) == available_workers:
                # If there is not an available task (so all remaining tasks have dependencies) or
                # we do not have any available worker, it's time to block until we receive results.
                #print "Main process waiting for task to end", task_name, len(self.finished), len(self.tasks)
                message, value  = pipe_start.recv()
                if message == "TASK FINISHED":
                    self.running.remove(value)
                    self.complete_task(value)
                    self.remove_from_dependencies(value)
                    #print "Main process freed", self.not_completed
                else:
                    print "Unexpected message:",message
                    exit()
        
        #print "Sending processes termination message."
        for i in range(self.number_of_processes):
            pipe_start.send(("FINISH",None))
            pipe_start.recv()
        
       
        #print "Joining processes."
        for process in processes:
            process.join()
        #print "Main loop exited. Closing pipes."
        pipe_start.close()
        pipe_end.close()
        
        # Get results (unordered)
        while not results_queue.empty():
            self.results.append(results_queue.get())
        
        self.ended()
        return self.results
