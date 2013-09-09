'''
Created on 16/05/2012

@author: victor
'''
from mpi4py import MPI
from pyscheduler.serialScheduler import SerialScheduler

class MPIParallelScheduler(SerialScheduler):  
    
    def __init__(self):
        SerialScheduler.__init__(self)
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.number_of_processes = self.comm.Get_size()
        print self.number_of_processes, "processes"
        self.running = []
        
    def run(self, share_results_with_all_processes = False):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        # Check that dependencies are OK
        
        # Wait till all processes are available
        self.comm.Barrier()
        # Execute all tasks
        # rank 0 is the controller
        
        busy_processes = [False]*(self.number_of_processes)
        busy_processes[0] = True
        if self.rank == 0:
            # controller
            print "I am the 0th, so the controller"
        else:
            # task executing
            print "I am a slave", self.rank
        
        available_workers = self.number_of_processes - 1
        while not len(self.finished) == len(self.tasks):
            if self.rank == 0:
                # controller
                cannot_choose_a_task = False
                #Choose an available task
                task_name = self.choose_runnable_task()
                if task_name is not None:
                    # If we can still execute a task we do it (dependency-free task)
                    # but first we need to have an available process
                    available_process = None
                    try:
                        available_process = busy_processes.index(False)
                    except:
                        pass
                    if available_process is not None:
                        self.comm.send(("EXECUTE",task_name), dest = available_process, tag=1)
                        self.lock_task(task_name) # from now on this task is not available for choosing
                        self.running.append(task_name)
                        busy_processes[available_process] = True
                else:
                    cannot_choose_a_task = True
                    
                if cannot_choose_a_task or len(self.running) == available_workers:
                    # Wait for a result
                    ended_task_name, result, sender_rank = self.comm.recv(source = MPI.ANY_SOURCE, tag = 2)
                    self.results.append(result)
                    self.running.remove(ended_task_name)
                    self.complete_task(ended_task_name)
                    self.remove_from_dependencies(ended_task_name)
                    busy_processes[sender_rank] = False
            else:
                # task executing
                # Wait for a task number to be executed
                message, task_name = self.comm.recv(source = 0, tag = 1)
                if message == "EXECUTE":
                    # Execute it
                    result = self.tasks[task_name].run()
                    # Tell controller that we have finished by sending the result
                    self.comm.send((task_name,result, self.rank), dest = 0, tag = 2)
                elif message == "FINISH":
                    break # exit the loop
        
        print self.rank, "reached main loop end"

        if self.rank == 0:
            for i in range(1,self.number_of_processes):
                self.comm.send(("FINISH",None), dest = i, tag=1)
        
        self.ended()
        
        if share_results_with_all_processes:
            self.results = self.comm.bcast(self.results, root=0)
            
        return self.results
          
        