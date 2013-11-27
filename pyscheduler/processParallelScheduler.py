'''
Created on 16/05/2012

@author: victor
'''
import multiprocessing
import pyscheduler
from pyscheduler.serialScheduler import SerialScheduler
import sys

def printnflush(*args):
    """
    Prints and flushes the things passes as arguments.
    @param args: The data we want to print.
    """
    if pyscheduler.verbose:
        print args
        sys.stdout.flush()

def run_task(process_name, tasks, pipe_end):
    """
    Helper function to run tasks inside a process. It implements an infinite loop controlled by the messages
    received from 'pipe_end'.
    Messages from the pipe are (message_type, value) tuples. Thsi is the currently implemented protocol:
    - "EXECUTE": Runs the task with id == value.
        -> Sends a "TASK FINISHED" message with value = (task_id, task_result)
    - "FINISH": Ends the loop so that process can end and free its resources.
    @param process_name: Unique id of the process executing this function.
    @param tasks: The dictionary of all tasks (we want to execute in this scheduling) indexed by their id .
    @param pipe_end: A process pipe used to send/receive messages from/to the master.
    """
    task_ended = False
    try:
        while not task_ended:
            # Blocks until it receives a message
            message_type, value = pipe_end.recv()

            if message_type == "EXECUTE":
                result = tasks[value].run()
                pipe_end.send(("TASK FINISHED", (value, result)))

            elif message_type == "FINISH":
                printnflush( "Communication successfully closed for",process_name)
                task_ended = True
            else:
                printnflush("Unexpected message: %s"%message_type)
                task_ended = True

    except EOFError:
        printnflush("Communication closed due to remote closing of the pipe in process %s"%process_name)

    except Exception, msg:
        printnflush("Communication closed due to unexpected exception: %s"%msg)

    pipe_end.close()
    printnflush( "Task reached end")

class TaskRunner(object):
    """
    Helper class that encapsulates a process used to execute a subset of the tasks list.
    """
    def __init__(self, process_name, target_function, tasks):
        """
        Creates the process that will be in charge of executing the tasks and a pipe to communicate
        with the main process.
        @param process_name: Unique id for this task executor.
        @param target_function: Is the function the process will execute. In the case of ProcessParallelScheduler
        the function used is 'run_task', however it can use any function that receives the same parameters that
        'run_task' needs.
        @param tasks: The dictionary of all tasks.
        """
        self.pipe_start, self.pipe_end =  multiprocessing.Pipe()
        printnflush ("Process started: %s"%process_name)
        self.process = multiprocessing.Process(group=None,
                                               target=target_function,
                                               name=process_name,
                                               args = (process_name, tasks, self.pipe_end))
        self.busy = False

    def run(self):
        """
        Starts the inner process (and therefore the defined function that is going to be used to control the
        messages).
        """
        self.process.start()

    def execute_task(self, task_name):
        """
        Sends the process an "EXECUTE" task message to run the task named 'task_name'.
        @param task_name: Name of the task to be executed.
        """
        self.busy = True
        self.pipe_start.send(("EXECUTE",task_name))

    def set_task_finished(self):
        """
        Sets the 'busy' flag in order to mark this task executor as busy (its associated process is
        performing a task)
        """
        self.busy = False

    def finalize(self):
        """
        Sends a finalization message (forces the associated process to break the loop and end)-
        """
        self.busy = False
        self.pipe_start.send(("FINISH",None))
        self.process.join()
        if self.process.is_alive():
            self.process.terminate()

    def has_an_incomming_message(self):
        """
        True if this task runner has received a message from its associated process.
        """
        return self.pipe_start.poll(1)

    def get_message(self):
        """
        Returns the message the associated process sent (using the 'run_task' function it can only be a
        "TASK FINISHED" message)
        """
        return self.pipe_start.recv()

class ProcessParallelScheduler(SerialScheduler):
    """
    Scheduler type that works by creating a limited number of processes and distributing the tasks between them.
    """

    def __init__(self, max_processes, functions = {}):
        """
        Creates the scheduler.
        @param max_processes: Indeed is the total number of processes that will be used for the scheduling parallelization
        plus one (which is representing the current process).
        @param functions: @see SerialScheduler
        """
        SerialScheduler.__init__(self,functions)
        self.number_of_processes = max_processes - 1
        self.running = []

    def run(self):
        """
        Like in the SerialScheduler, this function tries to run all the tasks, checking their dependencies. In this case
        some processes will be spawned so that they can share the work of executing the tasks.
        This run function acts as the real scheduler, telling the 'task executor' objects which task to run. This kind
        of dynamic scheduling fosters an efficient use of the resources (every time a 'task executor' ends a task, it is
        told to run another one, so that load is balanced).
        This is a simple implementation of a master-slave pattern (where slaves are the task runners).
        """
        self.function_exec('scheduling_started', {"number_of_tasks":len(self.not_completed)})

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
                        self.lock_task(task_name) # Ensure that it can't be selected again until task is finished
                        self.running.append(task_name)
                        break
            else:
                cannot_choose_a_task = True

            if cannot_choose_a_task or len(self.running) == available_workers:
                # If there is not an available task (so all remaining tasks have dependencies) or
                # we do not have any available worker, it's time to block until we receive results.

                # We start polling busy runners pipes to wait for a result and add this result to the
                # results list
                task_finished = False
                while not task_finished:
                    for task_runner in task_runners:
                        if task_runner.busy and task_runner.has_an_incomming_message():
                            message, value  = task_runner.get_message()
                            if message == "TASK FINISHED":
                                task_name, result = value
                                self.function_exec('task_ended', {"task_name":task_name, "finished":len(self.finished)})
                                self.running.remove(task_name)
                                self.complete_task(task_name)
                                self.remove_from_dependencies(task_name)
                                task_runner.set_task_finished()
                                self.results.append(result)
                            else:
                                printnflush ( "Unexpected message: %s"%message)
                                exit()
                            task_finished = True

        printnflush ("Sending processes termination message.")

        for task_runner in task_runners:
            task_runner.finalize()

        self.function_exec('scheduling_ended')

        return self.results
