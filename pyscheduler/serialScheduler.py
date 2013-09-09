'''
Created on 16/08/2012

@author: victor
'''
class SerialTask(object):
    def __init__(self, function, name, kwargs, description):
        self.function = function
        self.name = name
        self.kwargs = kwargs
        self.result = None
    
    def run(self):
        self.result = self.function(**(self.kwargs))
        return self.result
    
class SerialScheduler(object):
    """
    Serial scheduler that iteratively builds the dependency tree. It ensures that any task is executed before its dependencies.
    """
    
    def __init__(self, functions = {}):
        """
        Constructor. Initializes needed variables.
        
        @param fucntions: A dictionary containing 3 possible keys. Each key defines another dictionary of two 
        entries ('function' and 'kwargs') with a callable and its arguments. The possible keys are:
            'task_start' -> Were an action performed after each task is called is defined.
            'task_end' -> Defines the action performed when a task is finished.
            'scheduling_end' -> Defines the action performed when the scheduler has finished to run all tasks.
        """
        self.functions = functions
        self.tasks = {}
        # Example of dependencies dictionary: {"task_C":["dep_task_A", "dep_task_B"]}, this means task C cannot be run 
        # until task B and A are cleared.
        self.dependencies = {}
        self.not_completed = []
        self.finished = []
        self.results = []
        
    def function_exec(self, function_type, task_name = None):
        """
        Tries to execute one of the predefined functions.
        
        @param function_type: Type of the function to check and run (proper types should be 'task_start','task_end'
        and 'scheduling_end', each defining 'function' and 'kwargs' entries.
        
        """
        if function_type in self.functions:
            if task_name is not None:
                self.functions[function_type]['kwargs']['task_name'] = task_name
            self.functions[function_type]['function'](**(self.functions[function_type]['kwargs']))
    
    def run(self):
        """
        Tries to run all the tasks in a way that tasks are not executed before their dependencies are
        cleared.
        
        @return: An array with the results of task calculations.
        """
        ordered_tasks = self.get_ordered_tasks()
        
        for task in ordered_tasks:
            self.function_exec('task_start', task.name)
            self.results.append(task.run())
            self.function_exec('task_end', task.name)
            
        self.function_exec('scheduling_end')
        
        return self.results
    
    def get_ordered_tasks(self):
        """
        Returns a list of task names so that any task name will have an index bigger than the tasks it depends on. 
        
        @return: A list of task names.
        """
        ordered_tasks = []
        while len( self.not_completed) > 0:
            #Choose an available process
            task_name = self.choose_runnable_task()
            
            if task_name is None:
                print "It was impossible to pick a suitable task for running. Check dependencies."
                return []
            else:
                # Run a process
                ordered_tasks.append(self.tasks[task_name])
                self.lock_task(task_name)
                self.complete_task(task_name)
                self.remove_from_dependencies(task_name)
        return ordered_tasks
    
    def choose_runnable_task(self):
        """
        Returns a task name which dependencies have already been fulfilled.
        
        @return: The task name.
        """
        for task_name in self.not_completed:
            if len(self.dependencies[task_name]) == 0: # This process has no dependencies
                return task_name;
        return None # All task have dependencies (circular dependencies for instance)
    
    
    def lock_task(self, task_name):
        """
        Removes a task from the 'not complete list' making it unavailable for further selections.
        
        @param task_name: The name of the task to lock.
        """
        # Remove it from the not_completed list
        self.not_completed.remove(task_name)
    
    def complete_task(self, task_name):
        """
        Adds a task to the list of completed tasks.
        
        @param task_name: The name of the task to complete.
        """
        self.finished.append(task_name)
    
    def remove_from_dependencies(self, task_name):
        """
        Removes a task from the dependencies of all other uncomplete tasks. At the end of execution, all dependency 
        lists must be empty.
        
        @param task_name: The name of the task to remove from dependencies.
        """
        for tn in self.dependencies:
            if task_name in self.dependencies[tn]:
                self.dependencies[tn].remove(task_name)
        
    def add_task(self, task_name, dependencies, target_function, function_kwargs, description):
        """
        Adds a task to the scheduler. The task will be executed along with the other tasks when the 'run' function is called.
        
        @param task_name: 
        @param dependencies: A list with the task_names of the tasks that must be fulfilled before executing this other task.
        @param target_function: The function executed by this task.
        @param function_kwargs: Its arguments.
        @param description: A brief description of the task.
        """
        
        if not task_name in self.tasks:
            task = SerialTask( name = task_name, description = description, function = target_function, kwargs=function_kwargs)
            task.description = description
            self.tasks[task_name] = task
            self.not_completed.append(task_name)
            self.dependencies[task_name] = dependencies
        else:
            print "[Error SerialScheduler::add_task] Task %s already exists. Task name must be unique."%task_name
            exit()
        