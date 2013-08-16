'''
Created on 16/08/2012

@author: victor
'''
class SerialTask(object):
    def __init__(self, function, name, kwargs, description):
        self.function = function
        self.name = name
        self.kwargs = kwargs
    
    def run(self):
        return self.function(**(self.kwargs))
    
class SerialScheduler(object):
    """
    Not very efficient scheduler (it would be better to build a dependency tree).
    """
    
    def __init__(self):
        """
        dependencies {"task_name":["dep_task_A", "dep_task_B"]}
        """
        self.tasks = {}
        self.dependencies = {}
        self.not_completed = []
        self.finished = []
        self.results = []
    
    def run(self):
        """
        Tries to run all the tasks, checking for dependencies.
        """
        while len( self.not_completed) > 0:
            #Choose an available process
            task_name = self.choose_runnable_task()
            
            if task_name is None:
                print "It was impossible to pick a suitable task for running. Check dependencies."
                break;
            else:
                # Run a process
                self.run_task(task_name)
                self.remove_task(task_name)
                # List tasks
                #self.list_tasks()
        
        self.ended()
        return self.results
    
    def choose_runnable_task(self):
        for task_name in self.not_completed:
            if len(self.dependencies[task_name]) == 0: # This process has no dependencies
                return task_name;
        return None # All task have dependencies (circular dependencies for instance)
    
    def run_task(self, task_name):
        """
        """
        self.results.append(self.tasks[task_name].run())
    
    def remove_task(self, task_name):
        """
        
        """
        # Remove it from all dependencies. At the end all dependencies lists must be empty.
        for tn in self.dependencies:
            if task_name in self.dependencies[tn]:
                self.dependencies[tn].remove(task_name)
        
        # Remove it from the not_completed list
        self.not_completed.remove(task_name)
        
        # Add it to the list of completed tasks
        self.finished.append(task_name)
    
    def add_task(self, task_name, dependencies, target_function, function_kwargs, description):
        if not task_name in self.tasks:
            task = SerialTask( name = task_name, description = description, function = target_function, kwargs=function_kwargs)
            task.description = description
            self.tasks[task_name] = task
            self.not_completed.append(task_name)
            self.dependencies[task_name] = dependencies
        else:
            print "[Error SerialScheduler::add_task] Task %s already exists. Task name must be unique."%task_name
            exit()
            
    def ended(self):
        pass
        