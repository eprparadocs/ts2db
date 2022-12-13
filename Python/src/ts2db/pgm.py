from multiprocessing import Queue, Process

def ProcessPgm(inque:Queue, outque:Queue):
    """
    This function, which is run in multiple processes, reads from
    inque, does the processing and returns the results in outque.
    """
    pass


def ProcessCOmpletion(inque:Queue, outque:Queue):
    """
    """
    pass


def Startup(nCount):
    """
    This function is called during initialization of the system. It start
    'nCount' processes each running the ProcessPgm function. These processes
    evaluates various "programming step".
    """
    # Create the necessary queues.
    inQue = Queue()
    outQue = Queue()
    
    # Startup the processing processes.
    for i in range(nCount):
        p=Process(target=ProcessPgm, name='ProcessPgm %s' % (str(i)), args=(inQue, outQue))
        p.start()
        
    # Return the queues for additional handling.
    return [inQue, outQue]


def Dispatch(pgm:list, inque:Queue):
    
    # Normally we should use threading for this work, but Python because of the lock
    # isn't really a good solution. INstead in Python we use processes.
    for i in pgm:
        inque.put(i)


def Complete(pgm:list, intermediate, inque:Queue):
    """
    This is the final step of a string of programming steps. It takes the intermediate
    results and, per the pgm, processes them to produce the output.
    """
    inque.put([pgm, intermediate])
