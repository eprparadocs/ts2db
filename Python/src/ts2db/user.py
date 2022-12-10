"""
Per-User Configuration information support.

This information is maintained in a system-wude area and holds per-user 
information. 
"""

class User:
    
    _home = None
    _idxsize = 0
    
    def get_home(self):
        return self._home;
    
    def set_home(self, home):
        self._home = home
    home = property(get_home, set_home,  doc="Location of databases")
        

    def get_idxsize(self):
        return self._idxsize
    
    def set_idxsize(self, idxsize):
        self._idxsize = idxsize
    idxsize = property(get_idxsize, set_idxsize, doc="Maximum size of index files, in bytes")
    
    
def GetPerUser(user:str):
    return None
