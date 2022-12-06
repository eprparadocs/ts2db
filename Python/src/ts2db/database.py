from .user import USER

class DB:
    def __init__(self, table:dict, user:USER):
        self.name = table["name"]
        self.rows = table["table"]
        self.homepath = 
        
    def makedir(self)
def createdb(crreq:dict, user:dict):
    """
    Input to this function is the following:
    
    {
      "name": <table name>,
      "table": {
          <col1name>:<col1type>,
          <col2name>:<col2type>,
          ...
      }
    }
    
    We allow the following column types, though internally they are all kept as strings. When results
    are returned to the caller the strings will be returned as the proper type.
    
    What happens is that we create a directory named <table name>.ts2db that is stored in the per-user
    area and holds all the files that make up the database proper. The following files are created in
    that directory:
    
    1) def.tbl - the definition of the table, basically the stuff in the "table" JSON, some configuration
                  parameters. 
    2) record.tbl - that storage of the complete record
    3) i<N>.idx - the index file for the Nth key
    """
