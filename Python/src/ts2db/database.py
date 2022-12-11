"""
class DB - all things related to creating, reading, writing, deleting a database.

The following files are part of a "database"

    Record Table Info - all things about the specifics of the database including
                        file name to the index files, the the record table. The 
                        record table info is the only file with a fixed, knoen 
                        name. All the others are reference via this table. This
                        file is known as info.json.bz2
            XXXXX.idx - Index file associate with column XXXXX (see the table
                        definitions for what columns have indices,)
         record talbe - the file that holds the records of the database. Actual
                        name is records.bz2
"""
from os.path import join, exists
from os import mkdir, getcwd, chdir, rmdir, walk
import bz2
import json
import shutil

from user import User
import exceptions

class DB:
    def __init__(self, table:dict, user:User):
        self.name = table["name"]
        self.cols = table["table"]
        self._user = user
        self.validate(self.cols)        
        self.CreateDB()
        

    def makedir(self):
        """
        Make the root directory of this database. This is the "database".
        Every file in the database directory is compressed.
        """
        self.dbdir = join(self._user.home, self.name)
        mkdir(self.dbdir)
    
        
    def recordTableInfo(self):
        """
        This is the schema of the database. It includes all sorts of information
        besides just the compressed JSON description of the table. 
        """
        
        # Build the JSON structure we will write out
        j = {'table': self.cols}
        j['name'] = self.name
        j['idxsize'] = self._user.idxsize
        with bz2.open("info.json.bz2", "wb", compresslevel=9) as fp:
            fp.write(json.dumps(j).encode('utf-8'))
            
    
    def buildRecordTable(self):
        """
        This is a file that contains the unique complete records stored in the table.
        The format is one record following another. Each record looks like thisL
        
        <size of record>
        <record, in JSON format, compressed with bz2>
        """
        with bz2.open("records.dat", "wb", compresslevel=9) as fp:
            # There's nothing yet recorded in the file.
            fp.write(bytes(0))
    
    
    def buildIndices(self):
        """
        There is one index file for each index that is defined. Each of the index files are composed 
        of two tiers. The first tier maps the actual record field to an index (32-bit number). The 
        second tier of the file takes the numeric index and maps it to one or more record pointers.  
        The record pointers are 64 bit offsets into a records file, and point to a JSON representation 
        of the record. This second tier is real a B-Tree with the key being the index value from 
        the first tier.
        """        
        for i in self.cols.keys():
            # Preallocate the index files. We do this to make certain that we can map each index
            # file into the memory map.
            with open(i+'.idx','wb') as fp:
                fp.seek(self._user.idxsize+1,0)
                fp.write(bytes(0))
        

    def CreateDB(self):
        """
        Create the initial database directory and files.
        """
        cwd = getcwd()
        self.makedir()
        chdir(self.dbdir)
        self.recordTableInfo()
        self.buildRecordTable()
        self.buildIndices()
        chdir(cwd)
        
        
    def validate(self, cols):
        
        # Make certain the table doesn't exist.
        cname = join(self._user.home, self.name)
        if exists(cname) :
            raise exceptions.TableError("Table '%s' is already known in database '%s'." % (self.name, self._user.home))
        
        # We have a good table name, so now make sure the
        # column names are good and the # data types are 
        # supported.
        for i in cols:
            if cols[i] not in ['str', 'int', 'float']:
                raise exceptions.TypeError("Column '%s' has invalid type '%s'" % (i, cols[i]))
            if not i.isalnum():
                raise exceptions.NameError("Column '%s' has illegal characters in the column name." % (i))
                
        
def createdb(crreq:dict, user:User):
    """
    Input to this function is the following:
    
    ctreq is:
    
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
    """
    DB(crreq, user)
    

def deletedb(dbname:str, user:User):
    
    # See if the database exists.
    path = join(user.home, dbname)
    if exists(path):
        # REemove the "database!"
        shutil.rmtree(path)


def listTables(dbname:str|list, full:bool, user:User):
    
    def enumerateDir(path):
        dirlist = []
        for r,d,f in walk(path):
            for dir in d:
                dirlist.append(join(r,dir))
        return dirlist
                
    
    # Buidl the list of databases, just in case we haven't been given one.
    if dbname and isinstance(dbname, str) :
        dbname = [dbname]
        
    dblist = dbname if dbname else enumerateDir(user.home)
    answer = {} if full else []
    for i in dblist:
        path = join(user.home, i, 'info.json.bz2')
        if exists(path):
            if not full:
                answer.append(i)
            else:
                with bz2.open(path, 'rb', compresslevel=9) as fp:
                    info = json.loads(fp.read())
                    answer[i] = info['table']
    return(answer)

    
if __name__ == '__main__':
    
    # Place all database tables in the subdirectory test. The
    # index files must be 10,000 bytes long.
    user = User()
    user.home = './test'
    user.idxsize = 10000
    
    # Make test database area..required for testing...
    if exists('test'):
        shutil.rmtree('test')
    mkdir('test')

    # Various tests...
    tests = {}
    tests['name error'] = {
                             'name':'TESTIT',
                             'table': {
                                 'col1': 'int',
                                 'col2': 'str',
                                 'col3': 'float',
                                 'illegal-name': 'int'
                                 }
                          }
    tests['type error'] = {
                             'name':'TESTIT',
                             'table': {
                                 'col1': 'int',
                                 'col2': 'str',
                                 'col3': 'floating'
                                 }
                          }
    tests['no error'] = {
                             'name':'TESTIT',
                             'table': {
                                 'col1': 'int',
                                 'col2': 'str',
                                 'col3': 'float'
                                 }
                          }
    tests['table error'] = {
                             'name':'TESTIT',
                             'table': {
                                 'col1': 'int',
                                 'col2': 'str',
                                 'col3': 'int'
                                 }
                          }    
    for i in tests:
        try:
            which_err = i
            createdb(tests[i], user)
        except exceptions.TableError as e:
            if which_err == 'table error':
                print('Duplicate table test ok')
            else:
                print(e.args)
        except exceptions.NameError as e:
            if which_err == 'name error':
                print('Column name error test ok')
            else:
                print(e.args)
        except exceptions.TypeError as e:
            if which_err == 'type error':
                print('Column type error test ok')
            else:
                print(e.args)
                
                
    # Get the recorded information
    table = listTables('TESTIT', user)
    if not (table['TESTIT'] ==  tests['no error']['table']):
        print('Table differences!')
    else:
        print('Tables match!')

    # Clean up test directory
    deletedb("TESTIT", user)
    rmdir("test")
    
    print("All tests complete")
