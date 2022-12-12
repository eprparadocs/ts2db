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
from os.path import join, exists, getsize
from os import mkdir, rmdir, walk, listdir
import bz2
import json
import shutil

from user import User
import exceptions

class DB:
    """
    Handle Database level things such as create, delete, query
    
    Methods:
    
    DB(dbname, User, db-options) - create dababase 
                  delete(iffull) - delete the database even if not empty
                list(name, full) - list one or more tables in the database
    
                
    Attributes:

    opts - return database wide options
    name - return the name of the database
    path - return the filepath to the database
    
    """
    
    def __init__(self, dbname:str, user:User, dbopts:dict|None = None):
        
        # If the database exists, we don't need to do anything.
        self._name = dbname
        self._dbpath = join(user.home, dbname)
        ipath = join(self._dbpath, "info.json.bz2")
        if not exists(self._dbpath):
            # Make the root directory of the database.
            mkdir(self._dbpath)
            
            # Create the composite dboptions.
            if not dbopts:
                dbopts = {}
            self._opts = dbopts | { 'idxsize': user.idxsize }
            
            # Fill in the various defaults for the database.
            with bz2.open(ipath, "wb", compresslevel=9) as fp:
                fp.write(json.dumps(self._opts).encode('utf-8'))
        else:
            # The database already exists, so get the info options.
            with bz2.open(ipath, 'rb', compresslevel=9) as fp:
                self._opts = json.loads(fp.read())
                
    def get_opts(self):
        return self._opts
    opts = property(get_opts)
    
    def get_name(self):
        return self._name
    name = property(get_name)
    
    def get_path(self):
        return self._dbpath
    path = property(get_path)
    
    def list(self, name:str|None, full:bool):
        """
        Return information about a table or some tables.
        
        Input:
             name - None, a single table name or a list of table names
           full   - False, return only the indication if a table exists or not, 
                    True, return the column information of a table
        Returns:
            list of present table names, if full == False
            dict of present table names plus their column definitions, if full == True
        """
        
        def enumerateDir():
            dirlist = []
            for r,d,f in walk(self._dbpath):
                for dir in d:
                    dirlist.append(join(r,dir))
            return dirlist
                    
        
        # Buidl the list of databases, just in case we haven't been given one.
        if name and isinstance(name, str) :
            name = [name]
            
        nlist = name if name else enumerateDir()
        answer = {} if full else []
        for i in nlist:
            path = join(self._dbpath, i, 'info.json.bz2')
            if exists(path):
                if not full:
                    answer.append(i)
                else:
                    with bz2.open(path, 'rb', compresslevel=9) as fp:
                        info = json.loads(fp.read())
                        answer[i] = info['table']
        return(answer)
    
    
    def delete(self, iffull:bool = False):
        """
        Delete the database, possibly.
        
        Input:
            iffull - True, delete the database if there are tables in it
                     False, don't delete the database if there are tables present.
                     
        Return:
             False - Nothing deleted
              True - Database deleted  
        """
        citems = listdir(self._dbpath)
        del citems['info.json.bz2']
        rc = False
        if (len(citems) and iffull) or not len(citems):
            shutil.rmtree(self._dbpath)
            rc = True
        return(rc)
            
            
class TABLEDEF:
    """
    Class used to define a database table.
    
    Method:
              TableDef(name) - create an empty database table, and give the table 'name'
    AddCol(colname, coltype) - add a column named 'colname' and type 'coltype' to the table
                     reset() - reset table name and column definition
           
           
    Attributes:
                      table - get the table columns defined
                       name - set and get table name
    """
    def _init__(self, name:str):
        self._table = {}
        self._name = name
        
    def AddCol(self, name, coltype):
        
        # Validate name and type
        if not name.isalnum():
            raise exceptions.NameError("'%s' is an invalid name." % (name))
        if name in self._table:
            raise exceptions.NameError("'%s' is an alredy known name." % (name))
        if coltype not in ['str', 'int', 'float']:
            raise exceptions.TypeError("Type '%s' is invalid." % (coltype))
        self._table[name] = coltype
        
    def reset(self):
        self._table = self._name = None
    
    def get_table(self):
        return self._table
    table = property(get_table)
    
    def get_name(self):
        return self._name
    def set_name(self, name):
        self._name = name
    name = property(get_name)


class PERCONNECT:
    def __init__(self, name:str, tbl:TABLEDEF):
        self._name = name
        self._tabledef = tbl
        
    def get_name(self):
        return self._name
    def set_name(self, name):
        self._name = name
    name = property(get_name, set_name)
    
    def get_def(self):
        return self._tabledef
    def set_def(self, tbldef):
        self._tabledef = tbldef
    tabledef = property(get_def, set_def)
    
    
class Table:
    """
    Create/Delete/Open a table in the database
    
    Methods:
           Table(DB, opts) - Setup a database
    create(name, TABLEDEF) - create a new table in the database
             connect(name) - Connect a known table in the database
              delete(name) - Delete a table from the database
    
    Attributes:
                         opts - get per database options
                         cols - get data column definition
                         name - get name of the table
                           db - get DB for this table
                        dbdir - where is the database information stored
                       tbldir - where is the table information stored
    
    """
        
    def __init__(self, db:DB, dbopts:dict|None = None):
        self._db = db
        self._dbopts = db.opts | dbopts
        self._connectDict = {}
        
    def get_opts(self):
        return self._dbopts
    opts = property(get_opts)
    
    def get_db(self):
        return self._db
    db = property(get_db)
    
    def create(self, name:str, table:TABLEDEF):
        """
        Create a new table in the database.
        
        Input:
          name - name of table to create
         table - definition of table
                 
        Return:
        None
        
        Exceptions:
        TableError - excepti0on thrown if table exists.

        """
        
        # Create the per-connect structure
        tblcnt = PERCONNECT(name, table)
         
        # Make certian the table doesn't already exist
        path = join(self.db.path, name)
        if exists(path):
            raise exceptions.TableError("Table '%s' already exists!" % (table._name))
        
        # Make the database table
        self.make()
        
        # Tables exists. add ot tp tje cpmmectopm table
        self._connectDict = {name:tblcnt}
    
    def _isEmpty(self, name:str):
        path = join(self.db.path, name, "records.dat")
        return getsize(path) == 0
    
    def delete(self, name:str, iffull:bool):
        """
        Delete a table in the database.
        
        Input:
          name - name of table to delete
        iffull - True, delete even if table has content
                 False, delete only if table is empty
                 
        Return:
        None
        
        Exceptions:
        TableError - excepti0on thrown if table doesn't exist.
        """
        # Do we delete unconditionally?
        if iffull or self._isEmpty(name):
            # Yes, delete everything.
            path = join(self.db.path, name)
            if exists(path):
                # Remove the all the table information
                shutil.rmtree(path)
                
                # Remove the information from the connection table.
                del self._connectDict[name]
            else:
                raise exceptions.TableError("'%s' doesn't exist.")
    
    def connect(self, name:str):
        """
        Connect a known table to this database instance.
        
        Input:
          name - name of table to delete
                 
        Return:
        None
        
        Exceptions:
        TableError - excepti0on thrown if table doesn't exist.

        """
        # Makw certain we don't know about the table.
        if name not in self._connectDict:
            # Make certain we have a "real" table already defined
            path = join(self.db.path, name)
            if exists(path):
                # We have a real table!
                with bz2.open(join(path, "info.json.bz2"), "rb", compresslevel=9) as fp:
                    info = json.loads(fp.read())
                    perc = PERCONNECT(name, info)
                    self._connectDict = {name:perc}
            else:
                raise exceptions.TabkeError("'%s' doesn't exist." % (name))
    
    def make(self):
        """
        Make the root directory of this database. This is the where all 
        the table definitions are stored. Every file in the database 
        directory is compressed.
        """
        self._tbldir = join(self._db.path, self._name)
        mkdir(self._dbdir)
    
        """
        This is the schema of the database. It includes all sorts of information
        besides just the compressed JSON description of the table. 
        """
        # Build the JSON structure we will write out
        j = {'table': self._cols}
        j['name'] = self._name
        j['opts'] = self._dbopts
        with bz2.open(join(self._tbldir, "info.json.bz2"), "wb", compresslevel=9) as fp:
            fp.write(json.dumps(j).encode('utf-8'))

        """
        This is a file that contains the unique complete records stored in the table.
        The format is one record following another. Each record looks like thisL
        
        <size of record>
        <record, in JSON format, compressed with bz2>
        """
        with bz2.open(join(self._tbldir, "records.dat"), "wb", compresslevel=9) as fp:
            # There's nothing yet recorded in the file.
            fp.write(bytes(0))
    
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
            with open(join(self._tbldir, i+'.idx'),'wb') as fp:
                fp.seek(self._dbopts['idxsize']+1,0)
                fp.write(bytes(0))
                
    def delete(self):
        """
        Delete the table from the database.
        """
        if exists(self._tbldir):
            # Remove the database of table.
            shutil.rmtree(self._tbldir)

    
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
