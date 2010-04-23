
import MySQLdb
import MySQLdb.cursors
import pickle
import pprint


class PoqCtx(object):

    OP = {"eq": "=", "ne": "!=", "gt":">", "lt":"<"}


    __shared_con = None
    args = {"db_name": "poq_demo"}
    autocommit = True
    
    def __init__(self):
        if self.__shared_con is None:
            self.__shared_con = MySQLdb.connect(host = "localhost",
                                                user = "root",
                                                passwd = "S3n89mk!",
                                                db = "poq_demo",
                                                cursorclass=MySQLdb.cursors.DictCursor)

        self.con = self.__shared_con

    def sync(self):
        pg = PoqGenerator();
        pg.generate();


    def execute(self, sql, args = {}):
        print("------------------------------------------------------------")
        print(sql)
        print(args)
        a = args.copy()
        a.update(self.args)
        cursor = self.con.cursor()
        result = {}
        try:
            # Execute the SQL command
            v = cursor.execute(sql, a)
            # Fetch all the rows in a list of lists.
            result = cursor.fetchall()

            print("Affected rows: %d" % self.con.affected_rows())
            print("Info: %s" % self.con.info())            

            if self.autocommit:
                self.con.commit()
            cursor.close()

        except Exception as ex:
            if self.autocommit:
                self.con.rollback()
            print("PoqCtx: SQL Exception ----------------------------------------")
            print(ex)

        
        return result

    def commit(self):
        self.con.commit()

    def rollback(self):
        self.con.rollback()

    def start(self):
        self.autocommit = False


    def close(self):
        self.con.close()



class PoqGenerator(object):

    ctx = None
    drv = None

    def __init__(self):
        print("PoqGenerator: __init__")
        self.ctx = PoqCtx()
        self.drv = PoqMYSQL()

    def generate(self):
        print("PoqGenerator: generate")

        schema = {}
        tables = self.drv.get_tables()
        for row in tables:
            t = {}
            t["table"] = row["TABLE_NAME"]
            t["var"] = self.drv.get_columns(t["table"])
  
            t["foreign"] = self.drv.get_foreign(t["table"]);

            n = self.drv.get_primary(t["table"])
            if n :
                t["pri"] = n

            n = self.drv.get_auto(t["table"])
            if n :
                t["auto"] = n
            n = self.drv.get_uuid(t["table"])
            if n :
                t["uuid"] = n

            schema[t["table"]] = t
            
        fs = {}
        for tn, table in schema.items() :
            tn = table["table"]
            for tc, f in table["foreign"].items() :
                
                rc = f["ref_class"];
                rn = f["ref_name"];
                tn = f["this_name"];
                tc = f["this_class"];
                print(rc, tc)
                if not tc in  schema[rc]["foreign"] :                
                    schema[rc]["foreign"][tc] = {"this_class" : rc,
                                                 "this_name" : rn,
                                                 "ref_class" : tc,
                                                 "ref_name" : tn,
                                                 "rev" : "1" }
                    

        self.write(schema)

    def write(self, schema):
        print("PoqGenerator: write")
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(schema)
        impl = """
import pickle
import poq        

class Poq(object):
    def __getattr__(self, name):
        cn = \"poq_%s_impl\" % name
        return eval(cn+\"()\")
    

"""
        for cn, meta in schema.items():
            m = pickle.dumps(meta)
            impl += """
    def %s(self, *args):
        return %s%s_impl(args)
""" % (cn, "poq_", cn)

        for cn, meta in schema.items():
            m = pickle.dumps(meta)
            impl += """
class %s%s_impl(poq.PoqClass):
    _meta_base = \"\"\"%s\"\"\"
    def __init__(self, *args):
        poq.PoqClass.__init__(self, *args)

""" % ("poq_", cn, m)
            
        f = open('api.py', 'w')
        f.write(impl)


class PoqMYSQL(object):

    ctx = None

    def __init__(self):
        self.ctx = PoqCtx()

    def get_tables(self):
        result = self.ctx.execute("select TABLE_NAME, CREATE_TIME from information_schema.tables where TABLE_SCHEMA=%(db_name)s;");
        return result
    
    def get_columns(self, table):
        self.ctx.args["table"] = table
        result = self.ctx.execute("select COLUMN_NAME as name, COLUMN_TYPE as type from information_schema.COLUMNS WHERE TABLE_SCHEMA=%(db_name)s AND TABLE_NAME=%(table)s ;");
        return result

    def get_primary(self, table):
        self.ctx.args["table"] = table
        result = self.ctx.execute("select COLUMN_NAME as name, EXTRA, COLUMN_TYPE as type from information_schema.COLUMNS WHERE TABLE_SCHEMA=%(db_name)s AND TABLE_NAME=%(table)s AND COLUMN_KEY='PRI';");
        r = {}
        for f in result :
            r[f["name"]] = f
        return r

    def get_foreign(self, table):
        self.ctx.args["table"] = table
        result = self.ctx.execute("select TABLE_NAME as this_class, COLUMN_NAME as this_name, REFERENCED_TABLE_NAME as ref_class, REFERENCED_COLUMN_NAME as ref_name from information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=%(db_name)s AND TABLE_NAME=%(table)s AND REFERENCED_COLUMN_NAME IS NOT NULL;");
        r = {}
        for f in result :
            r[f["ref_class"]] = f
        return r

    def get_auto(self, table):
        self.ctx.args["table"] = table
        result = self.ctx.execute("select COLUMN_NAME as name, EXTRA, COLUMN_TYPE as type from information_schema.COLUMNS WHERE TABLE_SCHEMA=%(db_name)s AND TABLE_NAME=%(table)s AND COLUMN_KEY='PRI' AND EXTRA='auto_increment';");
        r = {}
        for f in result :
            r[f["name"]] = f

        return r

    def get_uuid(self, table):
        self.ctx.args["table"] = table
        result = self.ctx.execute("select COLUMN_NAME as name, EXTRA, COLUMN_TYPE as type from information_schema.COLUMNS WHERE TABLE_SCHEMA=%(db_name)s AND TABLE_NAME=%(table)s AND COLUMN_KEY='PRI' AND COLUMN_TYPE='char(36)'");
        r = {}
        for f in result :
            r[f["name"]] = f
        return r

    def checksum(self):
        result = self.ctx.execute("select MD5(SUM(CREATE_TIME)) as CS from information_schema.tables where TABLE_SCHEMA=%(db_name)s;");
        r = 0
        for f in result :
            r = f["CS"]
        return r



class PoqProp(object):
    parent = None
    name = None

    def __init__(self, class_obj, name):
        self.parent = class_obj
        self.name = name

    def __str__(self):
        str = "PoqProp: %s.%s" % ( self.parent._entity,  self.name)
        if self.parent.is_instnace() and hasattr(self.parent, self.name):
            str += " : %s" % getattr(self.parent, self.name)
        str += ""
        return str

    def __eq__(self, other):
        PoqCond(self.parent, "eq", self, other) 

    def __ne__(self, other):
        PoqCond(self.parent, "ne", self, other) 

    def __lt__(self, other):
        PoqCond(self.parent, "lt", self, other)
        
    def __gt__(self, other):
        PoqCond(self.parent, "gt", self, other) 
        

class PoqCond(object):
    refs = {}
    args = {}
    parent = None
    op = None
    lhs = None
    rhs = None
    counter = 0
    sql = ""
    
    def __init__(self, parent, op, lhs, rhs):
        self.parent = parent
        self.op = op
        self.lhs = lhs
        self.rhs = rhs

        if isinstance(self.lhs, PoqProp):
            ls = "%s.%s" % (self.lhs.parent._alias, self.lhs.name)
            self.refs[self.lhs.parent._alias] = self.lhs.parent
        else:
            ls = self.parent.bind(self.lhs)
            
        if isinstance(self.rhs, PoqProp):
            rs = "%s.%s" % (self.rhs.parent._alias, self.rhs.name)
            self.refs[self.rhs.parent._alias] = self.rhs.parent
        else:
            rs = self.parent.bind(self.rhs)

        self.sql = "%s %s %s" % ( ls, PoqCtx.OP[self.op], rs)

        self.parent.add_cond(self)

    def __str__(self):
        return self.sql

        

class PoqClass(object):

    _meta = {}
    _meta_base = {}
    _entity = None
    _alias = None
    _refs = {}
    _args = {}
    _counter = 0
    __sql = None
    
    __fields = {}
    _cond = []
    __result = None
    __pos = -1
    __ctx = None
    __instance = False
    
    def __iter__(self):
        return self

    def __init__(self, *args): 
        self.__ctx = PoqCtx()
        self._meta = pickle.loads(self._meta_base)
        self._entity = self._meta["table"]
        self._alias = "_"  + self._entity.__str__()
        self.__sync = 0;
        self._refs[self._alias] = self
        
        for obj in self._meta["var"]:
            self.__fields[obj["name"]] = PoqProp(self, obj["name"])
            
            load = None
            for arg in args:
                idx = 0
                for n, obj in self._meta["pri"].items():
                    if idx in args:
                        self.__fields[n].value = args[idx]
                        load = true
                        idx += 1

    def next(self):
        self.__pos += 1
        if self.__pos < len(self.__result):
            for n, obj in self.__result[self.__pos].items():
                self.__fields[n] = obj;
            return True
        else:
            print("pos %d not in result" % self.__pos)
            False

    def bind(self, value):
        self._counter += 1
        idx = "val_%s_%s" % (self._entity, self._counter)
        marker = "%%(%s)s" % (idx)
        self._args[idx] = "%s" % value
        return marker
    
    def is_instnace(self):
        return self.__instance
    
    def select(self):
        r = self.refs()
        c = self.cond()
        c = c and "where " + c or ""
        self.__sql = "SELECT %s.* FROM %s %s;" % (self._alias, r, c)
        self.query()

    def update(self):
        r = self.refs()
        c = self.cond()
        a = self.assign()
        c =  (c and ("where " + c) or "")
        self.__sql = "UPDATE %s SET %s %s;" % (r, a, c)
        self.query()

    def insert(self):
        a = self.assign(False)
        self.__sql = "INSERT INTO %s SET %s;" % (self._entity, a)
        self.query()

    def refs(self):
        str = ""
        sep = ""
        for n, r in self._refs.items():
            str += "%s%s AS %s" % (sep, r._entity, r._alias )
            sep = ", "
        return str

    def cond(self):
        str = ""
        sep = ""
        for f in self._cond:
            str += "%s%s" % (sep, f)
            sep = " and "
        return str

    def assign(self, use_alias = True):
        str = ""
        sep = ""
        for n, f in self.__fields.items():
            if not isinstance(f, PoqProp):
                m = self.bind(f)
                if use_alias:
                    str += " %s %s.%s = %s" % (sep, self._alias, n, m)
                else:
                    str += " %s %s = %s" % (sep, n, m)
                sep = ","

        return str


    def add_cond(self, cond):
        self._refs.update(cond.refs)
        self._args.update(cond.args)        
        self._cond.append(cond)

    def query(self):
        self.__result = self.__ctx.execute(self.__sql, self._args)
        self.__count = len(self.__result)
        self.__instance = True


    def __getattr__(self, name):
        if (name[0] == '_'):
            return self.__dict__[name]
        if name not in self.__fields:
            raise AttributeError, name
        return self.__fields[name]
            

    def __setattr__(self, name, value):
        if (name[0] == '_'):
            self.__dict__[name] = value
        else:
            self.__fields[name] = value
            
    def __str__(self) :
        str = "PoqClass %s \n" % (self._entity)
        for n, f in self.__fields.items():
            str += "  %s \n" % f
        return str
