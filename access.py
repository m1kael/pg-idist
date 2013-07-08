import sys
#import Decimal  #for an example below

# postgresql database access
# http://initd.org/psycopg/
import psycopg2
import csv

##### GLOBALS
#database access
DB_USER = "mike"
DB_PASS = "Alph@b3t"
DB_NAME = "idist"
DB_HOST = ""

_DBG = True


def insert(con, tbl, cols, vals):
    sql = "INSERT INTO " + tbl + " ("
    for c in cols:
        sql = sql + c + ","
    sql = sql[:-1] + ") VALUES ("
    for v in vals:
        sql = sql + "%s" + ","
    sql = sql[:-1] + ");"
    
    if _DBG:
        print sql
        print "vals:", vals
    
    try:
        cur = con.cursor()
        if _DBG:
            print cur.mogrify(sql, vals)
        cur.execute(sql, vals)
        con.commit()
    except Exception, e:
        print e
        con.rollback()
        con.close()
        exit()


def connect(dbhost=DB_HOST, dbname=DB_NAME, username=DB_USER, userpass=DB_PASS):
    """Connects to a database"""
    try:
        con = psycopg2.connect(database=dbname,user=username,password=userpass,host=dbhost)
    except Exception, e:
        con = None
        print e
        exit()
    return con


def _samples():
    """Run sample python calls through psycopg2 api"""
    
    con = connect()
    cur = con.cursor()
    print cur.mogrify("SELECT %s, %s, %s;", (None, True, False))
    #print cur.mogrify("SELECT %s, %s, %s, %s;", (10, 10L, 10.0, Decimal("10.00")))
    print cur.mogrify("SELECT %s;", ([10, 20, 30], ))
    
    
    sql = "create table if not exists thing ( code integer, name varchar(80) );" 
    print sql
    cur.execute(sql)
    
    sql = "truncate thing;"
    print sql
    cur.execute(sql)
    
    tbl = "thing"
    cols = ["code", "name"]
    multivals = [[123, "alpha"], [456, "beta"], [789, "gamma"]]
    for vals in multivals:
        insert(con, tbl, cols, vals)
    #endfor
    
    #basic select with iterable cursor
    sql = "select * from thing;"
    print sql
    cur.execute(sql)
    for record in cur:
        print record
    
    #or fetch commands
    sql = "select * from thing;"
    print sql
    cur.execute(sql)
    row = cur.fetchone() #fetches next row
    while row != None:
        print row
        row = cur.fetchone()
    
    sql = "select * from thing;"
    print sql
    cur.execute(sql)
    rows = cur.fetchmany(2) #fetches next n rows
    print rows
    rows = cur.fetchmany(2) #notice might be less than this left
    print rows
    rows = cur.fetchmany(2) #empty list if none left
    print rows
    
    sql = "select * from thing;"
    print sql
    cur.execute(sql)
    rows = cur.fetchall() #fetches the rest   
    for row in rows:
        print row
    
    sql = "select * from thing where code = %s;"
    vals = [123,]
    print cur.mogrify(sql, vals)
    cur.execute(sql, vals)
    results = cur.fetchall()
    for row in results:
        print row
    
    print cur.query  #last query sent to the cursor
    
    print "copy to/from table"
    fout = open('db.sql.copy', 'w')
    cur.copy_to(sys.stdout, 'thing', sep='|')
    cur.copy_to(fout, 'thing', sep=',')
    fout.flush()
    fout.close()
    
    sql = "truncate thing;"
    cur.execute(sql)
    fin = open('db.sql.copy', 'r')
    cur.copy_from(fin, 'thing', sep=',')
    fin.close()
    
    sql = "select * from thing;"
    print sql
    cur.execute(sql)
    rows = cur.fetchall() #fetches the rest   
    for row in rows:
        print row
    
    

def load_data(filename, tablename, filesep=',', clear=False):
    
    con = connect()
    cur = con.cursor()
    
    if clear:
        sql = 'truncate ' + tablename + ';'
        cur.execute(sql)
    
    fin = open(filename, 'r')
    cur.copy_from(fin, tablename, sep=filesep)
    fin.close()

#enddef


##### MAIN - Testing purposes

if __name__ == "__main__":
    
    _samples()
    
    load_data('test_2_10.txt', 'data')


#endmain
