from config import *
import pymysql

class Model():
    Name = ""
    Text = ""
    Schema = ""
    #init
    def __init__(self, name):
        self.Name = name
        self.Dbcon = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASS,MYSQL_DBNAME)

    # set Text value
    def set(self, text):
        self.Text = self.reformat(text)

    # Run mysql query
    def runQuery(self, query):
        dbcur = self.Dbcon.cursor()
        try:
            dbcur.execute(query)
            self.Dbcon.commit()
        except:
            self.Dbcon.rollback()
            print(query)
            print("SQL statement Error")
        dbcur.close()

    # reformat the fields' name or values
    def reformat(self, text):
        return text.replace('"','').replace('\n','').replace("'","''")

    # create a table from header of csv
    def creatTable(self):
        self.Schema = self.Text
        # check if table is existed or not
        if not self.checkTableExists(self.Name):
            names = self.Text.split(",")
            query = """CREATE TABLE `%s` ( `%s` int(10) NOT NULL AUTO_INCREMENT,""" %(self.Name, names[0])
            for i in range(1, len(names)):
                query += "`%s` text,"% (names[i])
            query += """PRIMARY KEY (`%s`) ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""" % (names[0])
            # create table with name
            self.runQuery(query)


    # func to check table existance
    def checkTableExists(self, tablename):
        dbcur = self.Dbcon.cursor()
        dbcur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(tablename.replace('\'', '\'\'')))
        if dbcur.fetchone()[0] == 1:
            dbcur.close()
            return True
        dbcur.close()
        return False

    # save model to db
    def save(self):
        names  = self.Schema.split(",")
        values = self.Text.split(",")
        # check duplicate id
        dbcur = self.Dbcon.cursor()
        dbcur.execute(
            """SELECT COUNT(*) FROM `%s` WHERE `TM_NUMBER` = %s LIMIT 1""" %(self.Name, values[0])
        )
        if not dbcur.fetchone()[0]:
            query = """INSERT into `%s` ( %s ) VALUES ( %s, """ % (self.Name, self.Schema, values[0])
            for i in range(1, len(names)):
                query += """'%s',"""%(values[i])
            query = query[0:len(query)-1] + ')'
        else:
            query = """UPDATE `%s` SET """ %(self.Name)
            for i in range(1, len(names)):
                query += """`%s`= '%s',""" % (names[i],values[i])
            query = query[0:len(query)-1] + """ where `TM_NUMBER`=%s""" % (values[0])
        dbcur.close()
        # run query
        self.runQuery(query)
