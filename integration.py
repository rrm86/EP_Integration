import MySQLdb
import csv
import time
import hashlib
import os
from time import gmtime, strftime

def connection(database):
    db = MySQLdb.connect("localhost","","",database)
    return db


def addCourse(dbEP):
    db = connection(dbEP)
    cursor = db.cursor()
    with open('teste.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                cursor.execute('INSERT INTO course values("%s","%s","%s","%s")' % \
                (row[0], row[0],row[1],row[2]))
                db.commit()
            except Exception as e:
                print e
                db.rollback()
    db.close()

def addEnrolment(dbEP):
    db = connection(dbEP)
    cursor = db.cursor()
    with open("ExportaCursoUsuario.csv","rb") as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                cursor.execute('INSERT INTO enrolment(id_course,username,role) values("%s","%s","%s")' % \
                    (row[0], row[1], row[2]))
                db.commit()
            except Exception as e:
                print e
                db.rollback()
    db.close()

def renameFile():
    if os.path.exists("ExportaUsuario.txt"):
        os.rename('ExportaUsuario.txt','ExportaUsuario.csv')
    if os.path.exists("ExportaCursoUsuario.txt"):
        os.rename('ExportaCursoUsuario.txt','ExportaCursoUsuario.csv')
    if os.path.exists("ExportaCurso.txt"):
        os.rename('ExportaCurso.txt','ExportaCurso.csv')

def addUser(dbMoodle):
    db = connection(dbMoodle)
    cursor = db.cursor()
    with open("ExportaUsuario.csv","rb") as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                cursor.execute('''INSERT INTO mdl_user (auth,confirmed,mnethostid,username,password,firstname,lastname,email)
               values(%s,%s,%s,%s,%s,%s,%s,%s)''',
           ("manual","1","1",row[0],hashlib.md5(row[1]).hexdigest(),row[2].decode('utf-8'),row[3].decode('utf-8'),row[4]))
                db.commit()
            except Exception as e:
                print e
                db.rollback()
    db.close()



param={"dbExtra":"escolaparque","moodle":"moodletesteEP"}
renameFile()
addCourse(param["dbExtra"])
addEnrolment(param["dbExtra"])
addUser(param["moodle"])
print "ok"
