import MySQLdb
import csv
import time
import hashlib
import os
from time import gmtime, strftime

def connection(database):
    db = MySQLdb.connect("localhost","","",database)
    return db


def addCourse(dbEP,dbMoodle):
    db = connection(dbEP)
    cursor = db.cursor()
    with open('ExportaCurso.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                handleCategory(dbMoodle,row[2])
                category = idParent(dbMoodle,row[2])
                cursor.execute('INSERT INTO course values("%s","%s","%s","%s")' % \
                (row[0], row[0],row[1],category))
                db.commit()
            except Exception as e:
                print e
                db.rollback()
    db.close()

def handleCategory(dbMoodle,category):
    db = connection(dbMoodle)
    cursor = db.cursor()
    getLastId = db.cursor()
    getPath = db.cursor()
    newCat = db.cursor()
    cursor.execute('''SELECT id FROM mdl_course_categories Where name = %s''',(category))
    categoryId= cursor.fetchone()
    #check if category exist
    if  categoryId is not None:
        db.commit()
        db.close()
    else:
        getLastId.execute('''SELECT MAX(id) FROM mdl_course_categories''')
        newId = int(getLastId.fetchone()[0])+1
        newId = str(newId)
        getPath.execute('''SELECT path FROM mdl_course_categories Where idnumber = %s''',(category[4:]))
        path = str(getPath.fetchone()[0])
        parent = int(path[1:])
        path = path+"/"+newId
        newCat.execute('''INSERT INTO mdl_course_categories (name,idnumber,parent,timemodified,depth,path)values(%s,%s,%s,%s,%s,%s)''',(category,category,parent,int(time.time()),2,path))
        db.commit()
        db.close()


def idParent(dbMoodle,category):
    db = connection(dbMoodle)
    cursor = db.cursor()
    cursor.execute('''SELECT id FROM mdl_course_categories Where name = %s''',(category))
    parentId = cursor.fetchone()[0]
    return parentId


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
addCourse(param["dbExtra"],param["moodle"])
addEnrolment(param["dbExtra"])
addUser(param["moodle"])

print "ok"
