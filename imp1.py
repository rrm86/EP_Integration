import MySQLdb
import csv
import time
from time import gmtime, strftime
"""db1 = MySQLdb.connect("localhost","user","pass","escolaparque")"""
db2 = MySQLdb.connect("localhost","user","pass","moodletesteEP")
name = [strftime("%Y")+"BB",strftime("%Y")+"GG"]
parent = ["barra","gavea"]
timestamp = int(time.time())
depth = 2
getLastId = db2.cursor()
getParent = db2.cursor()
for index in range(len(parent)):
    print parent[index]
    getLastId.execute('''SELECT MAX(id) FROM mdl_course_categories''')
    catId = int(getLastId.fetchone()[0])+1
    catId = str(catId)
    getParent.execute('''SELECT id FROM mdl_course_categories Where idnumber = %s''',(parent[index]))
    parentId = str(getParent.fetchone()[0])
    path = "/"+parentId+"/"+catId
    print path
    cursor2 = db2.cursor()
    cursor2.execute('''INSERT INTO mdl_course_categories (name,parent,timemodified,depth,path)
               values(%s,%s,%s,%s,%s)''',
           (name[index], parentId, timestamp, depth, path))
db2.commit()
db2.close()
"""cursor1 = db1.cursor()


with open('teste.csv', 'rb') as f:
	reader = csv.reader(f)
	for row in reader:
		try:
			cursor1.execute('INSERT INTO course values("%s","%s","%s","%s")' % \
			(row[0], row[0],row[1],row[2]))
			db1.commit()
		except:
			db1.rollback()

db1.close()"""