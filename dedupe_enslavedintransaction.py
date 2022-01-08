import mysql.connector
import json

d=open("dbcheckconf.json","r")
t=d.read()
d.close()
conf=json.loads(t)

cnx = mysql.connector.connect(**conf)
cursor = cnx.cursor()

cursor.execute("select id,enslaved_id,transaction_id from past_enslavedinrelation;")

results=cursor.fetchall()

duplicates=[]

relationdict={}
for result in results:
	id,enslaved_id,transaction_id=result
	if enslaved_id not in relationdict:
		relationdict[enslaved_id]=[transaction_id]
	else:
		if transaction_id in relationdict[enslaved_id]:
			duplicates.append(result)
			print(result)
		else:
			relationdict[enslaved_id].append(transaction_id)

for duplicate in duplicates:
	cursor.execute("delete from past_enslavedinrelation where id=%d" %duplicate[0])
	cnx.commit()