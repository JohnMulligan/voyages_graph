import mysql.connector
import time
import json
import re
import redis
from redisgraph import Node, Edge, Graph, Path
from multiprocessing import Pool, TimeoutError

r = redis.Redis(host='localhost', port=6379)


def add_item(item_class,item_properties):
	global r
	redis_graph = Graph('enslavements', r)
	
	person = Node(
			label=item_class,
			properties=item_properties
		)
	redis_graph.add_node(person)
	#print(person)
	redis_graph.commit()
	del(person)
	del(redis_graph)

d=open("dbcheckconf.json","r")
t=d.read()
d.close()
conf=json.loads(t)

cnx = mysql.connector.connect(**conf)
cursor = cnx.cursor()

#first get enslaved

print("ADDING DATA")
#steps 1 & 2

q="select\
	e.enslaved_id,\
	e.documented_name,\
	e.age,\
	e.gender,\
	e.height,\
	e.voyage_id,\
	e.skin_color,\
	cf.`name` as captive_fate,\
	cs.`name` as captive_status,\
	vp.place as post_disembark_location\
	from (select enslaved_id,documented_name,age,gender,height,post_disembark_location_id,voyage_id,dataset,skin_color,captive_fate_id,captive_status_id from past_enslaved where enslaved_id>500000) as e\
	join (select * from past_captivefate) as cf on (cf.id=e.captive_fate_id)\
	join (select * from past_captivestatus) as cs on (cs.id=e.captive_status_id)\
	join (select * from voyage_place) as vp on (vp.id=e.post_disembark_location_id);"

cursor.execute(q)
results=cursor.fetchall()

print("enslaved",len(results))
st=time.time()

varnames=[
	'person_id',
	'name',
	'age',
	'age',
	'height',
	'voyage_id',
	'skin_color',
	'captive_fate',
	'captive_status',
	'last_known_location']

for result in results:
	properties={varnames[c]:result[c] for c in range(len(varnames)) if result[c] is not None}
	add_item('person',properties)

print(time.time()-st,"seconds")

#now get enslavers
q="select \
	ei.principal_alias,\
	ei.text_id,\
    ei.first_active_year,\
    ei.last_active_year,\
    ei.number_enslaved,\
    vp.place \
from \
(SELECT * FROM past_enslaveridentity where text_id like 'KIN%') as ei left join \
(SELECT * FROM voyage_place) as vp on (ei.principal_location_id=vp.id);"

cursor.execute(q)
results=cursor.fetchall()

print("enslavers",len(results))
st=time.time()

varnames=[
	'name',
	'person_id',
	'first_active_year',
	'last_active_year',
	'number_enslaved',
	'principal_location']

for result in results:
	properties={varnames[c]:result[c] for c in range(len(varnames)) if result[c] is not None}
	add_item('person',properties)

print(time.time()-st,"seconds")


def dateparse(mmddyyyy):
	if len(mmddyyyy)>=4:
		delimiter=re.search("[/|,]",mmddyyyy).group(0)
		m,d,y=[int(i) if i not in ["?",""] else None for i in mmddyyyy.split(delimiter)]
	else:
		m,d,y=[None,None,None]
	return m,d,y


q="select  \
per.`date` as mmddyyyy,  \
per.text_ref,  \
per.voyage_id,  \
ei.text_id,  \
ert.relationtype,  \
er.enslaverrole as enslaver_role,  \
edir.enslaved_id,  \
vp.place \
from \
(select * from past_enslavementrelation) as per join  \
(select * from past_enslaverinrelation) as eir on (eir.transaction_id=per.id) join  \
(select * from past_enslaveralias) as ea on (ea.id=eir.enslaver_alias_id) join  \
(select * from past_enslaveridentity) as ei on (ei.id=ea.identity_id) join  \
(select * from past_enslaverrole) as er on (eir.`role`=er.id) join  \
(select * from past_enslavementrelationtype) as ert on (per.relation_type=ert.id) join  \
(select * from past_enslavedinrelation) as edir on (edir.transaction_id=per.id) join  \
(select * from past_enslaved) as ed on (ed.enslaved_id=edir.enslaved_id) left join \
(select * from voyage_place) as vp on (vp.id=per.place_id);"

cursor.execute(q)
results=cursor.fetchall()

print("enslavements",len(results))
st=time.time()

for result in results:
	print(result)
	redis_graph = Graph('enslavements', r)
	transaction_date,text_ref,voyage_id,enslaver_id,enslavement_type,enslaver_role,enslaved_id,transaction_location=result
	
	#print(voyage_id)
	
	transaction_month,transaction_day,transaction_year=dateparse(transaction_date)
	
	#query="""MATCH (enslaver:person {person_id:$enslaver_id}),(enslaved_person:person {person_id:$enslaved_id}) CREATE (enslaver)-[:enslaved]->(enslaved_person)"""
	
	if voyage_id is not None:
	
		q="select \
		v.id, \
		vd.imp_arrival_at_port_of_dis,  \
		vd.vessel_left_port,  \
		vip_embarkation.place as embarkation_place,  \
		vip_disembarkation.place as disembarkation_place  \
		from  \
		(select * from voyage_voyage where voyage_id=%d) as v join \
		(select * from voyage_voyagedates) as vd on (vd.voyage_id=v.voyage_id) join  \
		(select * from voyage_voyageitinerary) as vi on (v.voyage_id=vi.voyage_id) left join  \
		(select * from voyage_place) as vip_disembarkation on (vip_disembarkation.id=vi.imp_principal_port_slave_dis_id) left join  \
		(select * from voyage_place) as vip_embarkation on (vip_embarkation.id=vi.imp_principal_place_of_slave_purchase_id);" % voyage_id
		cursor.execute(q)
		result=cursor.fetchone()
		voyage_id,voyage_disembarkation_date,voyage_embarkation_date,embarkation_port,disembarkation_port=result
		embarkation_month,embarkation_day,embarkation_year=dateparse(voyage_embarkation_date)
		disembarkation_month,disembarkation_day,disembarkation_year=dateparse(voyage_disembarkation_date)
	else:
		embarkation_month=embarkation_day=embarkation_year=disembarkation_month=disembarkation_day=disembarkation_year=voyage_id=voyage_disembarkation_date=voyage_embarkation_date=embarkation_port=disembarkation_port=None

	#print(voyage_id,voyage_disembarkation_date,voyage_embarkation_date,embarkation_port,disembarkation_port)
	
	yearlist=[i for i in [transaction_year,embarkation_year,disembarkation_year] if i is not None]
	if yearlist!=[]:
		year=min(yearlist)
		
	params={
	"text_ref":text_ref,
	"voyage_id":voyage_id,
	"enslaver_id":enslaver_id,
	"enslavement_type":enslavement_type,
	"enslaver_role":enslaver_role,
	"enslaved_id":enslaved_id,
	"enslavement_type":enslavement_type,
	"transaction_location":transaction_location,
	"transaction_month":transaction_month,
	"transaction_day":transaction_day,
	"transaction_year":transaction_year,
	"embarkation_month":embarkation_month,
	"embarkation_day":embarkation_day,
	"embarkation_year":embarkation_year,
	"disembarkation_month":disembarkation_month,
	"disembarkation_day":disembarkation_day,
	"disembarkation_year":disembarkation_year,
	"year":year,
	"embarkation_port":embarkation_port,
	"disembarkation_port":disembarkation_port
	}
	for p in list(params.keys()):
		if params[p] is None:
			del(params[p])
	
	if year is None:
		print(params)
			
	paramstr=','.join(["%s:$%s" %(k,k) for k in params])
	
	query="""MATCH (enslaver:person {person_id:$enslaver_id}),(enslaved_person:person {person_id:$enslaved_id}) CREATE (enslaver)-[:enslaved {%s}]->(enslaved_person)""" %paramstr
	
	#print(query)
	
	result = redis_graph.query(query,params)
	#print('----------')
	#result.pretty_print()
	#print(query,params)
	redis_graph.commit()
	del redis_graph
	#print('----------')
	
print(time.time()-st,"seconds")
