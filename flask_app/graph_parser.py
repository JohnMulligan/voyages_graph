import json

def dateformat(departuredate):
	outvals=[]
	for s in departuredate:
		if s is None:
			i='?'
		else:
			i=str(s)
		outvals.append(i)
	return('/'.join(outvals))

def main(enslavements):
	nodes={}
	edges={}
	c=0
	e_ids=[]
	for enslavement in enslavements:
		#print(e)
		s,e,t=enslavement
		s_id=s['identity']+1
		if s_id not in nodes:
			properties={'id':s_id,'group':'enslavers','size':1}
			if 'name' in s['properties']:
				properties['name']=s['properties']['name']
			else:
				properties['name']=''
			if 'principal_location' in s['properties']:
				properties['label']="location: "+s['properties']['principal_location']
			else:
				properties['label']=''
			nodes[s_id]=properties
		else:
			nodes[s_id]['size']+=1
	
		t_id=t['identity']+1
		if t_id not in nodes:
			properties={'id':t_id,'group':'enslaved','size':1}
			if 'name' in t['properties']:
				properties['name']=t['properties']['name']
			else:
				properties['name']=''
			if 'age' in t['properties']:
				properties['label']='age ' + str(t['properties']['age'])
			else:
				properties['label']='age unknown'
			nodes[t_id]=properties
		
		else:
			nodes[t_id]['size']+=1
	
		#print(e)
		e_id=e['identity']
		ep=e['properties']
		e_ids.append(e_id)
		#print(e_id)
		
		enslaver_role=ep['enslaver_role']
		
		if enslaver_role in ['consignor','owner','shipper']:	
			departuredate=[]
			for x in ['embarkation_month','embarkation_day','embarkation_year']:
				try:
					o=ep[x]
				except:
					o=None
				departuredate.append(o)
	
			departure_date_str=dateformat(departuredate)
	
			arrivaldate=[]
			for x in ['disembarkation_month','disembarkation_day','disembarkation_year']:
				try:
					o=ep[x]
				except:
					o=None
				arrivaldate.append(o)
			arrival_date_str=dateformat(arrivaldate)
	
			try:
				embarkation_port=e['embarkation_port']
			except:
				embarkation_port='port unknown'
			try:
				disembarkation_port=e['disembarkation_port']
			except:
				disembarkation_port='port_unknown'	
			
			e_str="transported as " + enslaver_role
			edges[e_id]={'source':s_id,'target':t_id,'group':e_str,'id':e_id}
		elif enslaver_role in ['buyer','seller']:
			transactiondate=[]
			for x in ['transaction_month','transaction_day','transaction_year']:
				try:
					o=ep[x]
				except:
					o=None
				transactiondate.append(o)
	
			transaction_date_str=dateformat(transactiondate)
		
			transaction_location=ep['transaction_location']
		
			group={'buyer':'enslaved: bought','seller':'seller: sold'}
			
			edges[e_id]={'source':s_id,'target':t_id,'group':group[enslaver_role],'id':e_id}
		
	flatnodes=[nodes[i] for i in nodes]

	#print(flatnodes)

	flatedges=[edges[i] for i in edges]

	#print(flatedges)
	
	return {'nodes':flatnodes,'links':flatedges}

if __name__=='__main__':
	d=open('graph.json','r')
	t=d.read()
	j=json.loads(t)
	d.close()
	j2=main(j)
	d=open('final.json','w')
	d.write(json.dumps(j2))
	d.close()