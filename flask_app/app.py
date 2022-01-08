import re
import json
import os
import time
import redis
from redisgraph import Node, Edge, Graph, Path
from flask import Flask, request, render_template, redirect
from graph_parser import main as graphparse

application = app = Flask(__name__)

def build_array(s,e,t):
	
	final=[]
	
	
	keymap={'id':'identity','properties':'properties','label':'label','relation':'group'}
	
	for i in [s,e,t]:
		
		d=i.__dict__
		print(d)
		
		formatted={}
		for k in keymap:
			if k in d:
				formatted[keymap[k]]=d[k]
		
		final.append(formatted)
	
	return final
		
	

#accepts year ranges in comma-delimited inclusive ranges, e.g. 1835,1837 or 1835,1835


@app.route('/get_years/<yearspan>')
def get_selection(yearspan):
	r = redis.Redis(host='localhost', port=6379)
	redis_graph = Graph('enslavements', r)
	years=[int(i) for i in yearspan.split(',')]
	years.sort()
	minyear,maxyear=years
	params={
		'minyear':minyear,
		'maxyear':maxyear
	}
	
	#this is fragile -- will only work if the query returns subject, edge, target response
	
	query="""match (enslaver:person) - [t:enslaved] -> (enslaved_person:person) where t.year>=$minyear AND t.year<=$maxyear return enslaver,t,enslaved_person"""
	result = redis_graph.query(query, params)
	
	set_array=[]
	
	for record in result.result_set:
		s,e,t=record
		set_array.append(build_array(s,e,t))
	
	graphready=graphparse(set_array)
	
	return graphready

if __name__ == "__main__":
	application.run()