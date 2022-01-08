## Quick redis graph db container

```
docker run -p 6379:6379 -it --rm redislabs/redisgraph
```

Then, simply install the virtual environment

```
python3 -m venv venv
source /venv/bin/activate
pip3 install -r requirements.txt
```

Then, you should be able to import all the data into redis graph just by runinng

```
python sql_to_reisgraph.py
```

(it will take a while)


## flask app

Wrote a quick flask app that shows how the db can then be queried. It seems *really* fast.

Right now I just have a years endpoint.

Calling http://127.0.0.1:5000/get_years/1835,1836

Will execute the redisgraph query ```match (enslaving_person:person) - [enslavement:enslaved] -> (enslaved_person:person) where enslavement.arrival_year>=$minyear AND enslavement.arrival_year<=$maxyear return enslaving_person,enslavement,enslaved_person```

And return source-edge-target data in a json format readable by this observable notebook: https://observablehq.com/@johnmulligan/d3-force-directed-graph-on-canvas-with-drag-pan-and-zoom

In principle, then, we have a pipeline!


```
nodes	
	0	
		group	"enslavers"
		id	63584
		label	"location: New Orleans"
		name	"Green, John"
		size	1
...
links	
	0	
		group	"enslaved: bought"
		id	108
		source	63584
		target	2859
...
```

## Built in Levenstein search!

* https://oss.redis.com/redisearch/Query_Syntax/#fuzzy_matching
* https://oss.redis.com/redisgraph/commands/#full-text-indexes
* https://github.com/RedisGraph/RedisGraph/issues/853#issuecomment-571019701


## Other docs I used

* https://developer.redis.com/explore/redisinsight/redisgraph/
* https://oss.redis.com/redisgraph/commands/
* https://developer.redis.com/howtos/redisgraph/explore-python-code/
* https://reposhub.com/python/databases-implemented-in-python/emehrkay-rgp.html










## Redis insight

Redis insight can be set up like so (but it's not really worth it): 

Launch redisinstight:

```
docker run -d -v redisinsight:/db -p 8001:8001 redislabs/redisinsight:latest
```

Get the container ip address (until I figure out networking...)

```
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 48ae2862b08b(use container id)
```






