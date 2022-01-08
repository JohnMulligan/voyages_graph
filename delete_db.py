import json
import sqlite3
import redis
from redisgraph import Node, Edge, Graph, Path

r = redis.Redis(host='localhost', port=6379)
Graph('enslavements',r).delete()