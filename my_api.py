from fastapi import FastAPI, Request
from pydantic import BaseModel
import redis
from urllib.parse import urlparse
import datetime, time
from json import loads, dumps

app = FastAPI()

class RequestModel(BaseModel):

    links: list

r = redis.StrictRedis(
    host='127.0.0.1',
    port=6379,
    password=''
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/clear_db')
async def clear_db():
    keys = r.keys('*')
    r.delete(*keys)
    return {
        'deleted keys': keys
    }

@app.get("/visited_links")
async def show_links(start:int = 0, to:int = 0):
    answer = []
    if not start or not to:
        keys = r.keys('*')
    else:
        keys = [k for k in r.keys('*') if int(k) >= start and int(k) <= to]
    for key in keys:
        answer.extend(r.hmget(key, 'domains')[0].split())
    return {
        'domains': set(answer),
        'status': 'ok'
    }

@app.post("/visited_links")
async def get_links(request: Request):
    now = int(time.mktime(datetime.datetime.now().timetuple()))
    answer = await request.json()
    domains = [
        urlparse(l).netloc if urlparse(l).scheme else urlparse('//'+l).netloc for l in answer["links"]
     ]
    domains_set = set(domains)
    r.hmset(now, {'domains': ' '.join(domains_set)})
    return domains_set
