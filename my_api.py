from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import ValidationError
from typing import List
from pydantic import BaseModel, validator
import redis
from urllib.parse import urlparse
import datetime, time

app = FastAPI()

r = redis.StrictRedis(
    host='127.0.0.1',
    port=6379,
    password=''
)

class RequestModel(BaseModel):

    links: List[str]

    @validator('links')
    def check_links(cls, links):
        for link in links:
            if len(link) < 1:
            # if not urlparse(link).scheme and not urlparse(link).netloc:
                raise ValidationError
        return links

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    content = {
            "status": "wrong data"
        }
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=content
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
async def get_links(links: RequestModel):
    now = int(time.mktime(datetime.datetime.now().timetuple()))
    status = 'ok'
    try:
        domains = [
            urlparse(l).netloc if urlparse(l).scheme else urlparse('//'+l).netloc for l in links.dict()["links"]
        ]
        domains_set = set(domains)
        r.hmset(now, {'domains': ' '.join(domains_set)})
    except ValidationError as e:
        status = e.json()
    
    return {
        'status': status
    }
