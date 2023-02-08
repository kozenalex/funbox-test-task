from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from typing import List
from pydantic import BaseModel, validator
import redis
from urllib.parse import urlparse
import datetime, time
import settings

app = FastAPI()

r = redis.StrictRedis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD
)

class RequestModel(BaseModel):

    links: List[str]

    @validator('links')
    def check_strs(cls, links):
        for link in links:
            if not urlparse(link).scheme and not urlparse("//" + link).netloc:
                raise RequestValidationError
        return links

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    content = {
            "status": "POST data is invalid"
        }
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=content
    )

@app.get('/clear_db')
async def clear_db():
    try:
        keys = r.keys('*')
        r.delete(*keys)
        return {
            'deleted keys': keys
        }
    except redis.exceptions.ConnectionError:
        return {
            "status": "DB connection error"
        }

@app.get("/visited_links")
async def show_links(start:int = 0, to:int = 0):
    answer = []
    try:
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
    except redis.exceptions.ConnectionError:
        return {
            "status": "DB connection error"
        }

@app.post("/visited_links")
async def get_links(links: RequestModel):
    now = int(time.mktime(datetime.datetime.now().timetuple()))
    domains = [
        urlparse(l).netloc if urlparse(l).scheme else urlparse('//'+l).netloc for l in links.dict()["links"]
    ]
    domains_set = set(domains)
    try:
        r.hmset(now, {'domains': ' '.join(domains_set)})
        return {
            'status': 'ok'
        }
    except redis.exceptions.ConnectionError:
        return {
            "status": "DB connection error"
        }
