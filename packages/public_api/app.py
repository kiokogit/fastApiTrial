from datetime import timedelta, datetime
import sys
from pprint import pprint
import os

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware

from api_analytics.fastapi import Analytics

from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend


import uvicorn
from pydantic import BaseModel
from loguru import logger

from routers import demo, feed, funds, projects, graph, marketing, reports, search, user

from schemas.user import User

from dependencies import LoggedInUser
from utils import ACCESS_TOKEN_EXPIRE_MINUTES, authenticate_user, create_access_token, log_user_event


logger.remove()
logger.add(sys.stderr)


class Token(BaseModel):
    access_token: str
    token_type: str


API_ANALYTICS_KEY = os.environ.get('API_ANALYTICS_KEY', None)

if not API_ANALYTICS_KEY:
    logger.warning('Could not get API key for api_analytics. Please set the API_ANALYTICS_KEY env variable')


app = FastAPI()

app.include_router(demo.router, prefix='/demo')
app.include_router(feed.router, prefix='/v1/feed')
app.include_router(projects.router, prefix='/v1/projects')
app.include_router(funds.router, prefix='/v1/fund')
app.include_router(graph.router, prefix='/v1/graph')
app.include_router(marketing.router, prefix='/v1/landing')
app.include_router(reports.router, prefix='/v1/reports')
app.include_router(search.router, prefix='/v1/search')
app.include_router(user.router, prefix='/v1/user')


origins = [
    "https://lookingglass.arbmintel.com/",
    "https://lookingglass.arbmintel.com",
    "https://www.lookingglass.arbmintel.com",
    "*"
]

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# external analytics
app.add_middleware(Analytics, api_key=API_ANALYTICS_KEY)


@app.on_event("startup")
async def startup():
#    FastAPICache.init(InMemoryBackend(), prefix="public-fastapi-cache")
    ...

@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()) -> Token:
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/me")
async def read_users_me(request: Request, current_user: LoggedInUser) -> User:
    log_user_event(user=current_user,
                   event=request.url.path,
                   details={'ip': request.client,})

    return current_user


def get_filter_options(projects):
    funds = sorted(list(set([f.name for p in projects for f in p['project'].funds])), key=lambda x: x.lower())
    verticals = sorted(list(set([v for p in projects for v in (p['project'].verticals or [])])), key=lambda x: x.lower())
    return {'funds': funds, 'verticals': verticals}


def start():
    uvicorn.run(app, host="0.0.0.0", port=8000)






