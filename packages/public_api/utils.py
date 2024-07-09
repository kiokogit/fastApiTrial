from collections import defaultdict
from itertools import groupby
import json
from pprint import pformat
import re

from typing import Annotated
from pydantic import BaseModel
from fastapi import Depends
from datetime import date, datetime, timedelta
from uuid import UUID

from loguru import logger
import pytz

from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from sqlalchemy import and_


from fastapi import HTTPException
from jose import jwt

from arbm_core.public.projects import Project, UserProjectAssociation
from arbm_core.public.users import ClientUser
from arbm_core.public.logging import LogItem
from arbm_core.private import Session as BackendSession

from schemas.user import User, UserInDB
from schemas.project import ProjectSchema


ALGORITHM = "HS256"
SECRET_KEY = "e156304924a4e40ba5866728db07c02ad1100abb1ff4627fc7e3696820ca9c9c"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (UUID)):
        return str(obj)
    return str(obj)
    # raise TypeError ("Type %s not serializable" % type(obj))


class TokenData(BaseModel):
    username: str | None = None


def get_user(*, username: str):
    with BackendSession() as session:
        user_obj = session.query(ClientUser).get(username)
        if not user_obj:
            raise HTTPException(401, 'Could not validate credentials')
        return UserInDB.from_orm(user_obj)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str | Any = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
        if token_data.username is None:
            raise credentials_exception
        user = get_user(username=token_data.username)
    except JWTError:
        raise credentials_exception
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if not current_user.active:
        raise HTTPException(status_code=401, detail="Inactive user")
    return current_user


LoggedInUser = Annotated[User, Depends(get_current_active_user)]


def get_project_feedback(s, user, user_project):
    all_feedback = []

    for rec in user_project.project.users_recommended(user.organization_id):
        all_feedback.append({
            'user': rec.username,
            'feedback': rec.feedback,
            'feedback_time': rec.feedback_posted,
            'rating': rec.rating,
        })

    return all_feedback


def convert_timeline(signals: list):
    by_year = defaultdict(lambda: defaultdict(list))

    for signal in signals:
        year, month = signal.date.year, signal.date.month
        by_year[year][month].append(signal)

    timeline = [{
        'year': year,
        'months': [
            {
                'month': month,
                'signals': (month_signals := [{'fund_uuid': k, 'count': len(list(g))} for k, g in groupby(signals, key=lambda x: x.fund_uuid)]),
                'total': sum([s['count'] for s in month_signals])
            }
            for month, signals in months.items()
        ]
    } for year, months in by_year.items()]

    for year in timeline:
        year['total'] = sum([month['total'] for month in year['months']])

    # flat_timeline = []
    # if flatten:
    #     for year, months in timeline.items():
    #         for month, data in months.items():
    #             for signal in data.get('signals', []):
    #                 flat_timeline.append({
    #                     **signal,
    #                     'signal_date': date(year, month, 1),
    #                 })
    #     timeline = flat_timeline

    return timeline


def get_user_project(s, project_id: UUID, current_user, raise_if_none: Exception | None = HTTPException(status_code=404, detail="Project not found")) -> UserProjectAssociation:
    p = s.query(UserProjectAssociation).filter(and_(UserProjectAssociation.revoked==False,
                    UserProjectAssociation.project_id==project_id, UserProjectAssociation.username==current_user.username)).one_or_none()

    if not p and raise_if_none:
        raise raise_if_none

    return p


def get_user_feed_projects(user, query_date: date | None = None, favourite: bool | None = None, archived: bool | None = None) -> list[dict]:
    with BackendSession() as s:
        filters = [
            UserProjectAssociation.username==user.username,
            UserProjectAssociation.revoked==False,
            UserProjectAssociation.time_recommended <= datetime.now(tz=pytz.UTC),
        ]

        if query_date:
            filters.append(UserProjectAssociation.time_recommended >= datetime.combine(query_date, datetime.min.time()).astimezone(pytz.UTC))

        if favourite is not None:
            filters.append(UserProjectAssociation.favourite==favourite)

        if archived is not None:
            #print(archived)
            #filters.append(UserProjectAssociation.time_recommended >= datetime.now(pytz.UTC))
            filters.append(UserProjectAssociation.archived==archived)

        projects = s.query(Project, UserProjectAssociation).join(UserProjectAssociation)\
                    .filter(Project.website != None)\
                    .filter(*filters)\
                    .order_by(UserProjectAssociation.time_recommended.desc())\
                    .all()

        res = []
        for p in projects:
            feed_entry = {
                'project': ProjectSchema.from_orm(p[0]),
                'project_user_info': p[1],
                'comments': get_project_feedback(s, user, user_project=p[1])
            }

            res.append(feed_entry)

        return res


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str):
    user = get_user(username=username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def get_password_hash(password):
    return pwd_context.hash(password)


def cap_value(lower: int, upper: int, val: int):
    if not upper > lower:
        raise ValueError

    return min(max(val, lower), upper)


def get_ratio(min_val, max_val, val):
    """
    Given a lower and an upper bounds,
    return the ratio of value against the range between 0 and 1,
    where value is capped at the bounds
    """
    if not max_val > min_val:
        raise ValueError

    val = cap_value(min_val, max_val, val)
    ratio = (val - min_val) / (max_val - min_val)
    return ratio


def color_scaler(min_bound, max_bound, exponential=False):
    def scale_color(color_val, min_val: int, measure: int):
        offset = (color_val - min_val) * get_ratio(min_bound, max_bound, measure)

        a = 10
        if exponential:
            offset = (a ** offset - 1) / (a - 1)

        scaled = min_val + offset
        return scaled

    return scale_color


def clean_query_string(query_str: str):
    query_str = re.sub(r'\s+', ' ', query_str).strip().lower()

    if not re.search(r'\w+', query_str):
        raise HTTPException(status_code=400, detail='query string must not be empty')

    return query_str


def log_user_event(user: User, event: str, details: dict):
    details['user'] = user.username

    details_serial = json.dumps(details, default=json_serial)
    details_safe = json.loads(details_serial)

    with BackendSession() as log_session:
        log = LogItem(event=event, details=details_safe)
        log_session.add(log)
        log_session.commit()


def get_filter_options(projects):
    funds = sorted(list(set([f.name for p in projects for f in (p['project'].funds or [])])), key=lambda x: x.lower())
    verticals = sorted(list(set([v for p in projects for v in (p['project'].verticals or [])])), key=lambda x: x.lower())

    return {'funds': funds, 'verticals': verticals}







