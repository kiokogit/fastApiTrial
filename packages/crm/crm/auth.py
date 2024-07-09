from datetime import datetime, timedelta
import hashlib
from typing import Any, Annotated

from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi import APIRouter, Depends, HTTPException, status
from passlib.context import CryptContext
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound
from loguru import logger

from jose import JWTError, jwt

from arbm_core.private import Session
from arbm_core.private.users import User as CRMUser

router = APIRouter()


ALGORITHM = "HS256"
SECRET_KEY = "e156304924a4e40ba5866728db07c02ad1100abb1ff4627fc7e3696820ca9c9c"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None


class User(BaseModel):
    username: str
    role: str | None

    active: bool | None = None

    class Config:
        orm_mode = True


class UserInDB(User):
    password: str

    class Config:
        orm_mode = True


def get_user(*, username: str) -> UserInDB:
    with Session() as session:
        user_obj = session.query(CRMUser).filter_by(username=username).one()

        if not user_obj or not user_obj.password:
            raise HTTPException(
                status_code=status.HTTP_401_NOT_AUTHORIZED,
                detail="User is not active!",
            )

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
    return current_user


LoggedInUser = Annotated[User, Depends(get_current_active_user)]


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str):
    try:
        user = get_user(username=username)
    except NoResultFound:
        return False

    # if not verify_password(password, user.hashed_password):
    if not user.password == str(hashlib.sha256(password.encode()).hexdigest()):
        return False
    return user


@router.post("/auth")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()) -> Token:
    user = authenticate_user(form_data.username, form_data.password)
    logger.critical(form_data.username)
    logger.critical(user)
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


@router.get("/users/me")
async def read_users_me(current_user: LoggedInUser) -> User:
    return current_user
