from typing import Annotated, Any
from arbm_core.private import Session as BackendSession
from fastapi import Depends, HTTPException, status
from jose import JWTError, jwt
from sqlalchemy.orm import Session

from schemas.user import TokenData, User
from utils import ALGORITHM, SECRET_KEY, oauth2_scheme, get_user


class PaginationParams:
    # don't allow unbounded limits
    MAX_LIMIT = 1000

    def __init__(self, offset: int = 0, limit: int = 100) -> None:
        self.offset = offset
        self.limit = min(limit, self.MAX_LIMIT)


class QueryParams(PaginationParams):
    def __init__(self,  offset: int, limit: int, q: str | None = None) -> None:
        self.q = q
        super().__init__(offset=offset, limit=limit)


QueryParams = Annotated[QueryParams, Depends(QueryParams)]


# DBs Dependency
def get_db_private():
    private_session = BackendSession()
    try:
        yield private_session
    finally:
        private_session.close()


PrivateSession = Annotated[Session, Depends(get_db_private)]


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