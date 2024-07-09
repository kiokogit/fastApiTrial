from typing import Annotated

from loguru import logger
from fastapi import APIRouter, Body, HTTPException
from pydantic import EmailStr

from arbm_core.public.promo import TerminalRequest, NewsletterSubscriber

from dependencies import PrivateSession
from notifications import make_request_email
from schemas.schemas import AccessRequest

router = APIRouter()


@router.post('/request', status_code=204)
def request_dataset(s: PrivateSession, request_form: AccessRequest):
    if res := s.get(TerminalRequest, {'email': request_form.email, 'purpose': request_form.request.purpose}):
        raise HTTPException(status_code=400, detail=f'Request of type {request_form.request.purpose} already exists for the email {request_form.email}!')

    access_request = request_form.request.dict()
    access_request.update(email=request_form.email)
    s.add(TerminalRequest(**access_request))
    s.commit()

    make_request_email(s,
                              email=request_form.email,
                              text='A new dataset access request has been submitted.',
                              **request_form.request.dict()
                            )

    return


@router.post('/subscribe', status_code=204)
def subscribe(s: PrivateSession, email: Annotated[EmailStr, Body()], company: Annotated[str, Body()] | None = None):
    if s.get(NewsletterSubscriber, email):
        raise HTTPException(status_code=400, detail='Email already subscribed!')

    sub = NewsletterSubscriber(email=email, company=company)
    s.add(sub)
    s.commit()

    return