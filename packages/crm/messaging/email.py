import os
import smtplib
from email.headerregistry import Address
from email.message import EmailMessage

from loguru import logger

SMTP_SERVER = os.environ["SMTP_SERVER"]
SMTP_PORT = int(os.environ["SMTP_PORT"])
SENDER_LOGIN = os.environ["SMTP_USER"]
SENDER_PASSWORD = os.environ["SMTP_PASSWORD"]

SMTP_DEBUG_LEVEL = os.environ.get("SMTP_DEBUG_LEVEL", "0")
SMTP_DEBUG_LEVEL = int(SMTP_DEBUG_LEVEL) if SMTP_DEBUG_LEVEL.isnumeric() else None

logger.debug(f'SMTP DEBUG LEVEL SET TO: {SMTP_DEBUG_LEVEL}')
if SMTP_DEBUG_LEVEL is None:
    raise ValueError("non-numeric value provided for SMTP_DEBUG_LEVEL")


def smtp_send(subject, contents_plaintext, contents_html, sender: Address,
              receiver: Address):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    msg.set_content(contents_plaintext)
    msg.add_alternative(contents_html, subtype="html")

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.set_debuglevel(SMTP_DEBUG_LEVEL)
        server.login(SENDER_LOGIN, SENDER_PASSWORD)
        server.send_message(msg)

    return True
