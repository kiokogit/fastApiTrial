from email.headerregistry import Address
from sqlalchemy import or_

from arbm_core.private import Session
from arbm_core.private.users import User

from messaging import telegram
from messaging.email import smtp_send


USER_RECEPIENTS = {
    'alpha': {
        'email': 'theiterance@gmail.com',
        'telegram_id': '273134893'
    }
}


def get_subscribers(event_type: str):
    with Session() as session:
        subscribers = session.query(User).filter(
            or_(
                User.notifications.any('all'),
                User.notifications.any(event_type)
            )
        ).all()

        return subscribers


EVENT_CHANNELS = {
    'parsing_error': ['all'],
    'email_failed': ['email'],
}


def channel_enabled(event_type, channel):
    # if no channels defined for event, send to all
    event_channels = EVENT_CHANNELS.get(event_type, ['all'])

    return 'all' in event_channels or channel in event_channels


def notify_admins(event, header, message = '', email_html=None, *args, **kwargs):
    notification_subscribers = get_subscribers(event_type=event)

    for subscriber in notification_subscribers:
        if channel_enabled(event, 'telegram') and subscriber.telegram:
            msg = f"<b>{header}</b>"
            telegram.send_message(
                telegram_id=subscriber.telegram,
                msg=msg,
                **kwargs
            )

        if channel_enabled(event, 'email') and (email := subscriber.email):
            (email_username, email_domain) = email.split("@")
            smtp_send(
                subject="Admin alert: LookingGlass",
                contents_plaintext='\n\n'.join([header, message]),
                contents_html='<br/><br/>'.join([header, email_html if email_html else message]),
                sender=Address("ARBM Service", "noreply", "arbmintel.com"),
                receiver=(Address(subscriber.username, email_username, email_domain)),
            )


def notify_parsing_error(header, error):
    notify_admins('parsing_error', header=header, message=error)


def notify_email_failed(msg):
    notify_admins('email_failed', header=msg)


def notify_sent_email(recepient, subject, content, content_html: None):
    notification_text = (
        f"Email of type <b>{subject}</b> has just been sent "
        f"to user <i>{recepient} @ {recepient.organization} ({recepient.email})</i>.\n"
    )

    notify_admins(event='client_email', header=notification_text, message=content, email_html=content_html)
