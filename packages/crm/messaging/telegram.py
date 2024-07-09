from pprint import pprint

from notifiers import notify


def send_message(*, telegram_id, msg, **kwargs):
    # msg = msg.replace('\n', '\n\n')

    notify('telegram', message=msg,
                       token='5788826502:AAFItnVdom6hUMp-_zkYr7Y8KHicf2X8Qi4',
                       chat_id=telegram_id,
                       parse_mode='html')
