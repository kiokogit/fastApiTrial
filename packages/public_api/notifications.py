from datetime import datetime

from arbm_core.public.users import ClientUser, Email
from arbm_core.public.projects import UserProjectAssociation


def notify_feedback(s, project_user: UserProjectAssociation):
    notify_users = [
        s.query(ClientUser).get('DanielAfshar')
    ]

    plaintext = 'User {user} from {org} has just submitted feedback'\
                ' for the project {project_title}:\n {feedback}'.format(
                    user=project_user.user.username,
                    org=project_user.user.organization.name,
                    project_title=project_user.project.title,
                    feedback=project_user.feedback,
                )

    html = plaintext.replace('\n', '<br/>')

    for u in notify_users:
        email = Email(
            user=u,
            type='feedback_notify',
            plaintext=plaintext,
            html=html,
            time_scheduled=datetime.now()
        )

        s.add(email)
    s.commit()


def make_request_email(s, email: str, text: str, purpose: str, **form_data):
    notify_users = [
        s.query(ClientUser).get('DanielAfshar')
    ]

    plaintext = '\n'.join(f'{key}: {val}' for key, val in form_data.items())
    plaintext = text + '\n' + f'{email}' + '\n\n' + plaintext

    html = plaintext.replace('\n', '<br/>')

    for u in notify_users:
        email = Email(
            user=u,
            type=purpose,
            plaintext=plaintext,
            html=html,
            time_scheduled=datetime.now()
        )

        s.add(email)
    s.commit()
