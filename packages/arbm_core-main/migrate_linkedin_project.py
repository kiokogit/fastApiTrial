from arbm_core.private import Session


def migrate():
    with Session() as s:
        linkedin_projects = s.scalars()
 

if __name__ == '__main__':
    migrate()
