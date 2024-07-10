import datetime
import random
from pprint import pprint
import pytz

from sqlalchemy import func, distinct
from src.arbm_core.public.projects import UserProjectAssociation

from src.arbm_core.core.publishing import publish_project, PublishingError
# from arbm_core.merging.merge_projects import merge_objects
from src.arbm_core.private import Session
from src.arbm_core.public import Session as PublicSession
from src.arbm_core.private.projects import TrackedProject, ProjectAnalytics, DetailsEntry, \
    ProjectStatus, ProjectTagsAssociation
from src.arbm_core.public.projects import Project


def description_entries():
    with Session() as s:
        accepted_projects = s.query(TrackedProject).filter(TrackedProject.status==ProjectStatus.accepted)

        projects_with_tags = accepted_projects.join(ProjectAnalytics).filter(ProjectAnalytics.tags.any(
            ProjectTagsAssociation.effective_dates.contains(datetime.date.today())
        ))

        projects_with_details = accepted_projects.join(ProjectAnalytics).filter(ProjectAnalytics.details.any(
            DetailsEntry.effective_dates.contains(datetime.date.today())
        ))

        tags_by_type = s.query(ProjectTagsAssociation.tag_type, func.count(distinct(ProjectTagsAssociation.project_id)))\
            .group_by(ProjectTagsAssociation.tag_type, ProjectTagsAssociation.project_id)
        pprint(tags_by_type.all())

        details_by_type = s.query(DetailsEntry.type, func.count(DetailsEntry.type)) \
            .group_by(DetailsEntry.type)
        pprint(details_by_type.all())

        pprint(f'# accepted projects: {accepted_projects.count()}')

        pprint(f'# projects with tags / details: {projects_with_tags.count()} / {projects_with_details.count()}')


def enabled_tags():
    with Session() as s:
        accepted_project = s.query(TrackedProject).filter(TrackedProject.status == ProjectStatus.accepted, TrackedProject.analytics.has(ProjectAnalytics.tags.any())).first()
        print(accepted_project.analytics)
        pprint(accepted_project.analytics.tags)


def test_compute_timeline():
    with Session() as s:
        ids = random.choices(
            [p[0] for p in s.query(TrackedProject.id).filter(TrackedProject.status == ProjectStatus.accepted).all()],
            k=10
        )

        projects = s.query(TrackedProject).filter(TrackedProject.id.in_(ids)).all()

        for p in projects:
            print(p, p.uuid)
            print(' -> '.join([f"{s.date} {s.fund_uuid}" for s in p.compute_timeline()]))


def details_primaryjoin():
    with Session() as s:
        accepted_projects = s.query(TrackedProject).filter(TrackedProject.status == ProjectStatus.accepted,
                                                          TrackedProject.analytics != None).limit(50).all()

        for p in accepted_projects:
            analytics = p.analytics
            print(f"{len(analytics.tags)} = {len(analytics.historic_tags)}")


def test_publish_project():
    with Session() as s:
        accepted_projects = s.query(TrackedProject).filter(TrackedProject.status == ProjectStatus.accepted,
                                                           TrackedProject.analytics != None)\
            .order_by(TrackedProject.title).all()

        for i, p in enumerate(accepted_projects):
            try:
                publish_project(project_uuid=p.uuid)
            except PublishingError:
                continue


def test_merge_projects():
    with Session() as s:
        a = s.get(TrackedProject, '27908')
        b = s.get(TrackedProject, '20382')

        merge_objects(s, a, b)


def test_project_getattr():
    with PublicSession() as s:
        project = s.query(Project).filter(Project.tags.any()).first()

        print(project.x)


def test_projects_live():
    with PublicSession() as s:
        today = datetime.datetime.now(pytz.UTC) - datetime.timedelta(hours=24)

        projects = s.query(UserProjectAssociation)\
                    .filter_by(username='Mark')\
                    .filter(UserProjectAssociation.live == True,
                            UserProjectAssociation.time_recommended > today).all()

        print(projects)
        for p in projects:
            print(p, p.live)


if __name__ == '__main__':
    test_projects_live()
    # test_project_getattr()
    # test_merge_projects()
    # test_publish_project()
    # details_primaryjoin()
    # test_compute_timeline()
    # enabled_tags()
    # description_entries()
