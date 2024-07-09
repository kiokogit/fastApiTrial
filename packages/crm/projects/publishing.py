import enum

from loguru import logger
from pydantic import ValidationError
from sqlalchemy import select

import arbm_core.private as private
from arbm_core.private.projects import TrackedProject, ProjectAnalytics, FieldConfig, ProjectStatus
from arbm_core.public.projects import Project, Contact, MiscEntry, SocialEntry
from arbm_core.public import _TAG_ATTRS

# from arbm_core.public.schemas.project import ProjectSchema
# from ...public_api.schemas.project import ProjectSchema


class PublishingError(Exception):
    pass


def validate_publishing(project: TrackedProject, require_details_fields):
    with private.Session() as s:
        if project.status not in (ProjectStatus.accepted, ProjectStatus.review, ProjectStatus.pending, ProjectStatus.discovered, ProjectStatus.published):
            raise PublishingError('project must be accepted or pending before publishing!')

        analytics: ProjectAnalytics = project.analytics
        if not analytics:
            logger.critical(f'project {project.title} does not have an associated analytics data!')
            raise PublishingError('project does not have analytics data')


        if require_details_fields:
            required_fields = set(s.scalars(select(FieldConfig.field_name).filter(FieldConfig.enabled==True)).all())

            available_details = set()
            available_tags = set()

            for d in analytics.details:
                if d.type in available_details:
                    raise PublishingError("duplicate details entries found with"
                                        f" the same type '{d.type}'!"
                                        " this shouldn't happen...")

                available_details.add(d.type)

            for t in analytics.tags:
                available_tags.add(t.tag_type)

            if available_tags.intersection(available_details):
                raise PublishingError("project has conflicting tag and details values")

            project_fields = available_details.union(available_tags)

            if not required_fields.issubset(project_fields):
                raise PublishingError("some of the required fields are missing:"
                                    f"{required_fields.difference(project_fields)}")

        return True


def publish_project(*, project_uuid, require_details_fields=True):
    with private.Session(autoflush=False) as private_s:
        project: TrackedProject = private_s.query(TrackedProject).filter(TrackedProject.uuid == project_uuid).one_or_none()

        if not validate_publishing(project, require_details_fields):
            raise PublishingError('project does not pass publishing validation')

        public_entry: Project = private_s.get(Project, project_uuid)

        if public_entry is None:
            logger.critical(f"published project instance not found for {project.title}, creating...")

            if not project.discovered_date:
                logger.critical(f'no discovered date for project {project.title}')
                raise PublishingError('project is missing discovered date')

            public_entry = Project(
                uuid=project.uuid,
                discovered_date=project.discovered_date,
            )
        else:
            logger.critical(f"found published instance for {project.title}, updating...")

        # publish and update project funds
        # public_entry.funds = []

        # for f in project.interested_funds:
            # if fund is published, update it
            # if fund := private_s.query(FundProfile).get(f.uuid):
            #     fund.logo = f.logo
            #     fund.name = f.name
            #     private_s.add(fund)

            # use existing fund or create a new one
            # public_entry.funds.append(fund or FundProfile(uuid=f.uuid,
            #                                               name=f.name,
            #                                               logo=f.logo)
            #                          )

        analytics: ProjectAnalytics = project.analytics

        # industry & verticals
        # public_entry.industry = 'mobility'
        public_entry.verticals = [t.tag_name for t in analytics.get_attr(attr_type='tag',
                                                                     attr_name='verticals')]

        # project details
        public_entry.title = project.title
        public_entry.website = project.get_website()
        public_entry.logo = project.logo

        if not public_entry.website:
            raise PublishingError('project with title {project.title} is missing website')

        # urls
        socials = []

        if project.twitter:
            socials.append(SocialEntry(
                title='twitter_url',
                url=f'https://twitter.com/{project.twitter.username}',
                icon='twitter',
            ))

        if project.linkedin_profile:
            socials.append(SocialEntry(
                title='linkedin_url',
                url=project.linkedin_profile.linkedin_url or urls[0],
                icon='linkedin',
            ))

        for link_name in ['crunchbase', 'pitchbook']:
            if url := project.get_link(link_name):
                socials.append(SocialEntry(
                    title=f'{link_name}_url',
                    url=url,
                    icon=link_name,
                ))

        public_entry.socials = socials


        description_fields = ['description', 'summary', 'headline', 'catchline', 'Description']
        for field in description_fields:
            if (description := analytics.get_attr(field, 'detail')) and (description.value):
                public_entry.about = description.value
                break

        tag_entries = []
        for attr_name in _TAG_ATTRS:
            if val := getattr(analytics, attr_name):
                tag_entries.append(MiscEntry(
                    title=attr_name,
                    content=val if isinstance(val, enum.Enum) else str(val),
                    # icon=,
                    # category=,
                ))
        public_entry.tags = tag_entries

        public_entry.contacts = [
                Contact(
                    project=public_entry,
                    name=l.name,
                    linkedin=l.linkedin,
                    email=l.email,
                    img=l.img,
                    role=l.role,
                    recommended=l.recommended,
                )
                for l in analytics.leaders]

        # try:
        #     ProjectSchema(**public_entry.__dict__)
        # except ValidationError:
        #     logger.error(f"failed validating project {project}")
        #     raise PublishingError(f"failed validating project {project}")

        private_s.add(public_entry)
        private_s.commit()

        return public_entry
