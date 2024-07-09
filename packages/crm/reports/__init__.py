from datetime import datetime

from arbm_core.public.schemas.feed import ProjectUserInfo
from pydantic import BaseModel, root_validator


class UserStatsSchema(BaseModel):
    username: str

    all_projects: list[ProjectUserInfo]

    great_projects: list[ProjectUserInfo]
    good_projects: list[ProjectUserInfo]
    unfit_projects: list[ProjectUserInfo]

    unrated_projects: list[ProjectUserInfo]
    projects_with_feedback: list[ProjectUserInfo]

    rated_projects_percentage: int
    feedback_projects_percentage: int
    last_feedback: datetime | None

    @root_validator(pre=True)
    def find_last_feedback(cls, values):
        feedbacks = [f.feedback_posted
                     for f in values['projects_with_feedback'] if f.feedback_posted]

        last_feedback = max(feedbacks) if feedbacks else None
        values['last_feedback'] = last_feedback
        return values

    def n_total_projects(self):
        return len(self.all_projects)

    def n_projects_rated(self):
        return len(self.great_projects + self.good_projects + self.unfit_projects)

    def n_projects_feedback(self):
        return len(self.projects_with_feedback)

    def n_projects_full_marks(self):
        full_marks = 0
        for p in self.all_projects:
            if p.project_id not in [u.project_id for u in self.unrated_projects] \
            and p.project_id in [
                    f.project_id for f in self.projects_with_feedback
            ]:
                full_marks += 1
        return full_marks

    def activity_percentage(self):
        total_activity = self.n_projects_full_marks()

        if not self.n_total_projects():
            return 0

        return round(total_activity / self.n_total_projects() * 100)
