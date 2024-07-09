from projects.schemas.signals import InvestorIdSchema
from util import validate_linkedin_url


from pydantic import BaseModel, HttpUrl, constr, root_validator, validator


class LikerSchema(BaseModel):
    linkedin_id: str
    liker_url: constr(min_length=10)

    name: constr(min_length=1)
    keyword: str
    title: str
    img_id: str | None

    @validator('linkedin_id')
    def validate_linkedin_id(cls, v):
        if v is None:
            return v
        # remove parameters from linkedin_id, e.g. from investor-id-00000?miniProfileUrn=urn%
        # remove ?miniProfileUrn=urn%
        v = v.split('?')[0]
        return v

    @root_validator(pre=True)
    def handle_camel_case(cls, values):
        values['liker_url'] = values['link']
        values['img_id'] = values['imgId']

        return values

    @validator('liker_url')
    def validate_liker_url(cls, v):
        v = validate_linkedin_url(v)
        return v


class LinkedinLikesSignalSchema(BaseModel):
    # signal data
    post_url: HttpUrl

    investor: InvestorIdSchema

    leader_signals: list[LikerSchema]

    # metadata
    activity_type: str | None
    days_since_posted: int | None
    number_of_likes: int | None

    @root_validator(pre=True)
    def handle_camel_case(cls, values):
        values['post_url'] = values['postUrl']
        values['investor_id'] = values.get('investorId')
        values['investor_url'] = values.get('investorUrl')
        values['activity_type'] = values['activityType']
        values['days_since_posted'] = values['daysSincePosted']
        values['number_of_likes'] = values['numberOfLikes']

        values['leader_signals'] = values.get('founders') or values.get('competitors')
        return values

    @root_validator(pre=True)
    def require_investor_id_or_url(cls, values):
        investor_id, investor_url = values.get('investor_id'), values.get('investor_url')

        if not any([investor_id, investor_url]):
            raise ValueError('at least one of investor_id or investor_url must be supplied')

        values['investor'] = InvestorIdSchema(id=investor_id)
        return values

    @validator('post_url')
    def validate_post_url(cls, v):
        v = validate_linkedin_url(v)
        return v

    # @validator('investor_url')
    # def validate_investor_url(cls, v):
    #     if v is None:
    #         return v
    #     v = validate_linkedin_url(v)
    #     return v
