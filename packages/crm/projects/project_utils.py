import os
from pprint import pformat


from arbm_core.private.investors import Investor

from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.sql.expression import func

from projects import ProjectUpdateError


MODULE_NAME = os.path.basename(__file__)


def get_investor(s, *, investor_id: int | None = None, investor_url: str | None = None) -> Investor:
    if not investor_id and not investor_url:
        raise ValueError("investor_id or investor_url must be supplied!")

    if investor_id and investor_url:
        raise ValueError("only one investor identificator must be supplied!")

    investor = None
    if investor_id:
        investor = s.get(Investor, investor_id)
    elif investor_url and (cleaned_url := investor_url):
        try:
            investor = s.query(Investor).filter(
                func.lower(Investor.linkedin_url) == func.lower(cleaned_url)
            ).one()
        except MultipleResultsFound:
            raise ProjectUpdateError(f"multiple investors found with url {cleaned_url}," \
                                   "when one is expected!")
        except NoResultFound:
            similar_investors = s.query(Investor).filter(Investor.linkedin_url.ilike(f'%{cleaned_url}%')).all()

            similar_investors_str = '\n'
            if len(similar_investors) > 0:
                similar_investors_str = (
                                        'but found investors with similar urls:\n\n'
                                        + ('\n\n'.join([f"{i.name}, {i.linkedin_url}"
                                                        for i in similar_investors]))
                                        + '\n\nchoose one of the above urls or '
                                    )

            raise ProjectUpdateError("investor not found with url"
                                  f"\n'{cleaned_url.lower()}'\n"
                                  f"{similar_investors_str}"
                                  "use admin panel to upload investor")

    return investor


