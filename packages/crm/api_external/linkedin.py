from pathlib import Path

import config
import pandas as pd
from linkedin_api import Linkedin

# from api_external.base_parsing import PriorityParser
import util
from api_external import google_sheets
from api_external.phantom_buster import download_agent_output, run_pb_agent
from util import urls_to_usernames

email, password = Path(util.project_root(), "local/linkedin.txt").read_text().split("\n")
API = Linkedin(email, password)


def search_companies(name):
    res = API.search_companies(name, limit=3)

    for c in res:
        print(c["name"])


def get_company_info(company_id):
    res = API.get_company(company_id)
    return res


def parse_companies(urls: list[str]):
    company_ids = [url.strip("\t\n /").split("/")[-1] for url in urls]
    print(company_ids)

    for c_id in company_ids:
        # c = get_company_info(c_id)
        print(c_id)
        # for key, val in c.items():
        #     print(f'{key}: {val}')
        staff = API.search_people(current_company=c_id)
        print(len(staff))
        for p in staff:
            print(p)

        print()


# class LinkedinParser(PriorityParser):
#     def queue_profiles(self, profiles: list[str]):
#         pass


if __name__ == "__main__":
    companies_urls = [
        "https://www.linkedin.com/company/d3-network/",
        "https://www.linkedin.com/company/crypto-fundamental-secrets/",
        "https://www.linkedin.com/company/crypto-data-analytics/",
    ]
    parse_companies(companies_urls)
    # search_companies('d3 network')


async def parse_linkedin_employees(company_urls: list[str]):
    google_sheets.write_column(
        config.GOOGLE_WORKSHEET, col=config.WorksheetColumns.linkedin_urls, inp_values=company_urls
    )

    agent_id = config.LINKEDIN_EMPLOYEES_AGENT_ID
    res = await run_pb_agent(agent_id, out_prefix="")
    return res


async def fetch_linkedin(*args):
    linkedin_agent = "4513567032301059"
    # LinkedIn for startups
    # for each startup from before, fetch LinkedIn company and employees
    # drop duplicate startups here
    # twitter_linkedin = Path(util.project_root(), 'data/in/linkedin_urls.txt').read_text().split('\n')
    # twitter_urls, company_urls = zip(*[pair.split() for pair in twitter_linkedin])

    # res_file = asyncio.run(linkedin.parse_linkedin_employees(company_urls))
    # df_employees: pd.DataFrame = pd.read_csv(res_file)
    # df_employees.insert(loc=0, column="profileUrl", value=pd.Series(twitter_urls))

    file = await download_agent_output(linkedin_agent, "data/linkedin", "result", "csv")
    df_employees = pd.read_csv(file)
    print(df_employees[df_employees["error"].isna()])

    return df_employees[df_employees["error"].isna()]


def join_linkedin(startups: pd.DataFrame):
    # founders linkedin
    # linkedin_data = await fetch_linkedin()
    linkedin_twitter = pd.read_csv(util.project_root() / "data/linkedin/linkedin_urls.txt")
    linkedin_employees = pd.read_csv(util.project_root() / "data/linkedin/linkedin-employee-data.csv")

    company_to_twitter = pd.merge(
        linkedin_employees, linkedin_twitter, how="inner", left_on="query", right_on="linkedinUrl"
    )[["twitterUrl", "profileUrl"]]
    company_to_twitter = company_to_twitter[~company_to_twitter["profileUrl"].isna()]

    linkedin_data = pd.read_csv(util.project_root() / "data/linkedin/result.csv")
    linkedin_data = linkedin_data[~linkedin_data["linkedinProfile"].isna()]

    founder_to_twitter = pd.merge(
        linkedin_data, company_to_twitter, how="left", left_on="baseUrl", right_on="profileUrl"
    )
    founder_to_twitter = founder_to_twitter[~founder_to_twitter["linkedinProfile"].isna()]
    founder_to_twitter = founder_to_twitter[
        [
            "baseUrl",
            "company",
            "companyUrl",
            "headline",
            "jobTitle",
            "jobDescription",
            "school",
            "twitterUrl",
            "profileUrl",
        ]
    ]
    founder_to_twitter = founder_to_twitter[~founder_to_twitter["twitterUrl"].isna()]
    founder_to_twitter["twitterUrl"] = urls_to_usernames(founder_to_twitter["twitterUrl"]).apply(str.lower)

    print(founder_to_twitter)
    print(len(founder_to_twitter), "projects mapped to linkedin")

    startups = pd.merge(
        startups,
        founder_to_twitter[["companyUrl", "twitterUrl"]],
        left_on="profileUrl",
        right_on="twitterUrl",
        how="left",
    )
    startups["companyUrl"] = startups.apply(
        lambda row: row["companyUrl_x"] if not pd.isnull(row["companyUrl_x"]) else row["companyUrl_y"], axis=1
    )
    startups = startups.drop(labels=["companyUrl_x", "companyUrl_y", "twitterUrl_y"], axis=1)
    startups = startups.rename({"twitterUrl_x": "twitterUrl"})

    return startups, founder_to_twitter
