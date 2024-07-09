import json
from pprint import pprint
import re
from loguru import logger

import pandas as pd

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from arbm_core.private.investors import Fund
from arbm_core.private import Session
from arbm_core.private.projects import TrackedProject, ProjectAnalytics
from arbm_core.private.linkedin import LinkedinCompany
from api_external.iscraper import profile_company_details_v3
from projects import FilterPreconditionException
from projects.linkedin_utils import extract_project_data, parse_company_data

from projects.project_filtering import filter_signal
from projects.schemas.signals import FundIdSchema

from sqlalchemy import create_engine, Column, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

import util
from util import UrlParsingError

prodpass = '''Jfae723\^\&\{\{\,D\#234gODz3/'''
engine = create_engine(f"postgresql://alpha:Jfae723%5E&%7B%7B,D%23234gODz3%2F@167.71.140.237/alphaterminal", echo=False, future=True)
Prod = sessionmaker(engine, future=True)


def format_thesis():
    thesises = pd.read_csv('tests/thesis.csv')

    cols = ['Funding Stage', 'Geographic Preferences', 'Target Verticals', 'Industries/Verticals Specifics']

    out_records = []
    for i, r in thesises.iterrows():
        fund_thesis = ''

        for col in cols:
            content = r[col].replace('\n', ', ')
            fund_thesis += f'{col}: {content};\n'

        out_records.append(dict(
            fund_name=r['Name of Fund'],
            fund_thesis=fund_thesis
        ))

    out = pd.DataFrame.from_records(out_records)
    out.to_csv('fund_thesis_out.csv', index=False)


def set_thesis():
    name_column = 'Name of Fund'
    funds = pd.read_csv('new_thesis.csv')

    with Session() as s, Prod() as p:
        for i, r in funds.iterrows():
            fund = s.scalars(select(Fund).where(Fund.name == r[name_column])).one_or_none()

            if fund is None:
                # fund = p.scalars(select(Fund).where(Fund.name == r[name_column])).one_or_none()
                prod_fund = p.scalars(select(Fund).where(Fund.name == r[name_column])).one_or_none()

                if not prod_fund:
                    print(f'fund not found {r[name_column]}')
                    continue

                try:
                    fund = Fund(id=prod_fund.id, name=prod_fund.name, uuid=prod_fund.uuid)
                    s.add(fund)
                    s.commit()
                except IntegrityError:
                    s.rollback()
                    fund = s.get(Fund, prod_fund.id)
                    fund.name = prod_fund.name
                    s.add(fund)
                    s.commit()
                s.refresh(fund)

            # print(fund, '\n', fund.thesis)
            fund.thesis = r['thesis_formatted']
            s.add(fund)

        s.commit()


def extract_companies():
    projects = pd.read_csv('tests/fund_signals.csv')


    out_rows = []
    for i, r in projects.iterrows():
        signals = [f.strip() for f in r['Signals'].split(',')]

        # print(r[0])
        # print(signals)

        for signal in signals:
            match = re.match(r'(.+)\s*\[(y|x)[\]|\[]', signal)
            if match:
                fund_name, mark = match.groups()
                fund_name = fund_name.strip()

                # print(f'"{fund_name}" "{mark}"')

                out_rows.append(dict(
                    project_id=r[0],
                    fund_name=fund_name,
                    mark=mark
                ))
            else:
                print(f'Failed for {signal}')

    out = pd.DataFrame.from_records(out_rows)
    out.to_csv('fund_signals_out.csv', index=False)


def filter_signals():
    inp = pd.read_csv('fund_signals_out.csv')
    outp = pd.read_csv('fund_signals_out_validated.csv')
    # outp.insert(len(inp.columns), 'validated', None)

    with Session() as s, Prod() as p:
        valid = 0
        all = 0
        not_found = 0

        for i, r in outp.iterrows():

            if (valid_y := outp.iloc[i]['validated']) in (True, False):
                valid += valid_y
                all += 1
                continue

            logger.debug(f'processing {i}th project')

            project: TrackedProject = s.scalars(select(TrackedProject).where(TrackedProject.id == r['project_id'])).one_or_none()

            print(f'fund name: *{r["fund_name"]}*')
            fund = s.scalars(select(Fund).where(Fund.name == r['fund_name'])).one_or_none()

            if not fund:
                prod_fund = p.scalars(select(Fund).where(Fund.name == r['fund_name'])).one_or_none()

                if not prod_fund:
                    logger.error(f'fund {r["fund_name"]} not found')
                    not_found += 1
                    continue
                else:
                    try:
                        fund = Fund(id=prod_fund.id, name=prod_fund.name, uuid=prod_fund.uuid)
                        s.add(fund)
                        s.commit()
                    except IntegrityError:
                        fund = s.get(Fund, prod_fund.id)
                        fund.name = prod_fund.name
                        s.add(fund)
                        s.commit()
                    s.refresh(fund)

            if project is None:
                prod_project = p.scalars(select(TrackedProject).where(TrackedProject.id == r['project_id'])).one_or_none()

                if prod_project:
                    print('found project in prod')

                    prod_data = p.execute('select * from discovered_projects as p join project_analytics as a '
                          f'on p.id = a.project_id where p.id = {prod_project.id}').one()

                    linkedin_url = p.execute('select c.linkedin_url, c.raw_data from discovered_projects as p join projects_linkedin as l '
                          'on p.id = l.tracked_project_id join linkedin_profiles as c '
                          f'on l.company_profile_id = c.id where p.id = {prod_project.id}').one()

                    pprint(prod_data)
                    pprint(linkedin_url)

                    project = TrackedProject(
                        id=prod_data[0],
                        title=prod_data[1],
                        website=prod_data[2],
                        source=prod_data[3],
                        status=prod_data[4],
                        uuid=prod_data[5],
                        analytics=ProjectAnalytics(),
                        linkedin_profile=LinkedinCompany(
                            name=prod_data[1],
                            linkedin_url=linkedin_url[0],
                            raw_data=linkedin_url[1]
                        ) #type: ignore
                    ) # type: ignore
                    s.add(project)
                    s.commit()

            if project is not None:
                # print(project)
                # print(r['fund_name'])
                # print(r['mark'])
                # print()

                # project.fund_signals.append(dict(
                #     fund_name=r['fund_name'],
                #     mark=r['mark']
                # ))

                # s.add(project)
                # s.commit()
                signal = FundIdSchema(fund_id=fund.id)

                # print(type(project.linkedin_profile.raw_data))
                # pprint(project.linkedin_profile.raw_data)
                if project.linkedin_profile.raw_data is None:
                    try:
                        company_id = util.get_linkedin_id(project.linkedin_profile.linkedin_url, profile_type='company')
                    except UrlParsingError:
                        print(f'error parsing linkedin id from url: "{project.linkedin_profile.linkedin_url}"')
                        continue

                    project.linkedin_profile.raw_data = profile_company_details_v3(profile_id=company_id)
                    s.add(project)
                    s.commit()
                linkedin_data = project.linkedin_profile.raw_data

                project_data = extract_project_data(parse_company_data(linkedin_data))
                # print(project_data)

                if True:
                    try:
                        res = filter_signal(s, signal, project_data)
                    except FilterPreconditionException as e:
                        logger.error(e)
                        continue

                    mark = True if r['mark'] == 'y' else False

                    validated = mark == res.passed
                    valid += validated
                    all += 1

                    outp.at[i, 'validated'] = validated

                    print(f'res {i} is ' + ('HAPPY!!!!!1!!' if validated else 'SAAAAD :(((('))
                    outp.to_csv('fund_signals_out_validated.csv', index=False)

        print(valid, '/', all)
        print(not_found)



if __name__ == '__main__':
    # with Prod() as p:
    #     thesis2 = pd.read_csv('thesis2.csv')

    #     for i, r in thesis2.iterrows():
    #         id, thesis = r['id'], r['thesis']

    #         fund = p.get(Fund, id)
    #         fund.thesis = thesis
    #         p.add(fund)
    #         p.commit()

    # format_thesis()
    # set_thesis()
    # extract_companies()

    # filter_signals()