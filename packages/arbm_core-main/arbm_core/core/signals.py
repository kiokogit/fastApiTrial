import datetime
from pprint import pprint, pformat
from typing import Any
from uuid import UUID

from loguru import logger

import pytz

from pydantic import BaseModel, conint
from pymongo.database import Database

from arbm_core.core import MongoDb
from arbm_core.core.mongo import PymongoBaseModel
from arbm_core.private import Session
from arbm_core.private.relationships import LinkedinInvestorActivityAssociation


# class DataPoints(BaseModel):
#     # store classname and id of the object which is the source of the signal
#     # to allow for dynamic relationships
#     source_object_cls: str
#     source_object_id: int

#     def get_source(self):
#         orm_cls_ = getattr(__name__, self.source_object_cls)

#         s = object_session(self)
#         return s.get(orm_cls_, self.source_object_id)


class YearMonth(BaseModel):
    year: int
    month: conint(ge=1, le=12)


class BaseSignal(PymongoBaseModel):
    project_uuid: UUID
    fund_uuid: UUID
    timeframe: YearMonth


class SignalStored(BaseSignal):
    # todo: add mongo id mapping
    source_ids: list[Any]

    date_inserted: datetime.datetime
    date_updated: datetime.datetime


class AddSignal(BaseSignal):
    source: Any  # todo: specify type or a schema


def save_source(db: Database, source_signal):
    # don't use the date to find signal
    signal_identity = source_signal.dict(exclude={'picked_up_date'})

    logger.info(f'new source: {pformat(signal_identity)}')

    if (existing_source := db.sources.find_one(signal_identity)):
        logger.info(f'Source already exists: {existing_source}')
        return existing_source['_id']

    # store the date if doesn't exist
    return db.sources.insert_one(source_signal.dict()).inserted_id


def save_signal(db: Database, new_signal: AddSignal) -> SignalStored:
    # do not include any extra information in query
    query = new_signal.dict(include={'fund_uuid', 'project_uuid', 'timeframe'})

    # update the signal source
    new_source_id = save_source(db, new_signal.source.signal)

    # update signal between fund and project, if exists, upsert otherwise
    result = db.signals.update_one(filter=query, update={'$addToSet': {'source_ids': new_source_id},
                                                         '$set': {'date_updated': datetime.datetime.now(pytz.UTC)},
                                                         '$setOnInsert': {
                                                             'date_inserted': datetime.datetime.now(pytz.UTC)}
                                                         }, upsert=True)

    # todo: update inserted date for signals without one
    if result.matched_count:
        logger.info(f'Updated signal: {pformat(result.raw_result)}')

    return SignalStored(**db.signals.find_one(filter=query))


PROJECT_AGG = [
          {'$project':
            {'timeframe.id': 0,
             'sources.linkedin_details': 0}
          },
          {'$lookup': {
                'from': 'sources',
                'localField':'source_ids',
                'foreignField': '_id',
                'as':'sources'
          }},
          {
              '$unwind': '$sources'
          },
          { '$group': { '_id': {
                            'timeframe': '$timeframe',
                            'fund_uuid': '$fund_uuid'
                        },
                       'total': {'$count': {}},
                       'sources': {'$push': '$sources'}
                       },
          }
        #   ,
        #   {'$group': { '_id': '$_id.timeframe',
        #          #    'year': {'$first': '$_id.timeframe.year'},
        #          #    'total': {'$sum': '$total'},
        #                'signals': {
        #                    '$push': {
        #                         'fund_uuid': '$_id.fund_uuid',
        #                         'count': '$total',
        #                     }
        #                 },
        #                'total': {'$sum': '$total'}
        #              }
        #   },
        #   {'$sort': {'_id': 1}},
        #   {'$group': { '_id': '$_id.year',
        #                'year': {'$first': '$_id.year'},
        #                'total': {'$sum': '$total'},
        #                'months': { '$push': {
        #                     'month': '$_id.month',
        #                     'signals': '$signals',
        #                     'total': {'$sum': '$total'}
        #                    }
        #                 },
        #                'total': {'$sum': '$total'}
        #              }
        #   },
        #   {'$sort': {'_id': 1}},
        #   {'$unset': ['_id']}
    ]



def get_signals_for_fund(db, fund_uuid: UUID, cutoff: datetime.datetime):
    agg = [
          # remove irrelevant fields
          {'$project':
            {'timeframe.id': 0,
             'sources.linkedin_details': 0}
          },
          # create a record for each source
          {
              '$unwind': '$source_ids'
          },
          # group by timeframe and project and count records
          { '$group': { '_id': {
                            'timeframe': '$timeframe',
                            'project_uuid': '$project_uuid'
                        },
                       'total': {'$count': {}},
                       'date_inserted': {'$first': '$date_inserted'},
                   #   'sources': {'$push': '$sources'}
                       },
          },
          {'$sort': {'date_inserted': 1}},
          {'$group': { '_id': '$_id.timeframe',
                       'signals': {
                           '$push': {'project_uuid': '$_id.project_uuid'}
                        },
                        'total': {'$count': {}}
                     }
          },
          {'$sort': {'_id': 1}},
          {'$group': { '_id': '$_id.year',
                       'year': {'$first': '$_id.year'},
                       'months': { '$push': {
                            'month': '$_id.month',
                            'signals': '$signals',
                            'total': {'$size': '$signals'}
                           }
                        },
                        'total': {'$sum': '$total'}
                     }
          },
          {'$sort': {'_id': 1}},
          {'$unset': ['_id']}
    ]

    # filter signals for fund
    match_stage: dict = {'fund_uuid': fund_uuid}
    if cutoff:
        match_stage['date_inserted'] = {'$gte': cutoff}

    pipeline = [
        {'$match': match_stage},
        *agg
    ]

    signals = db.signals.aggregate(pipeline)

    return list(signals)


def get_unique_signals_for_fund(db, fund_uuid: UUID):
    FUND_AGG_UNIQUE = [
          # remove irrelevant fields
          {'$project':
            {'timeframe.id': 0,
             'sources.linkedin_details': 0}
          },
          # group by timeframe and project
          { '$group': {
              '_id': {
                    'timeframe': '$timeframe',
                    'project_uuid': '$project_uuid'
                },
                'date_inserted': {'$first': '$date_inserted'},
            },
          },
          # sort by timeframe and date inserted
          {'$sort': {'_id.timeframe': 1, 'date_inserted': 1}},
          {'$group': { '_id': '$_id.project_uuid'}},
          {'$sort': {'_id': 1}},
    ]

    signals = db.signals.aggregate([
        # filter signals for fund
        {'$match': {'fund_uuid': fund_uuid}},
        *FUND_AGG_UNIQUE
    ])

    return list(signals)


def get_signals_multiple_funds(db, fund_uuids: list[UUID]):
    FUND_AGG_UNIQUE = [
          # remove irrelevant fields
          {'$project':
            {'timeframe.id': 0,
             'sources.linkedin_details': 0}
          },
          # group by timeframe and project, keeping funds information
          { '$group': {
              '_id': {
                    'project_uuid': '$project_uuid'
                },
                'date_inserted': {'$min': '$date_inserted'},
                'funds': {'$addToSet': '$fund_uuid'}
            },
          },
          # sort by timeframe and date inserted
          {'$sort': {'date_inserted': -1}},
          {'$project':
            {
            '_id': 0,
             'project_uuid': '$_id.project_uuid',
             'funds': 1,
            }
          },
    ]

    signals = db.signals.aggregate([
        # filter signals for fund
        {'$match': {'fund_uuid': {'$in': fund_uuids}}},
        *FUND_AGG_UNIQUE
    ])

    return list(signals)


def get_signals_for_project(db, project_uuid: UUID):
    signals = list(db.signals.aggregate([
        # filter signals for project
        {'$match': {'project_uuid': project_uuid}},
        *PROJECT_AGG
    ]))

    for signal in signals:
        direct, indirect = 0, 0

        with Session() as s:
            for source in signal['sources']:
                # print(source)
                interaction: LinkedinInvestorActivityAssociation = s.get(LinkedinInvestorActivityAssociation,
                                                                         {'linkedin_post_id': source['post_id'],
                                                                          'investor_id': source['investing_entity'].get('id')})

                if not interaction:
                    logger.error(f'No linkedin_investor_activity_association found for post_id {source["post_id"]} and investor_id {source["investor_id"]}')
                    indirect += 1
                    continue

                activity = interaction.activity_type
                # print(activity)

                if activity.lower() in ['post interaction', 'like by investor', 'comment by investor', 'post by investor',
                                        'post', ] or 'likes this' in activity.lower():
                    direct += 1
                else:
                    indirect += 1

                # todo: match activity_type to inderct/direct

        if not (direct + indirect) == len(signal['sources']):
            logger.critical(f'Something went wrong with the signal for project {project_uuid}')

        signal['count_direct'] = direct
        signal['count_indirect'] = indirect
        del signal['sources']

    return list(signals)


PROJECT_AGG_UNIQUE = [
          {'$project':
            {'timeframe.id': 0,
             'sources.linkedin_details': 0}
          },
          { '$group': { '_id': {
                            'timeframe': '$timeframe',
                             'fund_uuid': '$fund_uuid'
                        },
                       },
          },
          {'$sort': {'_id.timeframe': 1}},
          {'$group': { '_id': '$_id.fund_uuid'}},
          {'$sort': {'_id': 1}},
    ]


def get_unique_signals_for_project(db, project_uuid: UUID):
    signals = db.signals.aggregate([
        # filter signals for project
        {'$match': {'project_uuid': project_uuid}},
        *PROJECT_AGG_UNIQUE
    ])

    return list(signals)


if __name__ == "__main__":
    # print(pformat(get_signals_for_fund(MongoDb, UUID("2ad567c2-62b3-473e-a63c-0b13d388525f"))))
    # print(pformat(get_unique_signals_for_project(MongoDb, UUID("e8974069-57b9-4270-b7b7-8882338fe3a3"))))
    print(pformat(get_signals_for_project(MongoDb, UUID("42f15019-8a0d-4d00-b1c5-64c688339a7f"))))