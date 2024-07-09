import datetime
from typing import Literal

from arbm_core.private import Session
from arbm_core.private.linkedin import LinkedinPost
from arbm_core.private.investors import Fund, Investor
from arbm_core.private.queue import QueuedItem
from loguru import logger
from sqlalchemy import delete, asc, desc, or_, update

from util import utc_now


class PersistentQueue:
    def __init__(self, queue_key: str, delete_on_pop=False, mode: Literal['fifo', 'lifo'] = 'fifo'):
        self.queue_key = queue_key
        self.delete_on_pop = delete_on_pop

        if mode not in ('fifo', 'lifo'):
            raise ValueError(f'invalid mode {mode}')

        self.mode = mode

    def get_query(self, s, n, max_priority: int | None, target):
        q = s.query(target)\
                .filter(QueuedItem.object_type==self.queue_key,
                        QueuedItem.popped==False) \

        if max_priority:
            q = q.filter(QueuedItem.priority <= max_priority)

        sort_order = None
        if self.mode == 'fifo':
            sort_order = asc
        elif self.mode == 'lifo':
            sort_order = desc
        else:
            raise ValueError(f'invalid mode {self.mode}')

        return q.order_by(asc(QueuedItem.priority),
                       sort_order(QueuedItem.time_queued)
            ).limit(min(n, 30))

    def peek(self, n, priority: int | None = None):
        with Session() as s:
            items = self.get_query(s, n, max_priority=priority, target=QueuedItem).all()
            return items

    def get(self, item_key):
        with Session() as s:
            item = s.get(QueuedItem, (self.queue_key, item_key))
            return item

    def _remove(self, s, keys):
        if self.delete_on_pop:
            stmt = delete(QueuedItem).where(QueuedItem.object_type==self.queue_key,
                                            QueuedItem.object_key.in_(keys))
        else:
            stmt = update(QueuedItem).where(QueuedItem.object_type==self.queue_key,
                                            QueuedItem.object_key.in_(keys)) \
                .values(popped=True, time_popped=utc_now())

        s.execute(stmt)
        s.commit()

    def remove(self, item_key):
        with Session() as s:
            self._remove(s, keys=[item_key])

    def pop(self, n, priority: int | None = None):
        with Session() as s:
            keys = self.get_query(s, n, max_priority=priority, target=QueuedItem.object_key).all()
            keys = [k[0] for k in keys]

            self._remove(s, keys)

            return keys

    def put(self, key: str, priority: int, data: str | None = None):
        with Session() as s:
            item = QueuedItem(object_type=self.queue_key,
                              object_key=key,
                              priority=priority,
                              data=data)
            s.add(item)
            s.commit()


def queue_investors(limit: int = 30, min_days_since_parsed: int = 14):
    with Session() as s:
        investors = s.query(Investor) \
            .filter(Investor.linkedin_url != None) \
            .filter(Investor.linkedin_url != '') \
            .filter(or_(Investor.linkedin_last_parsed == None,
                        Investor.linkedin_last_parsed
                            < utc_now() - datetime.timedelta(days=min_days_since_parsed)
                        )
                    ) \
            .filter(Investor.funds.any(Fund.enabled == True)) \
            .all()

        logger.debug(f'[queue_investors]: {len(investors)} investors matching filters')

        queued_investors = sorted(investors, key=lambda i: max([f.priority for f in i.funds]), reverse=True)

        logger.debug(f'[queue_investors]: {len(queued_investors)} investors added to queue:')
        logger.debug(', '.join([f'{i.name}' for i in queued_investors]))

        return queued_investors[:limit]


def get_priority_investors_from_funds(s):
    investors = s.query(Investor).filter(Investor.linkedin_url != None).all()
    selected_funds = s.query(Fund)\
        .filter(Fund.enabled == True)\
        .filter(Fund.priority > 1)\
        .order_by(desc(Fund.priority)).all()

    logger.debug('selected funds by priority:')
    logger.debug(', '.join([f'[{f.priority}] {f.name}' for f in selected_funds]))

    priority_investors = []
    for investor in investors:
        for f in investor.funds:
            if f in selected_funds and f.enabled:
                priority_investors.append(investor)

    return priority_investors


def get_valid_posts(s):
    posts = s.query(LinkedinPost).all()

    logger.debug(f"posts retrieved from queue: {len(posts)}")

    posts = s.query(LinkedinPost) \
        .filter(~LinkedinPost.likers.any()) \
        .filter(LinkedinPost.likers_parsed_date == None) \
        .filter(LinkedinPost.relative_post_date != None) \
        .filter(LinkedinPost.like_count > 200) \
        .filter(LinkedinPost.like_count < 500) \
        .order_by(LinkedinPost.parsed_date.desc()) \
        .order_by(LinkedinPost.like_count).all()

    logger.debug(f"posts retrieved from queue: {len(posts)}")
    posts = [p for p in posts if p.parse_relative_date() is not None and p.parse_relative_date() > utc_now() - datetime.timedelta(days=14)]
    posts = sorted(posts, key=lambda p: p.parse_relative_date(), reverse=True)

    logger.debug(f"posts filtered by date: {len(posts)}")
    return posts


def queue_next_posts(limit=30):
    with Session() as s:
        required = 10
        accepted = []

        priority_investors = get_priority_investors_from_funds(s)

        posts = get_valid_posts(s)
        for post in posts:
            for i in post.investor_interactions:
                if i in priority_investors:
                    if any([f.enabled for f in i.investor.funds]):
                        accepted.append(post)

        logger.debug(f"posts accepted by fund: {len(accepted)}")

        if len(accepted) < required:
            for post in posts:
                for i in post.investor_interactions:
                    if post not in accepted and any([f.enabled for f in i.investor.funds]):
                        accepted.append(post)
                if len(accepted) >= required:
                    break

        return accepted[:limit]

