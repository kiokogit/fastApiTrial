from crm.schemas.parsing import LinkedinLikesSignalSchema
from parsing.content_queue import PersistentQueue
from projects.linkedin_utils import create_post_from_signal, post_add_investor
from projects.project_utils import MODULE_NAME, get_investor
from projects.schemas.signals import LinkedinPostSignal
from util import clean_url, log_event, utc_now


from arbm_core import private as back_db
from arbm_core.core.utils import get_one_or_create
from arbm_core.private.linkedin import LinkedinLike, LinkedinPersonal, LinkedinUrl
from arbm_core.private.logging import EventType

from loguru import logger
from sqlalchemy.exc import IntegrityError


def queue_linkedin_signals(linkedin_likes_signal: LinkedinLikesSignalSchema, queue_id: str):
    like_queue = PersistentQueue(queue_id, delete_on_pop=False)
    likes_queued = 0

    logger.debug(f'Got linkedin likes signal with {len(linkedin_likes_signal.leader_signals)} leader signals')

    with back_db.Session() as s:
        investor = get_investor(s, investor_id=linkedin_likes_signal.investor.id)

        post = create_post_from_signal(s, linkedin_likes_signal)
        post = post_add_investor(post, investor, activity_type=linkedin_likes_signal.activity_type)

        s.add(post)
        s.commit()
        s.refresh(post)

        logger.debug(f'Created or updated post {post}')

        for leader_signal in linkedin_likes_signal.leader_signals:
            # likes indicate interactions.
            # we don't parse projects from repeated founders,
            # but should record repeated likes to establish
            # connections with investors

            # if leader_signal.liker_url == linkedin_likes_signal.investor.:
                # continue

            # print('linkedin_signal.leader_url:', linkedin_signal.leader_url)
            leader_profile, _ = get_one_or_create(s,
                                LinkedinPersonal,
                                filter_expression=LinkedinPersonal.urls.any(
                                    LinkedinUrl.url == leader_signal.liker_url
                                ),
                                create_method_kwargs=dict(name=leader_signal.name)
                            )
            leader_profile.liked_posts.append(post)
            s.add(leader_profile)
            s.commit()

            logger.debug(f'leader loaded {leader_profile}')

            like, existing = get_one_or_create(s,
                                     LinkedinLike,
                                     profile_url=clean_url(leader_signal.liker_url),
                                     post_id=post.id,
                                     create_method_kwargs=dict(
                                        investor=investor,

                                        activity_type=linkedin_likes_signal.activity_type,

                                        liker_id=leader_signal.linkedin_id,
                                        liker_name=leader_signal.name,
                                        liker_keyword=leader_signal.keyword,
                                        liker_title=leader_signal.title,
                                        img_id=leader_signal.img_id,
                                     )
                    )

            logger.debug(f'like loaded {like} (existing {existing})')

            # logger.debug(pformat(leader_signal.dict()))
            # logger.debug(pformat(linkedin_likes_signal.dict()))

            try:
                post_like_signal = LinkedinPostSignal(
                    picked_up_date=utc_now(),
                    # estimated_date=today - timedelta(days=1),

                    investing_entity=linkedin_likes_signal.investor,

                    post_id=post.id,
                    leader_id = leader_profile.id,
                )
                like_queue.put(f"{like.profile_url};=;=;{like.post_id}",
                               priority=5,
                               data=post_like_signal.json())
                likes_queued += 1
            except IntegrityError as e:
                logger.error(f'unable to queue like {like}, aready queued?\n{e}')

        s.commit()

        logger.info(f'Queued {likes_queued} likes from post {post.post_url}')
        log_event(s,
            type=EventType.info,
            module=MODULE_NAME,
            event='SignalsQueued',
            message={
                'event': 'likes_queued',
                'queued_count': likes_queued
            }
        )

        return likes_queued
