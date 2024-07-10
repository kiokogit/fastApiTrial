from pprint import pprint
from uuid import UUID

from arbm_core.core import MongoDb
from arbm_core.core.signals import AddSignal, save_signal, YearMonth

from arbm_core.private import Session
from arbm_core.private.projects import TrackedProject

db = MongoDb

def test_add_signal():
    with Session() as s:
        p: TrackedProject = s.get(TrackedProject, 28930)

        new_signal = AddSignal(project_uuid=p.uuid,
                           fund_uuid=UUID('bebf92ae-baed-4837-b570-3de104d6103d'),
                           timeframe=YearMonth(year=2023, month=12),
                           source='test')

        query = new_signal.dict(include={'fund_uuid', 'project_uuid', 'timeframe'})

        # find signal between fund and project, if exists
        signal_in_db = db.signals.find(query) or {}
        print('signals in db:', len(list(signal_in_db)))

        stored = save_signal(db, new_signal)

        signal_in_db = db.signals.find(query) or {}
        print('signals in db:', len(list(signal_in_db)))

        pprint(stored)



def get_signals():
    with Session() as s:
        p: TrackedProject = s.get(TrackedProject, 28930)

        pprint(
            list(db.signals.find({'project_id': p.uuid}))
        )

if __name__ == '__main__':
    with Session() as s:
        p: TrackedProject = s.get(TrackedProject, 28930)

        # db.signals.delete_many({'id': UUID('61c621fc-714f-4b77-9fd9-46608759a2f6')})


    # get_signals()
    test_add_signal()