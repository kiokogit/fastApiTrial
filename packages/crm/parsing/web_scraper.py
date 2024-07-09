import asyncio
import datetime
import subprocess
import time

from loguru import logger

from arbm_core.private import Session
from arbm_core.private.projects import DetailsEntry, ProjectAnalytics, ProjectStatus, TrackedProject


async def parse_website(url: str, timeout=30):
    p = await asyncio.create_subprocess_exec(*['/usr/bin/node', '/home/alpha/website_parser/WebScrapper.js', url],
                         stdout=subprocess.PIPE)

    res = None

    try:
        retcode = await asyncio.wait_for(p.wait(), timeout)
    except asyncio.TimeoutError:
        p.terminate()
        return res

    if retcode == 0:
        out = await p.stdout.read()
        res = out.decode('utf-8')
    else:
        res = None

    try:
        p.terminate()
    except Exception:
        pass

    return res


def parse_websites(projects: list[TrackedProject]):
    running_procs = {}

    for p in projects:
        url = p.website
        running_procs[p.id] = dict(
            proc=subprocess.Popen(['/usr/bin/node',
                                           '/home/alpha/website_parser/WebScrapper.js',
                                           url],
                                           stdout=subprocess.PIPE),
            started=utc_now(),
            website=p.website
        )

    total = len(running_procs.keys())
    contents = {}
    while running_procs:
        retcode = None

        for project_id, proc_dict in running_procs.items():
            proc = proc_dict['proc']

            retcode = proc.poll()
            print(proc_dict['website'], f'({project_id}) = ', retcode)

            if retcode is not None:  # Process finished.
                # Here, `proc` has finished with return code `retcode`
                if retcode == 0:
                    out = proc.stdout.read()
                    print(out)
                    res = out.decode('utf-8')
                else:
                    """Error handling."""
                    contents[project_id] = 'retcode nonzero'
                    del running_procs[project_id]
                    break

                out = proc.stdout.read()
                print(out)

                contents[project_id] = res

                running_procs[project_id].terminate()
                del running_procs[project_id]
                break
            elif (utc_now() - proc_dict['started']).total_seconds() > 60:
                contents[project_id] = 'timed out'
                del running_procs[project_id]
                break
            else:  # No process is done, wait a bit and check again.
                time.sleep(2)
                print(f"projects processed: {len(contents.keys())} / {total}")

    return contents


if __name__ == '__main__':
    with Session() as s:
        q = s.query(TrackedProject).filter(TrackedProject.status == ProjectStatus.accepted,
                                             TrackedProject.website != None,
                                             TrackedProject.analytics.has(
                                                 ~ProjectAnalytics.details.any(DetailsEntry.type == 'website_content')
                                             ))
        count = q.count()
        print(f'found {count} projects without website content')

        res = q.limit(10).all()

        contents = parse_websites(res)

        for project_id, res in contents.items():
            print(project_id, 'result: ', res)
            analytics = s.get(ProjectAnalytics, project_id) or ProjectAnalytics(project_id=project_id)
            analytics.details.append(DetailsEntry(
                data_source='web_scrapper',
                type='website_content',
                value=res,
                effective_dates=f'({datetime.date.today().strftime("%Y-%m-%d")},)',
            ))
