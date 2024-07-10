import subprocess
import time

from ..private.projects import TrackedProject


def parse_website(script_path: str, url: str):
    p = subprocess.Popen(['/usr/bin/node',
                          script_path,
                          url],
                         stdout=subprocess.PIPE)
    retcode = p.wait(30)
    if retcode != 0:
        return ''

    out = p.stdout.read()
    res = out.decode('utf-8')

    return res


def parse_websites(script_path: str, projects: list[TrackedProject]):
    running_procs = {}

    for p in projects:
        url = p.website
        running_procs[p.id] = subprocess.Popen(['/usr/bin/node',
                                               script_path,
                                               url],
                                               stdout=subprocess.PIPE)

    contents = {}
    while running_procs:
        retcode = None

        for project_id, proc in running_procs.items():
            retcode = proc.poll()
            print(project_id, retcode)

            if retcode is not None:  # Process finished.
                # Here, `proc` has finished with return code `retcode`
                if retcode != 0:
                    """Error handling."""
                out = proc.stdout.read()
                print(out)

                res = out.decode('utf-8')
                contents[project_id] = res

                del running_procs[project_id]
                break
            else:  # No process is done, wait a bit and check again.
                time.sleep(.1)

    return contents
