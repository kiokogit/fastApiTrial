import json
from pathlib import Path
from pprint import pprint

import util
from api_external.session import ApiSession, HeaderAuth


class PBApiError(Exception):
    pass


class PBAgentRunningError(Exception):
    pass


class PhantomBusterApi:
    BASE_URL = "https://api.phantombuster.com/api/v2/{endpoint}"

    def __init__(self, api_key: str, timeout: int = 10):
        auth = HeaderAuth('X-Phantombuster-Key-1', api_key)
        self.session = ApiSession(self.BASE_URL, timeout, auth)
        self.headers = {"Accept": "application/json"}  # default agent headers
        self.post_headers = {"content-type": "application/json"}  # default agent headers

    def agent_launch(self, agent_id: int):
        endpoint = "agents/launch"
        payload = {"id": str(agent_id)}

        return self.session.request('post', endpoint, params=None, json=payload, headers=self.post_headers)

    def agent_fetch_output(self, agent_id: int):
        endpoint = "agents/fetch-output"
        params = {"id": agent_id}

        return self.session.request('get', endpoint, params=params, headers=self.headers)

    def agent_fetch(self, agent_id: int):
        endpoint = "agents/fetch"
        params = {"id": agent_id}

        return self.session.request('get', endpoint, params=params, headers=self.headers)

    def agent_download_output(self, agent_id: id, remote_filename: str, remote_ext: str, output_path: Path) -> Path:
        """
        :param agent_id: PB agent id
        :param remote_filename: agent output filename without extension
        :param remote_ext: agent output file extension
        :param output_path: where to save file
        :return:
        """
        agent_output = self.agent_fetch_output(agent_id).json()

        if agent_output['isAgentRunning'] or agent_output['status'] != 'finished':
            raise PBAgentRunningError("agent is still running or status != 'finished'")

        agent_state = self.agent_fetch(agent_id).json()

        orgs3folder, s3folder = agent_state["orgS3Folder"], agent_state["s3Folder"]
        s3_url = f"https://phantombuster.s3.amazonaws.com/{orgs3folder}/{s3folder}/{remote_filename}.{remote_ext}"

        res_file = util.download_file(file_url=s3_url, filepath=output_path,
                                      filename=f'{remote_filename}'
                                               f'{util.now_fmt("%Y-%m-%d %H:%M")}'
                                               f'.{remote_ext}')

        return res_file

    def agent_save(self, agent_id: id, agent_argument: dict):
        endpoint = "agents/save"
        payload = {
            "argument": agent_argument,
            "id": str(agent_id),
        }

        return self.session.request('post', endpoint, json=payload, headers=self.post_headers)

    def agent_update_arguments(self, agent_id: int, **kwargs):
        agent_argument = json.loads(self.agent_fetch(agent_id).json()["argument"])

        for k, arg in kwargs.items():
            if k not in agent_argument:
                raise PBApiError("parameter not found in agent_argument object")
            agent_argument[k] = arg

        return self.agent_save(agent_id, agent_argument)
