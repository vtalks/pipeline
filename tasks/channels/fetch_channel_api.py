import os
import json
import logging
from datetime import datetime

import requests
import luigi

from youtube_data_api3 import channel

logger = logging.getLogger(__name__)


class FetchChannelAPIData(luigi.Task):
    youtube_url = luigi.Parameter(default="")

    channel_code = ""

    task_namespace = 'vtalks.channels'

    def requires(self):
        return []

    def output(self):
        output_path = self._get_output_path()
        return luigi.LocalTarget(output_path)

    def complete(self):
        # Check if output path exists
        if not os.path.exists(self.output().path):
            return False

        # Check output_path modification date
        if self._is_outdated():
            return False

        return super(FetchChannelAPIData, self).complete()

    def run(self):
        api_json_data = self._fetch_channel_data()
        with self.output().open('w') as f:
            f.write(json.dumps(api_json_data))

    def _get_output_path(self):
        self.channel_code = channel.get_channel_code(self.youtube_url)
        output_path = "/opt/pipeline/data/vtalks/channels/{:s}.json".format(self.channel_code)

        return output_path

    def _is_outdated(self):
        """ Check if output modification date is older than a day
        """
        timestamp = os.path.getmtime(self.output().path)
        updated = datetime.fromtimestamp(timestamp)
        d = datetime.now() - updated
        is_outdated = d.total_seconds() > 86400  # one day
        return is_outdated

    def _fetch_channel_data(self):
        channel_url = "http://web:8000/api/channel/"
        payload = {'code': self.channel_code,}
        resp = requests.get(channel_url, params=payload)
        if resp.status_code != 200:
            raise Exception('Error fetching channel data "%s"' % resp.status_code)
        response_json = resp.json()
        channel_data = None
        if response_json["count"] > 0:
            channel_data = response_json["results"][0]
        return channel_data


if __name__ == "__main__":
    luigi.run()
