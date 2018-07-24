import os
import json
from datetime import datetime

import requests
import luigi

from youtube_data_api3 import video


class FetchTalkAPIData(luigi.Task):
    priority = 80

    youtube_url = luigi.Parameter(default="")

    video_code = ""

    task_namespace = 'vtalks.talks'

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

        return super(FetchTalkAPIData, self).complete()

    def run(self):
        api_json_data = self._fetch_video_data()
        with self.output().open('w') as f:
            f.write(json.dumps(api_json_data))

    def _get_output_path(self):
        self.video_code = video.get_video_code(self.youtube_url)
        output_path = "/opt/pipeline/data/vtalks/talks/{:s}.json".format(self.video_code)

        return output_path

    def _is_outdated(self):
        """ Check if output modification date is older than a day
        """
        timestamp = os.path.getmtime(self.output().path)
        updated = datetime.fromtimestamp(timestamp)
        d = datetime.now() - updated
        is_outdated = d.total_seconds() > 86400  # one day
        return is_outdated

    def _fetch_video_data(self):
        talk_url = "http://web:8000/api/talk/"
        payload = {'code': self.video_code,}
        resp = requests.get(talk_url, params=payload)
        if resp.status_code != 200:
            raise Exception('Error fetching video data "%s"' % resp.status_code)
        response_json = resp.json()
        video_data = None
        if response_json["count"] > 0:
            video_data = response_json["results"][0]
        return video_data


if __name__ == "__main__":
    luigi.run()
