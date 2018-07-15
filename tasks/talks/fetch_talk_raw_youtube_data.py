import os
import json
import logging
from datetime import datetime

import luigi

from youtube_data_api3 import video

logging.getLogger('luigi-interface').setLevel(level=logging.WARNING)


class FetchRawYoutubeData(luigi.Task):
    priority = 80

    youtube_url = luigi.Parameter(default="")
    youtube_playlist_url = luigi.Parameter(default="")

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

        return super(FetchRawYoutubeData, self).complete()

    def run(self):
        youtube_api_token = os.getenv("YOUTUBE_API_KEY")
        self.video_code = video.get_video_code(self.youtube_url)

        youtube_json_data = video.fetch_video_data(youtube_api_token, self.video_code)
        with self.output().open('w') as f:
            f.write(json.dumps(youtube_json_data))

    def _get_output_path(self):
        if self.youtube_url != "":
            self.video_code = video.get_video_code(self.youtube_url)

        output_path = "/opt/pipeline/data/youtube/talks/{:s}.json".format(self.video_code)

        return output_path

    def _is_outdated(self):
        """ Check if output modification date is older than a day
        """
        timestamp = os.path.getmtime(self.output().path)
        updated = datetime.fromtimestamp(timestamp)
        d = datetime.now() - updated
        is_outdated = d.total_seconds() > 86400    # one day
        return is_outdated


if __name__ == "__main__":
    luigi.run()
