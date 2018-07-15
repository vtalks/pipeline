import os
import json
from datetime import datetime

import luigi

from youtube_data_api3 import playlist
from youtube_data_api3 import video

from ..talks import fetch_talk_raw_youtube_data


class FetchRawYoutubeData(luigi.Task):
    priority = 90

    youtube_url = luigi.Parameter()

    playlist_code = ""

    task_namespace = 'vtalks.playlists.items'

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

        self.playlist_code = playlist.get_playlist_code(self.youtube_url)

        youtube_json_data = playlist.fetch_playlist_items(youtube_api_token, self.playlist_code)
        with self.output().open('w') as f:
            f.write(json.dumps(youtube_json_data))

        for talk_code in youtube_json_data:
            youtube_talk_url = video.get_video_youtube_url(talk_code)
            yield fetch_talk_raw_youtube_data.FetchRawYoutubeData(youtube_url=youtube_talk_url)

    def _get_output_path(self):
        if self.youtube_url != "":
            self.playlist_code = playlist.get_playlist_code(self.youtube_url)

        output_path = "/opt/pipeline/data/youtube/playlists_items/{:s}.json".format(self.playlist_code)

        return output_path

    def _is_outdated(self):
        """ Check if output modification date is older than a day
        """
        timestamp = os.path.getmtime(self.output().path)
        updated = datetime.fromtimestamp(timestamp)
        d = datetime.now() - updated
        is_outdated = d.total_seconds() > 86400  # one day
        return is_outdated

    if __name__ == "__main__":
        luigi.run()
