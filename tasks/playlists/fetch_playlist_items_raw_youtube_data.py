import os
import json
from datetime import datetime

import luigi

from youtube_data_api3 import playlist


class FetchRawYoutubeData(luigi.Task):
    youtube_url = luigi.Parameter()
    task_namespace = 'vtalks.playlists.items'

    def requires(self):
        return []

    def output(self):
        output_path = self._get_output_path()
        return luigi.LocalTarget(output_path)

    def complete(self):
        is_complete = super(FetchRawYoutubeData, self).complete()
        if not is_complete:
            return False

        # Check output_path modification date
        if self._is_outdated():
            is_complete = False

        return is_complete

    def run(self):
        youtube_api_token = os.getenv("YOUTUBE_API_KEY")
        playlist_code = playlist.get_playlist_code(self.youtube_url)

        youtube_json_data = playlist.fetch_playlist_items(youtube_api_token, playlist_code)
        with self.output().open('w') as f:
            f.write(json.dumps(youtube_json_data))

    def _get_output_path(self):
        playlist_code = playlist.get_playlist_code(self.youtube_url)
        return "/opt/pipeline/data/youtube/playlists_items/{:s}.json".format(playlist_code)

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
