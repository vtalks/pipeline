import os
import json
import logging
from datetime import datetime

import requests
import luigi

from youtube_data_api3 import playlist

logger = logging.getLogger(__name__)


class FetchPlaylistAPIData(luigi.Task):
    youtube_url = luigi.Parameter(default="")

    playlist_code = ""

    task_namespace = 'vtalks.playlists'

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

        return super(FetchPlaylistAPIData, self).complete()

    def run(self):
        api_json_data = self._fetch_playlist_data()
        with self.output().open('w') as f:
            f.write(json.dumps(api_json_data))

    def _get_output_path(self):
        self.playlist_code = playlist.get_playlist_code(self.youtube_url)
        output_path = "/opt/pipeline/data/vtalks/playlists/{:s}.json".format(self.playlist_code)

        return output_path

    def _is_outdated(self):
        """ Check if output modification date is older than a day
        """
        timestamp = os.path.getmtime(self.output().path)
        updated = datetime.fromtimestamp(timestamp)
        d = datetime.now() - updated
        is_outdated = d.total_seconds() > 86400  # one day
        return is_outdated

    def _fetch_playlist_data(self):
        playlist_url = "http://web:8000/api/playlist/"
        payload = {'code': self.playlist_code,}
        resp = requests.get(playlist_url, params=payload)
        if resp.status_code != 200:
            raise Exception('Error fetching playlist data "%s"' % resp.status_code)
        response_json = resp.json()
        playlist_data = None
        if response_json["count"] > 0:
            playlist_data = response_json["results"][0]
        return playlist_data


if __name__ == "__main__":
    luigi.run()
