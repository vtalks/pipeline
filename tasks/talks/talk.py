import os
import logging
import json
from datetime import datetime

import requests
import luigi

from ..channels import fetch_channel_api
from ..channels import fetch_channel_youtube_api
from ..playlists import fetch_playlist_api
from ..playlists import fetch_playlist_youtube_api
from ..playlists import fetch_playlist_items_youtube_api
from . import fetch_talk_api
from . import fetch_talk_youtube_api

logger = logging.getLogger(__name__)


class Talk(luigi.Task):
    youtube_url = luigi.Parameter(default="")

    task_namespace = 'vtalks.talks'

    channel_code = ""

    playlist_code = ""

    log = None

    def output(self):
        channel_api_output_path = self._get_channel_api_output_path()
        playlist_api_output_path = self._get_playlist_api_output_path()
        return {
            'channel_api': luigi.LocalTarget(channel_api_output_path),
            'playlist_api': luigi.LocalTarget(playlist_api_output_path),
        }

    def requires(self):
        return {
            # Fetch talk from api data
            'talk_api': fetch_talk_api.FetchTalkAPIData(youtube_url=self.youtube_url),
            # Fetch talk from youtube api data
            'talk_youtube_api': fetch_talk_youtube_api.FetchTalkYoutubeAPIData(youtube_url=self.youtube_url),
        }

    def run(self):
        self.log = logging.getLogger('luigi-interface')
        self.log.propagate = 0

        talk_api_input = self.input()['talk_api']
        with talk_api_input.open('r') as f:
            api_json_data = json.load(f)

            if api_json_data['channel']:
                response_json = self._fetch_channel_data(api_json_data['channel'])
                channel_youtube_url = response_json['youtube_url']
                self.channel_code = response_json['code']

                with self.output()['channel_api'].open('w') as f:
                    f.write(json.dumps(response_json))

                yield fetch_channel_api.FetchChannelAPIData(youtube_url=channel_youtube_url)
                yield fetch_channel_youtube_api.FetchChannelYoutubeAPIData(youtube_url=channel_youtube_url)

            if api_json_data['playlist']:
                response_json = self._fetch_playlist_data(api_json_data['playlist'])
                playlist_youtube_url = response_json['youtube_url']
                self.playlist_code = response_json['code']

                with self.output()['playlist_api'].open('w') as f:
                    f.write(json.dumps(response_json))

                yield fetch_playlist_api.FetchPlaylistAPIData(youtube_url=playlist_youtube_url)
                yield fetch_playlist_youtube_api.FetchPlaylistYoutubeAPIData(youtube_url=playlist_youtube_url)
                yield fetch_playlist_items_youtube_api.FetchPlaylistItemsYoutubeAPIData(youtube_url=playlist_youtube_url)

    def complete(self):
        is_completed = True

        for require in self.requires().values():
            if not require.complete():
                is_completed = False

        for output in self.output().values():
            # Check if output path exists
            if not os.path.exists(output.path):
                is_complete = False

            # Check output_path modification date
            if self._is_outdated(output.path):
                is_complete = False

        is_complete = super(Talk, self).complete()

        return is_complete

    def _fetch_channel_data(self, channel_id):
        url = "http://web:8000/api/channel/{:d}/".format(channel_id)
        resp = requests.get(url)
        if resp.status_code != 200:
            raise Exception('Error fetching channel data %s : "%s"' % (channel_id, resp.status_code))
        response_json = resp.json()
        return response_json

    def _fetch_playlist_data(self, playlist_id):
        url = "http://web:8000/api/playlists/{:d}/".format(playlist_id)
        resp = requests.get(url)
        if resp.status_code != 200:
            raise Exception('Error fetching playlist data %s : "%s"' % (playlist_id, resp.status_code))
        response_json = resp.json()
        return response_json

    def _get_channel_api_output_path(self):
        output_path = "/opt/pipeline/data/vtalks/channels/{:s}.json".format(self.channel_code)
        return output_path

    def _get_playlist_api_output_path(self):
        output_path = "/opt/pipeline/data/vtalks/playlist/{:s}.json".format(self.playlist_code)
        return output_path

    def _is_outdated(self, path):
        """ Check if output modification date is older than a day
        """
        is_outdated = False
        if os.path.exists(path):
            timestamp = os.path.getmtime(path)
            updated = datetime.fromtimestamp(timestamp)
            d = datetime.now() - updated
            is_outdated = d.total_seconds() > 86400  # one day
        return is_outdated


if __name__ == "__main__":
    luigi.run()
