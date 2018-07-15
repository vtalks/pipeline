import os
import json
import logging
from datetime import datetime

import luigi

from youtube_data_api3 import channel
from youtube_data_api3 import playlist
from youtube_data_api3 import video

logging.getLogger('luigi-interface').setLevel(level=logging.WARNING)


class FetchRawYoutubeData(luigi.Task):
    priority = 100

    youtube_url = luigi.Parameter(default="")
    playlist_youtube_url = luigi.Parameter(default="")
    talk_youtube_url = luigi.Parameter(default="")

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

        return super(FetchRawYoutubeData, self).complete()

    def run(self):
        youtube_api_token = os.getenv("YOUTUBE_API_KEY")

        youtube_json_data = channel.fetch_channel_data(youtube_api_token, self.channel_code)
        with self.output().open('w') as f:
            f.write(json.dumps(youtube_json_data))

    def _get_output_path(self):
        if self.youtube_url != "":
            self.channel_code = channel.get_channel_code(self.youtube_url)

        if self.talk_youtube_url != "":
            video_code = video.get_video_code(self.talk_youtube_url)
            video_json_path = "/opt/pipeline/data/youtube/talks/{:s}.json".format(video_code)
            if os.path.exists(video_json_path):
                with open(video_json_path, 'r') as f:
                    video_json_data = json.load(f)
                    self.channel_code = video_json_data["snippet"]["channelId"]
                    f.close()

        if self.playlist_youtube_url != "":
            playlist_code = playlist.get_playlist_code(self.playlist_youtube_url)
            playlist_json_path = "/opt/pipeline/data/youtube/playlists/{:s}.json".format(playlist_code)
            if os.path.exists(playlist_json_path):
                with open(playlist_json_path, 'r') as f:
                    playlist_json_data = json.load(f)
                    self.channel_code = playlist_json_data["snippet"]["channelId"]
                    f.close()

        output_path = "/opt/pipeline/data/youtube/channels/{:s}.json".format(self.channel_code)

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
