import json
import luigi

from . import fetch_playlist_raw_youtube_data
from . import fetch_playlist_items_raw_youtube_data
from ..channels import fetch_channel_raw_youtube_data
from ..videos import fetch_video_raw_youtube_data

from youtube_data_api3 import channel
from youtube_data_api3 import video


class Playlist(luigi.Task):
    youtube_url = luigi.Parameter()
    task_namespace = 'vtalks.playlists'

    def requires(self):
        return {
            "playlist": fetch_playlist_raw_youtube_data.FetchRawYoutubeData(self.youtube_url),
            "playlist_items": fetch_playlist_items_raw_youtube_data.FetchRawYoutubeData(self.youtube_url),
        }

    def run(self):
        # Fetch channel raw youtube data
        with self.input()['playlist'].open() as f:
            playlist_data = json.loads(f.read())
            channel_code = playlist_data['snippet']['channelId']
            channel_youtube_url = channel.get_channel_youtube_url(channel_code)
            yield(fetch_channel_raw_youtube_data.FetchRawYoutubeData(channel_youtube_url))

        # Fetch playlist videos raw youtube data
        with self.input()['playlist_items'].open() as f:
            playlist_items = json.loads(f.read())
            for video_code in playlist_items:
                video_youtube_url = video.get_video_youtube_url(video_code)
                yield(fetch_video_raw_youtube_data.FetchRawYoutubeData(video_youtube_url))


if __name__ == "__main__":
    luigi.run()








