import logging

import luigi

from . import fetch_playlist_raw_youtube_data
from . import fetch_playlist_items_raw_youtube_data
from ..channels import fetch_channel_raw_youtube_data

logger = logging.getLogger(__name__)
logging.getLogger(__name__).setLevel(level=logging.DEBUG)


class Playlist(luigi.WrapperTask):
    youtube_url = luigi.Parameter()

    task_namespace = 'vtalks.playlists'

    def requires(self):
        outputs = [
            fetch_playlist_raw_youtube_data.FetchRawYoutubeData(youtube_url=self.youtube_url),
            fetch_playlist_items_raw_youtube_data.FetchRawYoutubeData(youtube_url=self.youtube_url),
            fetch_channel_raw_youtube_data.FetchRawYoutubeData(playlist_youtube_url=self.youtube_url),
        ]
        return outputs


if __name__ == "__main__":

    luigi.run()








