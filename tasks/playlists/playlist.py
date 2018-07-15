import luigi

from . import fetch_playlist_raw_youtube_data
from . import fetch_playlist_items_raw_youtube_data
from ..channels import fetch_channel_raw_youtube_data


class Playlist(luigi.WrapperTask):
    youtube_url = luigi.Parameter()

    task_namespace = 'vtalks.playlists'

    def requires(self):
        return [
            fetch_playlist_raw_youtube_data.FetchRawYoutubeData(youtube_url=self.youtube_url),
            fetch_playlist_items_raw_youtube_data.FetchRawYoutubeData(youtube_url=self.youtube_url),
            fetch_channel_raw_youtube_data.FetchRawYoutubeData(playlist_youtube_url=self.youtube_url),
        ]


if __name__ == "__main__":
    luigi.run()








