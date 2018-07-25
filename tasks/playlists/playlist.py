import logging

import luigi

from . import fetch_playlist_api
from . import fetch_playlist_youtube_api
from . import fetch_playlist_items_youtube_api

logger = logging.getLogger(__name__)


class Playlist(luigi.Task):
    youtube_url = luigi.Parameter(default="")

    task_namespace = 'vtalks.playlists'

    def requires(self):
        return [
            # Fetch channel from api data
            fetch_playlist_api.FetchPlaylistAPIData(youtube_url=self.youtube_url),
            # Fetch playlist from youtube api data
            fetch_playlist_youtube_api.FetchPlaylistYoutubeAPIData(youtube_url=self.youtube_url),
            # Fetch playlist items from youtube api data
            fetch_playlist_items_youtube_api.FetchPlaylistItemsYoutubeAPIData(youtube_url=self.youtube_url),
        ]

    def complete(self):
        is_completed = True

        for req in self.requires():
            if not req.complete():
                is_completed = False

        return is_completed


if __name__ == "__main__":
    luigi.run()
