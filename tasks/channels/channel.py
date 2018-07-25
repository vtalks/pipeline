import logging

import luigi

from . import fetch_channel_api
from . import fetch_channel_youtube_api

logger = logging.getLogger(__name__)


class Channel(luigi.Task):
    youtube_url = luigi.Parameter(default="")

    task_namespace = 'vtalks.channels'

    def requires(self):
        return [
            # Fetch channel from api data
            fetch_channel_api.FetchChannelAPIData(youtube_url=self.youtube_url),
            # Fetch channel from youtube api data
            fetch_channel_youtube_api.FetchChannelYoutubeAPIData(youtube_url=self.youtube_url),
        ]

    def complete(self):
        is_completed = True

        for req in self.requires():
            if not req.complete():
                is_completed = False

        return is_completed


if __name__ == "__main__":
    luigi.run()
