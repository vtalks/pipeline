import logging

import luigi

from . import fetch_talk_api
from . import fetch_talk_youtube_api

logger = logging.getLogger(__name__)


class Talk(luigi.Task):
    priority = 80

    youtube_url = luigi.Parameter(default="")

    task_namespace = 'vtalks.talks'

    def requires(self):
        return [
            # Fetch talk from api data
            fetch_talk_api.FetchTalkAPIData(youtube_url=self.youtube_url),
            # Fetch talk from youtube api data
            fetch_talk_youtube_api.FetchTalkYoutubeAPIData(youtube_url=self.youtube_url),
        ]

    def complete(self):
        is_completed = super(Talk, self).complete()

        for req in self.requires():
            if not req.complete():
                is_completed = False

        return is_completed


if __name__ == "__main__":
    luigi.run()
