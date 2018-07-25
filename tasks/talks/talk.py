import luigi

from . import fetch_talk_api
from . import fetch_talk_youtube_api


class Talk(luigi.Task):
    """ Complete wrapper task executes all tasks and subtasks for the given
    talk url.
    """
    youtube_url = luigi.Parameter(default="")

    task_namespace = 'vtalks.talks'

    def requires(self):
        return [
            # Fetch talk from api data
            fetch_talk_api.FetchTalkAPIData(youtube_url=self.youtube_url),
            # Fetch talk from youtube api data
            fetch_talk_youtube_api.FetchTalkYoutubeAPIData(youtube_url=self.youtube_url),
        ]


if __name__ == "__main__":
    luigi.run()
