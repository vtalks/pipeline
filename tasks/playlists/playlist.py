import logging

import luigi

logger = logging.getLogger(__name__)


class Playlist(luigi.Task):
    priority = 90

    youtube_url = luigi.Parameter(default="")

    task_namespace = 'vtalks.playlists'

    def requires(self):
        return [
        ]

    def complete(self):
        is_complete = super(Playlist, self).complete()
        if not is_complete:
            return False

        return True


if __name__ == "__main__":
    luigi.run()
