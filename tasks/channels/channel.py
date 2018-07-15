import luigi

from ..channels import fetch_channel_raw_youtube_data


class Talk(luigi.WrapperTask):
    youtube_url = luigi.Parameter(default="")

    task_namespace = 'vtalks.channels'

    def requires(self):
        return (
            fetch_channel_raw_youtube_data.FetchRawYoutubeData(youtube_url=self.youtube_url),
        )


if __name__ == "__main__":
    luigi.run()
