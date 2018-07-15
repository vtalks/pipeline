import json
import luigi

from ..channels import fetch_channel_raw_youtube_data
from ..talks import fetch_talk_raw_youtube_data

from youtube_data_api3 import channel


class Talk(luigi.Task):
    youtube_url = luigi.Parameter()
    task_namespace = 'vtalks.talks'

    def requires(self):
        return {
            "talk": fetch_talk_raw_youtube_data.FetchRawYoutubeData(self.youtube_url),
        }

    def run(self):
        # Fetch channel raw youtube data
        with self.input()['talk'].open() as f:
            playlist_data = json.loads(f.read())
            channel_code = playlist_data['snippet']['channelId']
            channel_youtube_url = channel.get_channel_youtube_url(channel_code)
            yield (fetch_channel_raw_youtube_data.FetchRawYoutubeData(channel_youtube_url))

    def output(self):
        pass


if __name__ == "__main__":
    luigi.run()








