import os
import json
import luigi

from youtube_data_api3 import channel


class FetchRawYoutubeData(luigi.Task):
    youtube_url = luigi.Parameter()
    channel_code = None
    task_namespace = 'vtalks.channels'

    def requires(self):
        return []

    def output(self):
        channel_code = channel.get_channel_code(self.youtube_url)

        output_path = "/pipeline/data/youtube/channels/{:s}.json"

        # Check output_path modification date
        if self._is_outdated(output_path):
            return False

        return luigi.LocalTarget(output_path.format(channel_code))

    def run(self):
        print("Executing task: {task}".format(task=self.__class__.__name__))

        youtube_api_token = os.getenv("YOUTUBE_API_KEY")
        channel_code = channel.get_channel_code(self.youtube_url)

        youtube_json_data = channel.fetch_channel_data(youtube_api_token, channel_code)
        with self.output().open('w') as f:
            f.write(json.dumps(youtube_json_data))

    def _is_outdated(self, output_path):
        timestamp = os.path.getmtime(output_path)
        print("Output modified {:d}".format(timestamp))
        return False


if __name__ == "__main__":
    luigi.run()
