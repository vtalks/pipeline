import os
import json
from datetime import datetime

import luigi

from . import fetch_talk_raw_youtube_data
from youtube_data_api3 import video


class GenerateElasticSearchDocument(luigi.Task):
    priority = 80

    talk_code = luigi.Parameter(default="")

    task_namespace = 'vtalks.talks'

    def requires(self):
        return [
            fetch_talk_raw_youtube_data.FetchRawYoutubeData(youtube_url=video.get_video_youtube_url(self.talk_code)),
        ]

    def output(self):
        output_path = self._get_output_path()
        return luigi.LocalTarget(output_path)

    def complete(self):
        # Check if output path exists
        if not os.path.exists(self.output().path):
            return False

        # Check output_path modification date
        if self._is_outdated():
            return False

        return super(GenerateElasticSearchDocument, self).complete()

    def run(self):
        talk_youtube_json_path = "/opt/pipeline/data/youtube/talks/{:s}.json".format(self.talk_code)
        talk_youtube_json_data = None
        with open(talk_youtube_json_path, 'r') as f:
            talk_youtube_json_data = json.load(f)
            f.close()


        talk_elasticsearch_json_data = {
            "id": None,
            "tags": []
        }
        if talk_youtube_json_data['snippet']:
            snippet = talk_youtube_json_data['snippet']
            if "title" in snippet:
                talk_elasticsearch_json_data['title'] = snippet['title']
            if "description" in snippet:
                talk_elasticsearch_json_data['description'] = snippet['description']
            if "tags" in snippet:
                talk_elasticsearch_json_data['tags'] += snippet["tags"]
            if "publishedAt" in snippet:
                published_at = snippet["publishedAt"]
                epoch = datetime.utcfromtimestamp(0)
                datetime_published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%S.000Z")
                datetime_published_timestamp = (datetime_published_at - epoch).total_seconds() * 1000.0
                talk_elasticsearch_json_data['created'] = datetime_published_timestamp

        with self.output().open('w') as f:
            f.write(json.dumps(talk_elasticsearch_json_data))

    def _get_output_path(self):
        output_path = "/opt/pipeline/data/elasticsearch/talks/{:s}.json".format(self.talk_code)

        return output_path

    def _is_outdated(self):
        """ Check if output modification date is older than a day
        """
        timestamp = os.path.getmtime(self.output().path)
        updated = datetime.fromtimestamp(timestamp)
        d = datetime.now() - updated
        is_outdated = d.total_seconds() > 86400    # one day
        return is_outdated


if __name__ == "__main__":
    luigi.run()
