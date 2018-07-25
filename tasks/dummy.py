import logging

import luigi

logger = logging.getLogger(__name__)


class Dummy(luigi.Task):
    task_namespace = 'vtalks'

    def requires(self):
        return []

    def run(self):
        print("Executing task: {task}".format(task=self.__class__.__name__))


if __name__ == "__main__":
    luigi.run()
