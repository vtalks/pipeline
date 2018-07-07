import luigi


class DummyTask(luigi.Task):
    task_namespace = 'vtalks'

    def run(self):
        print("Executing task: {task}".format(task=self.__class__.__name__))


if __name__ == "__main__":
    luigi.run()
