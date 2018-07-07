import luigi


class DummyTask(luigi.Task):

    def requires(self):
        pass

    def run(self):
        f = self.output().open('w')
        print >>f, "dummy task"
        f.close()

    def output(self):
        return luigi.localTarget('/usr/share/pipeline/data/dummy_task.txt' % self.params)


if __name__ == "__main__":
    luigi.run()
