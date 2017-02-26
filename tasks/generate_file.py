from worker import app


class GenerateFileTask(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        pass

    def on_success(self, retval, task_id, args, kwargs):
        pass

    def run(self, process_id):
        pass