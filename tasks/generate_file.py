from worker import celery_app


class GenerateFileTask(celery_app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        pass

    def on_success(self, retval, task_id, args, kwargs):
        pass

    def run(self, process_id):
        pass