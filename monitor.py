from contextlib import contextmanager
import luigi


class Monitor(object):
    def __init__(self, events):
        self.EVENTS = ["FAILURE", "SUCCESS"]
        self.event_status = dict()
        self.events = events
        for item in self.events:
            if item not in self.EVENTS:
                raise Exception("event parameters error")
        self.event_map = {
            "FAILURE": {"function": self.failure,
                        "handler": luigi.Event.FAILURE},
            "SUCCESS": {"function": self.success,
                        "handler": luigi.Event.SUCCESS}
        }

    def failure(self, task, exception):
        if not self.event_status.get("FAILURE", None):
            self.event_status["FAILURE"] = list()
        if self.event_status.get("SUCCESS", None):
            self.event_status["FAILURE"] += self.event_status["SUCCESS"]
        self.event_status["FAILURE"].append({"status": "failure", "task": str(task), "exception": str(exception)})

    def success(self, task):
        if not self.event_status.get("SUCCESS", None):
            self.event_status["SUCCESS"] = list()
        self.event_status["SUCCESS"].append({"status": "success", "task": str(task)})

    def send_message(self):
        print self.event_status

    def set_handler(self):
        for event in self.events:
            handler = self.event_map[event]['handler']
            function = self.event_map[event]['function']
            luigi.Task.event_handler(handler)(function)


@contextmanager
def monitor(events):
    if not isinstance(events, list):
        raise Exception("parameter events should be list type")
    m = Monitor(events)
    m.set_handler()
    yield m
    m.send_message()
