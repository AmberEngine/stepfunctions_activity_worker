"""Listen for a Activity task and then perform it."""
import json
import socket
import sys
import traceback

import boto3
import botocore.config

from .heartbeat import Heartbeat


class ActivityWorker:
    """Activity worker for a Stepfunctions task."""

    def __init__(self, activity_arn, activity_fxn, heartbeat_interval=4,
                 *, client=None, **kwargs):
        """Instantiate with the activity ARN to listen to and the callable."""
        self.activity_kwargs = {
            "activityArn": activity_arn,
            "workerName": kwargs.get("worker_name", socket.gethostname()),
        }
        self.activity_name = activity_arn.split(":")[-1]
        self.activity_fxn = activity_fxn
        self.heartbeat_interval = heartbeat_interval

        self.stepfunctions = client if client else self._default_config()

    @staticmethod
    def _default_config():
        """Return default stepfunctions client configuration.

        StepFunctions GetActivitiyTask opens connections for 60 seconds.
        botocores's default read timeout is 60 seconds.

        Occaisonally delays will cause the socket to close before the request
        completes, causing an exception to be raised.
        """
        config = botocore.config.Config(read_timeout=70)
        return boto3.client('stepfunctions', config=config)

    def __call__(self):
        """Listen for and run a StepFunctions activity task."""
        self.perform_task()

    def _send_heartbeat(self, task):
        self.stepfunctions.send_task_heartbeat(taskToken=task["taskToken"])
        print("♡")

    def perform_task(self):
        """Listen for and run a Stepfunctions activity task."""
        task = dict()
        while not task.get("taskToken"):
            print("Polling for an activity task...")
            task = self.stepfunctions.get_activity_task(**self.activity_kwargs)

        print("Recieved a task!")
        print(json.dumps(json.loads(task["input"]), indent=4, sort_keys=True))

        task_input = json.loads(task["input"])

        heartbeat = Heartbeat(self.heartbeat_interval, self._send_heartbeat,
                              args=(task,), kwargs=None)

        try:
            print(f"Performing {self.activity_name}")
            with heartbeat:
                results = self.activity_fxn(**task_input)
        except (Exception, KeyboardInterrupt) as error:
            *_, raw_traceback = sys.exc_info()
            formatted_traceback = traceback.format_tb(raw_traceback)

            print(f"{self.activity_name} failed!")
            self.stepfunctions.send_task_failure(
                taskToken=task["taskToken"],
                error=str(error)[:256],
                cause="\n".join(formatted_traceback),
            )

            raise

        # NOTE: The output formatting may change depending on your workflow
        output = {**task_input, self.activity_name: results}
        print(f"{self.activity_name} is completed!")
        print(json.dumps(output, indent=4, sort_keys=True))
        self.stepfunctions.send_task_success(taskToken=task["taskToken"],
                                             output=json.dumps(output))

    def listen(self):
        """Repeatedly listen & execute tasks associated with this activity."""
        print(f"Listening for {self.activity_name}...")
        try:
            while True:
                self()
        except (KeyboardInterrupt):
            print("\nStopping listener...")