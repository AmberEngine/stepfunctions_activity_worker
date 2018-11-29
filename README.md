# StepFunctions Activity Worker

A worker that listens to a StepFunctions activity and executes a provided function using the inputs from the activity task.

The StepFunctions Activity Worker encapsulates all the parts of communicating with the StepFunctions API so you don't have to worry about task heartbeats or maintaining task tokens and success/failure scenarios; all you have to worry about is executing the task.

### Installation

Install from [PyPI](https://pypi.org/project/stepfunctions-activity-worker/):

```
pip install stepfunctions_activity_worker
```


### Usage

```python
from stepfunctions_activity_worker import ActivityWorker


def my_task(**task_input):
    """Perform the task based on this task's input."""
    # Perform your task here! 
    return {"result": "done!"}


if __name__ == "__main__":
    activity_arn = "PLACE YOUR ACTIVITY ARN HERE"
    worker = ActiityWorker(activity_arn, my_task)
    worker.listen()
```

## Warning

The `ActivityWorker` class, if not provided with a `client` argument on instantiation, will create a *properly configured* client from your default session.

However, if you are providing an already instantiated `client` to the `ActivityWorker` class, *make sure it is proply configured to make StepFunctions API calls*!

The [`GetActivityTask` API call](https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetActivityTask.html) __blocks for 60 seconds__ which *matches* the [`botocore.config.Config` default `read_timeout`](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html). This means that if the API response for `GetActivityTask` is not punctual (which it often isn't) it will cause unnecessary retry-requests & eventually bubble up an HTTP exception.

```python
import boto3
import botocore
from stepfunctions_activity_worker import ActivityWorker

config = botocore.config.Config(
  read_timeout=70,
  # Insert other custom configuration here
)
stepfunctions = boto3.client('stepfunctions', config=config)

activity_worker = ActivityWorker(my_function, client=stepfunctions)
```
