"""Tests for stepfunctions_activity_worker.activity_worker."""
import itertools
import json
from unittest import mock

import boto3
import pytest

import stepfunctions_activity_worker.activity_worker  # for patching
from stepfunctions_activity_worker.activity_worker import ActivityWorker


@pytest.fixture(autouse=True)
def boto3_client(monkeypatch):
    """Mock away boto3.client()."""
    mock_client = mock.Mock()
    monkeypatch.setattr(boto3, "client", mock_client)
    return mock_client


@pytest.fixture(autouse=True)
def mock_Heartbeat(monkeypatch):
    """Mock away stepfunctions_activity_worker.heartbeat.Heartbeat."""
    mock_Heartbeat = mock.MagicMock(
        spec=stepfunctions_activity_worker.activity_worker.Heartbeat,
    )
    monkeypatch.setattr(
        stepfunctions_activity_worker.activity_worker,
        "Heartbeat",
        mock_Heartbeat,
    )

    return mock_Heartbeat


@pytest.fixture
def sfn_client():
    """Mock boto3 StepFunctions client."""
    mock_client = mock.Mock()
    return mock_client


@pytest.fixture
def activity_name():
    """Create an activity name."""
    return "TestActivity"


@pytest.fixture
def activity_worker_kwargs(sfn_client, activity_name):
    """Return default kwargs for ActivityWorker()."""
    return {
        "activity_arn": "arn:aws:states:us-east-1:123456789012"
                        f":activity:{activity_name}",
        "activity_fxn": mock.Mock(return_value={}),
        "client": sfn_client,
        "heartbeat_interval": 2,
    }


def test_activity_worker_default_client(activity_worker_kwargs, boto3_client):
    """Assert that ActivityWorker creates a configured default client.

    Incoming Command: Assert direct public side effects.
    """
    activity_worker_kwargs.pop("client")
    worker = ActivityWorker(**activity_worker_kwargs)

    assert worker.stepfunctions is boto3_client.return_value

    client_args, client_kwargs = boto3_client.call_args
    assert client_args == ("stepfunctions",)
    assert client_kwargs["config"].read_timeout >= 70


@pytest.mark.parametrize("tasks", [
    ({"taskToken": "abc123", "input": "{}"},),
    (
        {"taskToken": ""},
        {"taskToken": ""},
        {"taskToken": "def456", "input": "{}"},
    ),
])
def test_activity_worker_gets_tasks(activity_worker_kwargs, tasks):
    """Assert that ActivityWorker.__call__ polls for a task.

    Outgoing Command: Assert sent message.
    """
    mock_client = activity_worker_kwargs["client"]
    mock_client.get_activity_task.side_effect = tasks

    worker = ActivityWorker(**activity_worker_kwargs)
    worker()

    assert len(mock_client.get_activity_task.mock_calls) == len(tasks)

    expected_call = mock.call(
        activityArn=activity_worker_kwargs["activity_arn"],
        workerName=mock.ANY,
    )
    for call in mock_client.get_activity_task.mock_calls:
        assert call == expected_call


@pytest.mark.parametrize("task", [
    {"foo": "bar"},
    {"foo": {"bar": "baz"}, "hello": ["world", "mark"]},
    {},
])
def test_activity_worker_calls_activity_fxn_w_task_input(
    activity_worker_kwargs,
    task,
):
    """Assert ActivityWorker.__call__ passes task input to activity_fxn.

    Outgoing Command: Assert sent message.
    """
    mock_client = activity_worker_kwargs["client"]
    mock_client.get_activity_task.return_value = {
        "taskToken": "abc123",
        "input": json.dumps(task),
    }

    activity_fxn = activity_worker_kwargs["activity_fxn"]

    worker = ActivityWorker(**activity_worker_kwargs)
    worker()

    activity_fxn.assert_called_once_with(**task)


@pytest.mark.parametrize("task_token", ["zzz999", "def456", "klaslkjfsa888kl"])
def test_activity_worker_sends_taskToken_on_success(activity_worker_kwargs,
                                                    task_token):
    """Assert ActivityWorker.__call__ sends taskToken on task success.

    Outgoing Command: Assert sent message.
    """
    mock_client = activity_worker_kwargs["client"]
    mock_client.get_activity_task.return_value = {"taskToken": task_token,
                                                  "input": "{}"}

    worker = ActivityWorker(**activity_worker_kwargs)
    worker()

    mock_client.send_task_success.assert_called_once_with(
        taskToken=task_token,
        output=mock.ANY,
    )


@pytest.mark.parametrize("fxn_output", [
    ({"foo": "bar"}),
    ({"foo": "blue", "hello": ["world", "mark"]}),
])
def test_activity_worker_puts_activity_fxn_return_into_output(
    activity_worker_kwargs, fxn_output,
):
    """Assert ActivityWorker.__call__ sends output on task success.

    Outgoing Command: Assert sent message.
    """
    mock_client = activity_worker_kwargs["client"]
    mock_client.get_activity_task.return_value = {
        "taskToken": "abc123",
        "input": "{}",
    }

    activity_fxn = activity_worker_kwargs["activity_fxn"]
    activity_fxn.return_value = fxn_output

    worker = ActivityWorker(**activity_worker_kwargs)
    worker()

    mock_client.send_task_success.assert_called_once_with(
        taskToken=mock.ANY,
        output=json.dumps(fxn_output, sort_keys=True),
    )


@pytest.mark.parametrize("exception_message", [
    "This is an error."
    f"This is a really{' really,' * 25} long error."
])
def test_activity_worker_sends_task_failure_when_exception_is_raised(
    activity_worker_kwargs,
    exception_message,
):
    """Assert ActivityWorker.__call__ sends task failure on exceptions.

    Outgoing Command: Assert sent message.
    """
    task_token = "abc123"
    mock_client = activity_worker_kwargs["client"]
    mock_client.get_activity_task.return_value = {
        "taskToken": task_token,
        "input": "{}",
    }

    activity_fxn = activity_worker_kwargs["activity_fxn"]
    activity_fxn.side_effect = Exception(exception_message)

    worker = ActivityWorker(**activity_worker_kwargs)

    with pytest.raises(Exception):
        worker()

    mock_client.send_task_failure(
        taskToken=task_token,
        error=exception_message[:256],
        cause=mock.ANY,
    )


def test_activity_worker_sends_task_heartbeat(
    activity_worker_kwargs,
    mock_Heartbeat,
):
    """Assert ActivityWorker.__call__ sends task heartbeat.

    Outgoing Command: Assert sent message.
    """
    task = {
        "taskToken": "abc123",
        "input": "{}",
    }

    mock_client = activity_worker_kwargs["client"]
    mock_client.get_activity_task.return_value = task

    worker = ActivityWorker(**activity_worker_kwargs)
    worker()

    mock_Heartbeat.assert_called_with(
        activity_worker_kwargs["heartbeat_interval"],
        mock_client.send_task_heartbeat,
        args=None,
        kwargs={"taskToken": task["taskToken"]},
    )

    heartbeat_instance = mock_Heartbeat.return_value

    heartbeat_instance.__enter__.assert_called_once()
    heartbeat_instance.__exit__.assert_called_once()


def test_activity_worker_listen_listens_until_keyboard_interrupt(
    activity_worker_kwargs,
):
    """Assert ActivityWorker.listen() runs tasks until KeyboardInterrupt.

    Incoming Command(?): Assert direct public side effects.
    """
    mock_client = activity_worker_kwargs["client"]
    infinite_tasks = itertools.repeat({"taskToken": "abc123", "input": "{}"})
    mock_client.get_activity_task.side_effect = infinite_tasks

    # There's no real way to insert a KeyboardInterrupt, but we can make it a
    # side effect of activity_fxn
    activity_fxn = activity_worker_kwargs["activity_fxn"]
    output = [dict()] * 438
    output.append(KeyboardInterrupt("This is a keyboard interrupt"))
    output.append(pytest.fail)  # fail on output after KeyboardInterrupt
    activity_fxn.side_effect = output

    worker = ActivityWorker(**activity_worker_kwargs)
    worker.listen()
