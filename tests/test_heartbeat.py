"""Tests for stepfunctions_activity_worker.heartbeat.

Since Heartbeat inherits from threading.Timer we're testing against the
functionality that we override/extending to it.
"""
from unittest import mock

import pytest

from stepfunctions_activity_worker.heartbeat import Heartbeat


@pytest.fixture
def heartbeat_args():
    """Return default kwargs for Heartbeat."""
    args = (4, mock.Mock())
    kwargs = {"args": ("foo", "bar"), "kwargs": {"hello": "world"}}
    return args, kwargs


@pytest.fixture(autouse=True)
def patch_threading_timer_inheritance():
    """Patch Heartbeat.start() & Heartbeat.cancel()."""
    Heartbeat.start = mock.Mock()
    Heartbeat.cancel = mock.Mock()


def test_Heartbeat_enter_calls_start(heartbeat_args):
    """Test Heartbeat calls .start() on enter."""
    args, kwargs = heartbeat_args
    heartbeat = Heartbeat(*args, **kwargs)

    with heartbeat:
        heartbeat.start.assert_called_once()


def test_Heartbeat_exit_calls_cancel(heartbeat_args):
    """Test Heartbeat calls .cancel() on exit."""
    args, kwargs = heartbeat_args
    heartbeat = Heartbeat(*args, **kwargs)

    with heartbeat:
        pass

    heartbeat.cancel.assert_called_once()


def test_Heartbeat_run_calls_function_until_finished(heartbeat_args):
    """Test Heartbeat calls passed function on run."""
    args, kwargs = heartbeat_args
    heartbeat = Heartbeat(*args, **kwargs)

    function = args[1]

    heartbeat.finished = mock.Mock()
    heartbeat.finished.is_set.side_effect = (False, False, False, True)

    heartbeat.run()

    calls = [mock.call(*kwargs["args"], **kwargs["kwargs"])] * 3
    function.assert_has_calls(calls)


def test_Heartbeat_run_calls_wait_until_finished(heartbeat_args):
    """Test Heartbeat calls wait with provided interval."""
    args, kwargs = heartbeat_args
    heartbeat = Heartbeat(*args, **kwargs)

    interval = args[0]

    heartbeat.finished = mock.Mock()
    heartbeat.finished.is_set.side_effect = (False, False, False, True)

    heartbeat.run()

    calls = [mock.call(interval)] * 3
    heartbeat.finished.wait.assert_has_calls(calls)
