"""Module for executing a block of code at a regular interval."""
import threading


class Heartbeat(threading.Timer):
    """Execute a function at an interval until canceled."""

    def __enter__(self):
        """Start heartbeat on context management."""
        self.start()

    def __exit__(self, *args):
        """Terminate heartbeat when exiting context management."""
        self.cancel()

    def run(self):
        """Execute timer function until .cancel() is called."""
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            print("♡")
            self.finished.wait(self.interval)
