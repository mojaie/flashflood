#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time
import uuid
import warnings

from tornado import gen

from flashflood import debug


class InvalidOperationError(Exception):
    pass


class UnexpectedOperationWarning(Warning):
    pass


class Task(object):
    """Task interface

    Args:
        specs (TaskSpecs): task implementation object
        name (str): display name for verbose input
        verbose (bool): shows verbose output or not

    Attributes:
        specs (TaskSpecs): task implementation object
        name (str): display name for verbose input
        id (str): Task instance ID (UUID)
        status (str):
            ``ready``, ``running``, ``done``, ``aborted``,
            ``interrupted``: interrupted but the task is not yet aborted
            ``cancelled``: submitted but cancelled before start
        creation_time (time): creation time
        start_time (time): start time
        finish_time (time): finish time
        verbose (bool): shows verbose output or not
    """
    def __init__(self, specs, name=None, verbose=False):
        if not isinstance(specs, TaskSpecs):
            raise TypeError("Invalid task specs")
        self.specs = specs
        self.name = name or type(specs).__name__
        self.id = str(uuid.uuid4())
        self.status = None
        self.creation_time = time.time()
        self.start_time = None
        self.finish_time = None
        self.verbose = verbose
        if self.verbose:
            self.specs.verbose = True

    @gen.coroutine
    def run(self):
        """Run the task"""
        self.on_start()
        yield self.specs.run(self.on_finish, self.on_abort)

    @gen.coroutine
    def execute(self):
        """Submit and run shortcut"""
        self.on_submit()
        yield self.run()

    def on_submit(self):
        """Event when the task is put into the job queue"""
        if self.status is not None:
            raise InvalidOperationError("Already submitted")
        self.specs.on_submit()
        self.status = "ready"
        if self.verbose:
            print("Submitted task: {}".format(self.name))

    def on_start(self):
        """Event when the task run started"""
        if self.status != "ready":
            raise InvalidOperationError("Not ready")
        self.start_time = time.time()
        self.specs.on_start()
        self.status = "running"
        if self.verbose:
            print("Started task: {}".format(self.name))

    def on_finish(self):
        """Event when the task run finished"""
        self.finish_time = round(time.time() - self.start_time, 3)
        self.specs.on_finish()
        self.status = "done"
        if self.verbose:
            print("Finished task: {}".format(self.name))

    def interrupt(self):
        """Interrupt the task"""
        if self.status in ("done", "aborted"):
            warnings.warn("Already done", UnexpectedOperationWarning)
            return
        if self.status is None or self.status == "ready":
            raise InvalidOperationError("Not yet running")
        self.specs.interrupt()
        self.status = "interrupted"
        if self.verbose:
            print("Interrupted task: {}".format(self.name))

    def on_abort(self):
        """Task.interrupt callback"""
        self.finish_time = round(time.time() - self.start_time, 3)
        self.specs.on_abort()
        self.status = "aborted"
        if self.verbose:
            print("Aborted task: {}".format(self.name))

    def size(self):
        """Total size of objects which are bound to the task"""
        return debug.total_size(self)

    def execution_time(self):
        """Total execution time"""
        if self.start_time is None:
            return
        if self.finish_time is None:
            return round(time.time() - self.start_time, 3)
        return self.finish_time


class TaskSpecs(object):
    """Interface of Task implementation"""
    def run(self, on_finish, on_abort):
        """Implementation of Task.run

        Args:
            on_finish(callable): Task.on_finish callback
            on_abort(callable): Task.on_abort callback
        """
        raise NotImplementedError()

    def on_submit(self):
        """Implementation of Task.on_submit"""
        raise NotImplementedError()

    def on_start(self):
        """Implementation of Task.on_start"""
        raise NotImplementedError()

    def on_finish(self):
        """Implementation of Task.on_finish"""
        raise NotImplementedError()

    def interrupt(self):
        """Implementation of Task.interrupt"""
        raise NotImplementedError()

    def on_abort(self):
        """Implementation of Task.on_abort"""
        raise NotImplementedError()
