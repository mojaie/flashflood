#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import time
import uuid

from kiwiii import worker as wk


def timestamp(data):
    now = time.time()
    start = time.mktime(time.strptime(data["startDate"], "%X %x %Z"))
    data["responseDate"] = time.strftime("%X %x %Z", time.localtime(now))
    data["execTime"] = round(now - start, 1)


class RowWiseWorker(wk.Worker):
    """ Multi-process worker for row by row task processing """
    def __init__(self, args, func, builder):
        super().__init__(args, func)
        self.builder = builder
        self.builder.data["searchDoneCount"] = 0

    def on_start(self):
        self.builder.data["status"] = "In progress"

    def on_task_done(self, res):
        self.builder.data["searchDoneCount"] += 1
        timestamp(self.builder.data)
        self.builder.on_task_done(res)

    def on_finish(self):
        self.builder.data["status"] = "Completed"
        timestamp(self.builder.data)
        self.builder.on_finish()

    def on_interrupted(self):
        self.builder.data["status"] = "Aborted"
        timestamp(self.builder.data)

    def interrupt(self):
        super().interrupt()
        self.builder.data["status"] = "Aborting"
        timestamp(self.builder.data)


def filter_cascade(row_procs, args_next):
    row = {}
    for func, arg in zip(row_procs, args_next):
        if not func(row, arg):
            return None
    return row


class TableBuilder(object):
    def __init__(self):
        id_ = str(uuid.uuid4())
        self.data = {
            "id": id_,
            "name": id_[:8],
            "columns": [],
            "records": []
        }
        self.args_generators = []
        self.row_processors = []
        self.on_task_done_cascade = []
        self.on_finish_cascade = []

    def add_filter(self, flt):
        self.args_generators.append(flt.args_generator())
        self.row_processors.append(flt.row_processor)
        if hasattr(flt, 'initialize'):
            flt.initialize(self.data)
        if hasattr(flt, 'on_task_done'):
            self.on_task_done_cascade.append(flt.on_task_done)
        if hasattr(flt, 'on_finish'):
            self.on_finish_cascade.append(flt.on_finish)

    def on_task_done(self, res):
        for otd in self.on_task_done_cascade:
            otd(res, self.data)

    def on_finish(self):
        for onf in self.on_finish_cascade:
            onf(self.data)

    def flush(self):
        start = time.time()
        func = functools.partial(filter_cascade, self.row_processors)
        args = zip(*self.args_generators)
        self.data["records"] = list(filter(None, map(func, args)))
        finish = time.time()
        self.data["responseDate"] = time.strftime("%X %x %Z",
                                                  time.localtime(finish))
        self.data["execTime"] = round(finish - start, 1)
        self.data["status"] = "Completed"
        self.data["recordCount"] = len(self.data["records"])
        return self.data

    def queue(self, handler):
        self.data["status"] = "Queued"
        self.data["recordCount"] = 0
        self.data["startDate"] = time.strftime("%X %x %Z")
        timestamp(self.data)
        handler.write(self.data)
        handler.store.register(self.data)
        func = functools.partial(filter_cascade, self.row_processors)
        args = zip(*self.args_generators)
        worker = RowWiseWorker(args, func, self)
        handler.wq.put(self.data["id"], worker)
