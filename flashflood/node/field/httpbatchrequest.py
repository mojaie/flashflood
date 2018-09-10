#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from tornado import httpclient

from flashflood import functional
from flashflood.core.node import AsyncNode


class AsyncHttpBatchRequest(AsyncNode):
    def __init__(self, key, url_key, response_parser=functional.identity,
                 fetch_interval=0.1, in_place=False,
                 response_failed=lambda: None, response_headers=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.key = key
        self.url_key = url_key
        self.parser = response_parser
        self.failed = response_failed
        self.headers = response_headers
        self.fetch_interval = fetch_interval
        self.old_key = None
        if in_place and key != url_key:
            self.old_key = url_key

    def merge_fields(self):
        super().merge_fields()
        if self.old_key is not None:
            self._out_edge.fields.delete("key", self.old_key)

    @gen.coroutine
    def async_loop(self, edge):
        while 1:
            in_ = yield edge.get()
            yield self._out_edge.put(self.process_record(in_))
            yield gen.sleep(self.fetch_interval)

    @gen.coroutine
    def asynchronizer(self, edge, on_finish, on_abort):
        if self.edge_type(edge) == "IterEdge":
            rcds = edge.records
        elif self.edge_type(edge) == "FuncEdge":
            rcds = map(edge.func, edge.records)
        for in_ in rcds:
            if self._interrupted:
                yield self._out_edge.abort()
                on_abort()
                return
            yield self._out_edge.put(self.process_record(in_))
            yield gen.sleep(self.fetch_interval)
        yield self._out_edge.done()
        on_finish()

    def process_record(self, rcd):
        request = httpclient.HTTPRequest(rcd[self.url_key])
        request.headers = self.headers
        try:
            res = self.http_client.fetch(request)
            rcd = self.parser(res.body.decode("utf-8"))
        except httpclient.HTTPError:
            rcd = self.failed()
        return rcd

    def on_start(self):
        self.http_client = httpclient.HTTPClient()

    def on_finish(self):
        self.http_client.close()

    def on_abort(self):
        self.http_client.close()
