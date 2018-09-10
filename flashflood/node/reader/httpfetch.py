#
# (C) 2014-2018 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json

from tornado import httpclient

from flashflood.node.reader.readerbase import ReaderBase


class HttpFetchInput(ReaderBase):
    def __init__(self, url, headers=None, response_parser=json.loads,
                 response_failed=lambda: None,
                 **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.response_parser = response_parser
        self.failed = response_failed
        self.headers = headers or {}

    def run(self, on_finish, on_abort):
        http_client = httpclient.HTTPClient()
        request = httpclient.HTTPRequest(self.url)
        request.headers = self.headers
        try:
            res = http_client.fetch(request)
            rcds = self.response_parser(res.body.decode("utf-8"))
        except httpclient.HTTPError:
            rcds = self.failed()
        finally:
            http_client.close()
        self._out_edge.send(rcds)
        on_finish()
