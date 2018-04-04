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
                 **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.response_parser = response_parser
        self.headers = headers or {}

    def run(self, on_finish, on_abort):
        http_client = httpclient.HTTPClient()
        request = httpclient.HTTPRequest(self.url)
        request.headers = self.headers
        response = http_client.fetch(request)
        http_client.close()
        res = self.response_parser(response.body.decode("utf-8"))
        self._out_edge.send(res)
        on_finish()
