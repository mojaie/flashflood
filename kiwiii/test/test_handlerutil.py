#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time
import unittest

from kiwiii import handlerutil


class TestHandlerUtil(unittest.TestCase):
    def test_temptbl(self):
        temp = handlerutil.TemporaryDataStore()
        data1 = {
            "id": "tempdata1",
            "startDate": "15:00:00 1/1/16 JST"
        }
        data2 = {
            "id": "tempdata2",
            "startDate": "16:00:00 1/1/16 JST"
        }
        data3 = {
            "id": "tempdata3",
            "startDate": "15:01:00 1/8/16 JST"
        }
        temp.max_age = 86400 * 7
        temp.container.append(data1)
        temp.container.append(data2)
        now = time.mktime(time.strptime("15:01:00 1/8/16 JST", "%X %x %Z"))
        temp.register(data3, now)
        self.assertEqual(len(temp.container), 2)


if __name__ == '__main__':
    unittest.main()
