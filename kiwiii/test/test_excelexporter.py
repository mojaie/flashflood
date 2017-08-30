#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import unittest


def _method_ipc(a, b=None):
    return (a + b, os.getpid())


@unittest.skip('')
class TestMultiProcess(unittest.TestCase):
    # @multiprocess.queue
    def _method_queue(self, a, b=None):
        return (a + b, os.getpid())

    @unittest.skip("")
    def test_queue(self):
        parent = os.getpid()
        result, child = self._method_queue(1, 2)
        self.assertEqual(result, 3)
        self.assertNotEqual(parent, child)

    def test_ipc_run(self):
        parent = os.getpid()
        # result, child = multiprocess.ipc_run(_method_ipc)(1, 2)
        result, child = multiprocess.FileBasedIPC(_method_ipc)()(1, 2)
        self.assertEqual(result, 3)
        self.assertNotEqual(parent, child)

if __name__ == '__main__':
    """ for Windows compatibility """
    import multiprocessing as mp
    mp.set_start_method("spawn")
    unittest.main()
