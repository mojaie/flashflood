#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import multiprocessing as mp
import os
import pickle


def session_auth(method):
    """ Auth without redirect (for async request)"""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.current_user:
            def reject(self, *args, **kwargs):
                self.write({"authenticated": False})
            return reject(self, *args, **kwargs)
        return method(self, *args, **kwargs)
    return wrapper


class LoginHandler(BaseHandler):
    def post(self):
        user = self.get_argument("name")
        pw = self.get_argument("pass")
        if user in config["user"] and pw == config["user"][user]["password"]:
            self.set_secure_cookie("user", user)
            self.write({"authenticated": True})
        else:
            self.write({"authenticated": False})


class FileBasedIPC(object):
    """ Windows compatible file-based IPC
    """
    def __init__(self, func):
        self.func = func

    def _f(self, *args, **kwargs):
        tmp_name = "_tmp.pickle"
        argsl = list(args)
        argsl.append(tmp_name)
        proc = mp.Process(target=self._p, args=argsl, kwargs=kwargs)
        proc.start()
        proc.join()
        with open(tmp_name, "rb") as f:
            result = pickle.load(f)
        os.remove(tmp_name)
        return result

    def _p(self, *args, **kwargs):
        al = list(args)
        n = al.pop()
        res = self.func(*al, **kwargs)
        with open(n, "wb") as f:
            pickle.dump(res, f)

    def __call__(self):
        return self._f
