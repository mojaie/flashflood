#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import base64
import functools

from flashflood import configparser as conf


def basic_auth(method):
    @functools.wraps(method)
    def wrapper(handler, *args, **kwargs):
        def reject(hdl):
            hdl.set_header(
                'WWW-Authenticate',
                'Basic realm={}'.format(conf.BASIC_AUTH_REALM)
            )
            hdl.set_status(401)

        auth = handler.request.headers.get('Authorization')
        if auth is None:
            return reject(handler)
        if not auth.startswith('Basic '):
            return reject(handler)
        auth_decoded = base64.b64decode(auth[6:]).decode('utf-8')
        user, passwd = auth_decoded.split(':')

        if conf.user_passwd_matched(user, passwd):
            return method(handler, *args, **kwargs)
        return reject(handler)
    return wrapper
