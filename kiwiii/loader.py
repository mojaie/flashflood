
import glob
import os
import yaml


with open("server_config.yaml") as f:
    config = yaml.load(f.read())


def web_home_path():
    return config["web_home"]


def basic_auth_realm():
    return config["basic_auth_realm"]


def user_passwd_matched(user, passwd):
    users = config["user"]
    return user in users and passwd == users[user]['password']


def db_list():
    pathname = os.path.join(config.get("db_base_dir", ""), "**/*.sqlite3")
    ps = set(glob.glob(pathname, recursive=True))
    ps |= set(os.path.realpath(p) for p in config.get("db_include", []))
    ps -= set(os.path.realpath(p) for p in config.get("db_exclude", []))
    return list(ps)


def api_list():
    pathname = os.path.join(config.get("api_base_dir", ""), "**/*.yaml")
    ps = set(glob.glob(pathname, recursive=True))
    exc = config.get("api_exclude", []) + ["server_config.yaml"]
    ps |= set(os.path.realpath(p) for p in config.get("api_include", []))
    ps -= set(os.path.realpath(p) for p in exc)
    return list(ps)


def report_tmpl_list():
    pathname = os.path.join(config.get("report_template_dir", ""), "*.csv")
    return glob.glob(pathname)


def db(filename):
    # TODO: deprecated - db name should be given by resource scheme
    return "{}.sqlite3".format(filename)


def report_tmpl_file(filename):
    # TODO: deprecated - template path should be given by resource scheme
    return os.path.join(config.get("report_template_dir", ""), filename)


def screener_api():
    # TODO: deprecated - API URL should be given by resource scheme
    return config["screener_api"]
