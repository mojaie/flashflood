
import glob
import os
import yaml

from flashflood import sqliteconnection as sqlite
from flashflood import static
from flashflood.lod import ListOfDict


try:
    with open("server_config.yaml") as f:
        config = yaml.load(f.read())
except FileNotFoundError:
    """ use server_config stub"""
    config = {}


BASIC_AUTH_REALM = config.get("basic_auth_realm")
INSTANCE_PREFIX = config.get("server_instance_prefix")

SQLITE_BASE_DIR = config.get("sqlite_base_dir")
API_BASE_DIR = config.get("api_base_dir")
REPORT_TEMPLATE_DIR = config.get("report_template_dir")
WEB_BUILD = config.get("web_build")
WEB_DIST = config.get("web_dist")

EXTERNALS = config.get("externals", [])

COMPID_PLACEHOLDER = config.get("compound_id_placeholder", "")
USERS = config.get("user")


def user_passwd_matched(user, passwd):
    return user in USERS and passwd == USERS[user]["password"]


def schema():
    resources = ListOfDict()
    # SQLite databases
    for filename in glob.glob(
            os.path.join(SQLITE_BASE_DIR, "*.sqlite3")):
        conn = sqlite.Connection(filename)
        data = conn.document()
        for rsrc in data["resources"]:
            rsrc["domain"] = data["domain"]
            rsrc["resourceType"] = data["resourceType"]
            rsrc["resourceFile"] = data["resourceFile"]
            fields = ListOfDict(rsrc["fields"])
            if rsrc["domain"] == "chemical":
                fields.merge(static.CHEM_FIELDS)
                fields.delete("key", "_molobj")
            rsrc["fields"] = fields
        resources.extend(data["resources"])
    # API schema
    for filename in glob.glob(os.path.join(API_BASE_DIR, "*.yaml")):
        with open(filename) as f:
            doc = yaml.load(f.read())
            resources.append(doc)
    # Templates
    templates = ListOfDict()
    for tm in glob.glob(os.path.join(REPORT_TEMPLATE_DIR, "*.csv")):
        templates.append({
            "name": os.path.splitext(os.path.basename(tm))[0],
            "sourceFile": os.path.basename(tm)
        })
    return {
        "resources": resources,
        "templates": templates,
        "compoundIDPlaceholder": COMPID_PLACEHOLDER
    }
