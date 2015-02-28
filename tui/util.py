
import os


def get_dbpath(app):
    pth = app.conf.get('dbpath', '~/.local/db/tui')
    pth = os.expanduser(pth)
    if not os.path.exists(pth):
        os.makedirs(pth)
