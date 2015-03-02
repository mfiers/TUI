
from datetime import datetime
import collections
import hashlib
import logging
from multiprocessing.dummy import Pool
from queue import Queue, Empty
import os
import subprocess as sp
import time

from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

import leip
import tui.db
from tui.db import TuiFile

lg = logging.getLogger(__name__)
lg.setLevel(logging.DEBUG)

COUNTS = collections.Counter()
READY = Queue()


def get_sha1sum(tuifile):

    global COUNTS

    P = sp.Popen(['sha1sum', tuifile.fullpath], stdout=sp.PIPE)
    out, _ = P.communicate()
    out = out.strip()

    if not out:
        COUNTS['fail'] += 1
        return

    COUNTS['sha1'] += 1

    sha1 = out.split()[0]

    if len(sha1) == 40:
        tuifile.sha1sum = sha1
        READY.put(tuifile)
    else:
        COUNTS['fail'] += 1


def flush_queue(session):
    global READY
    stored = 0
    try:
        while True:
            tuifile = READY.get(False)
            stored += 1
            session.add(tuifile)
    except Empty:
        pass
    if stored > 0:
        session.commit()


def statusline(runtime, cnts):
    print("{:.2f}:q{}".format(runtime, READY.qsize()),
          " ".join(["{}:{}".format(k, v) for k, v in cnts.items()]),
          end="<<    \r")


@leip.arg('-m', '--min-file-size', default=100, type=int)
@leip.arg('-j', '--threads', default=4, type=int)
@leip.flag('-d', '--scan_dotfiles')
@leip.flag('-D', '--scan_dotdirs')
@leip.command
def scan(app, args):

    start = time.time()
    laststatus = time.time()
    global COUNTS

    curwd = os.path.realpath(os.path.abspath(os.getcwd()))
    session = tui.db.get_session(app)

    pool = Pool(processes=args.threads)
    futures = []

    for dirpath, dirs, files in os.walk(curwd):

        # print progress
        COUNTS['dir'] += 1
        statusline(time.time()-start, COUNTS)

        if not args.scan_dotdirs:
            dirs[:] = [d for d in dirs if not d.startswith('.')]
        if not args.scan_dotfiles:
            files[:] = [f for f in files if not f.startswith('.')]

        for f in files:

            COUNTS['file'] += 1

            if time.time() - laststatus > 1:
                statusline(time.time()-start, COUNTS)
                laststatus = time.time()

            fullpath = os.path.realpath(os.path.join(dirpath, f))
            try:
                stats = os.lstat(fullpath)
            except FileNotFoundError:
                #file not found? broken symlink??
                COUNTS['notfound'] += 1
                continue

            fsize = stats.st_size
            mtime = datetime.utcfromtimestamp(stats.st_mtime)

            if fsize < args.min_file_size:
                COUNTS['skip'] += 1

            # check for an object
            result = session.query(TuiFile)
            result = result.filter_by(fullpath=fullpath)

            try:
                tuifile = result.one()
                if tuifile.filesize == fsize and tuifile.mtime == mtime:
                    # all is well - continue
                    COUNTS['ok'] += 1

                    continue
                # filesize or mtime mismatch -> recalc shasum
                COUNTS['rm'] += 1
                session.delete(tuifile)
                session.commit()
            except NoResultFound:
                # ok - nothing found
                pass

            tuifile = TuiFile(fullpath=fullpath, filesize=fsize, mtime=mtime)
            pool.apply_async(get_sha1sum, (tuifile,))

            flush_queue(session)

    pool.close()
    pool.join()
    flush_queue(session)
    statusline(time.time()-start, COUNTS)
    print()
