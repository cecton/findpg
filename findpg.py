#!/usr/bin/env python2

from __future__ import print_function

import sys
import os
import io
import re
import errno
from select import select
import logging
import subprocess
from urlparse import urlparse
from urllib import unquote
import argparse
from itertools import starmap
import operator

if not hasattr(subprocess, 'DEVNULL'):
    subprocess.DEVNULL = io.open(os.devnull, 'wb')

__all__ = ['restore']


LOGGER = logging.getLogger(__name__)

def base_arguments(url, program): return reduce(operator.add,
    starmap(
        lambda arg, param, wrapper: ([arg, wrapper(param)] if param else []),
        [('-h', url.hostname, unquote), ('-p', url.port, str),
         ('-U', url.username, unquote), ('-W', url.password, unquote)]),
    [os.path.join(unquote(url.path), program) if url.path else program])

def echo_url(url):
    return re.sub(
        r"^([a-z0-9]+://[^/:]+:)[^/@]+@", r"\1********@", url.geturl())

def restore(it, dbname, postgres_list, drop=False):
    """
    Restore a SQL dump read from an iterable (can be a fileobj) by creating the
    database dbname on each PostgreSQL instance provided by urls in
    postgres_list. Drop the database prior to the operation if drop is not
    falsy. URLs are in format
    postgres://[user[:password]@][host][:port][/bin_path]
    """

    # create databases on each connection
    for postgres in postgres_list:
        if drop:
            LOGGER.debug("Dropping database \"%s\" on %s"
                         % (dbname, echo_url(postgres)))
            subprocess.call(base_arguments(postgres, 'dropdb') +
                            ['-w', dbname],
                            stderr=subprocess.DEVNULL)
        LOGGER.debug("Creating database \"%s\" on %s"
                     % (dbname, echo_url(postgres)))
        # NOTE: do not use check_call, it displays the full command parameters
        cmd = base_arguments(postgres, 'createdb') + ['-w', dbname]
        retcode = subprocess.call(cmd)
        if retcode:
            raise subprocess.CalledProcessError(retcode, cmd[:1])

    # run psql sub processes
    psql = []
    succeeded = None
    try:
        # open the psql pipes in the reverse order to avoid some bug that
        # make a psql unable to exit when its stdin is closed
        for postgres in reversed(postgres_list):
            LOGGER.debug("Starting psql on %s", echo_url(postgres))
            pipe = subprocess.Popen(
                base_arguments(postgres, 'psql') + \
                ['-wX', '-v', 'ON_ERROR_STOP=1', dbname],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            LOGGER.debug("psql pid=%d", pipe.pid)
            psql.insert(0, pipe)

        # execute line on each instance
        for line in it:
            for pipe, postgres in zip(psql, postgres_list):
                if pipe.poll() is not None:
                    continue

                try:
                    print(line, file=pipe.stdin)
                    pipe.stdin.flush()
                except IOError, exc:
                    if not exc.errno == errno.EPIPE:
                        raise

                while any(select([pipe.stderr], [], [], 0)):
                    errline = pipe.stderr.readline()
                    if not errline:
                        break
                    if errline.startswith("ERROR:"):
                        LOGGER.info("Does not import on %s (pipe pid=%d), "
                                    "reason is: %s"
                                    % (echo_url(postgres), pipe.pid,
                                       errline.rstrip()))
                        pipe.wait()
                        break

    finally:
        for pipe, postgres in zip(psql, postgres_list):
            # close pipes
            if pipe.poll() is None:
                LOGGER.debug("Closing pipe pid=%d", pipe.pid)
                pipe.stdin.close()
            retcode = pipe.wait()
            # clean-up failed restores
            if not retcode and not succeeded:
                succeeded = postgres
                LOGGER.info("Restoration succeeded on " + echo_url(postgres))
            else:
                LOGGER.debug("Dropping database \"%s\" on %s"
                             % (dbname, echo_url(postgres)))
                retcode = subprocess.call(
                    base_arguments(postgres, 'dropdb') + ['-w', dbname])
                if retcode:
                    LOGGER.error("Failed to drop database")

    if not succeeded:
        LOGGER.warn("Can not restore dump")

    return succeeded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbname', '-d', dest='dbname', required=True)
    parser.add_argument('--dump', default=sys.stdin,
                        type=argparse.FileType('r'))
    parser.add_argument('--clean', '-c', action='store_true', default=False,
        help="Clean (drop) database objects before recreating them.")
    parser.add_argument('--debug', action='store_true',
        default=bool(os.environ.get('DEBUG', False)))
    parser.add_argument('postgres', nargs='+',
        help="postgres://[username[:password]@][host][:port][/bin_path]")
    args = parser.parse_args()

    postgres_list = map(urlparse, args.postgres)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    postgres = restore(args.dump, args.dbname, postgres_list, drop=args.clean)

    if postgres:
        print("Dump restored on database \"%s\" on %s" \
              % (args.dbname, echo_url(postgres)))
    else:
        print("Failed to restore dump \"%s\" using:\n%s" \
              % (args.dbname, "\n".join(map(echo_url, postgres_list))))

if __name__ == '__main__':
    main()
