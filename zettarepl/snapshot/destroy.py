# -*- coding=utf-8 -*-
import re
import logging

from zettarepl.transport.interface import ExecException, Shell
from zettarepl.utils.itertools import sortedgroupby

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["destroy_snapshots"]

# Delete at most this many snapshots at a time
# See https://ixsystems.atlassian.net/browse/NAS-119329
CHUNK_SIZE = 20


def destroy_snapshots(shell: Shell, snapshots: [Snapshot]):
    for dataset, snapshots in sortedgroupby(snapshots, lambda snapshot: snapshot.dataset):
        names = [snapshot.name for snapshot in snapshots]

        logger.info("On %r for dataset %r destroying snapshots %r", shell, dataset, names)

        chunk = set()
        while chunk or names:
            if not chunk:
                chunk.update(names[:CHUNK_SIZE])
                names = names[CHUNK_SIZE:]

            args = ["zfs", "destroy", f"{dataset}@" + ",".join(sorted(chunk))]
            try:
                shell.exec(args)
                chunk.clear()
            except ExecException as e:
                if m := re.search(r"cannot destroy snapshot .+?@(.+?): dataset is busy", e.stdout):
                    reason = "busy"
                    name = m.group(1)
                elif m := re.search(r"cannot destroy '.+?@(.+?)': snapshot has dependent clones", e.stdout):
                    reason = "cloned"
                    name = m.group(1)
                else:
                    raise

                logger.info("Snapshot %r on dataset %r is %s, skipping", name, dataset, reason)
                chunk.discard(name)
