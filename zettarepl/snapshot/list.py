# -*- coding=utf-8 -*-
import logging
import os

from zettarepl.transport.interface import Shell

logger = logging.getLogger(__name__)

__all__ = ["list_snapshots"]


def list_snapshots(shell: Shell, mountpoint: str) -> [str]:
    snapshots_path = os.path.join(mountpoint, ".zfs/snapshot")
    if not shell.exists(snapshots_path):
        return []
    return shell.ls(snapshots_path)
