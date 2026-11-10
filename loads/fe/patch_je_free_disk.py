#!/usr/bin/env python3
"""Lower bdb-je's je.freeDisk default inside the FE's bundled bdb-je jar.

Why this exists
---------------
Antithesis containers keep their files in a RAM disk (tmpfs) sized at half the
machine's 10 GB of memory, so every filesystem in the container reports about
4.9 GB. StarRocks' FE refuses to start when the filesystem holding its meta dir
has less usable space than bdb-je's `je.freeDisk` default of 5 GB:

    MetaHelper.checkMetaDir()
        long lowerFreeDiskSize = Long.parseLong(EnvironmentParams.FREE_DISK.getDefault());
        if (store.getUsableSpace() < lowerFreeDiskSize) throw new InvalidMetaDirException();

Every released branch (3.5, 4.0, 4.1) reads that library default directly, so no
FE config can lower it; only `main` made it the configurable `bdbje_free_disk_size`
(default still 5 GB). The run therefore aborts during setup, 200 MB short.

This script rewrites that one constant so the released FE behaves like `main`
configured with a smaller headroom. It changes the single CONSTANT_Long entry
holding 5368709120 in com/sleepycat/je/config/EnvironmentParams, which the class
uses only to build FREE_DISK's default, and refuses to run if it does not find
exactly one such entry.

The proper fix is to ask Antithesis to raise the machine's memory, which raises
the RAM disk above 5 GB; see antithesis/FINDINGS.md.
"""

import struct
import sys
import zipfile

CLASS = "com/sleepycat/je/config/EnvironmentParams.class"
OLD = 5 * 1024 * 1024 * 1024  # 5 GB, bdb-je's je.freeDisk default
NEW = 512 * 1024 * 1024  # 512 MB of headroom, as `main` allows via config


def patch(path: str) -> None:
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        if CLASS not in names:
            sys.exit(f"{path}: {CLASS} not found")
        items = [(z.getinfo(n), z.read(n)) for n in names]

    old = b"\x05" + struct.pack(">q", OLD)  # CONSTANT_Long tag, then the value
    new = b"\x05" + struct.pack(">q", NEW)
    out = []
    for info, data in items:
        if info.filename == CLASS:
            found = data.count(old)
            if found != 1:
                sys.exit(f"{CLASS}: expected 1 constant {OLD}, found {found}")
            data = data.replace(old, new)
        out.append((info, data))

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for info, data in out:
            z.writestr(info, data)
    print(f"patched {path}: je.freeDisk default {OLD} -> {NEW}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: patch_je_free_disk.py <bdb-je jar>")
    patch(sys.argv[1])
