#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 VisorCraft LLC
# SPDX-License-Identifier: Apache-2.0
#
# Tiny peak-RSS launcher. Runs argv[1:] and prints the child's peak resident
# set size (KiB) to stdout. The point is to fork the target from THIS small
# process so its ru_maxrss isn't inflated by a large parent's copy-on-write
# pages (os.wait4 reports max(parent_rss_at_fork, child_peak)). Both sides of
# a comparison share the same ~Python-interpreter launcher floor, so the
# delta above the floor is the real data cost. Conservative for tiny native
# children (their true RSS sits below the floor).
import os
import sys

pid = os.fork()
if pid == 0:
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    try:
        os.execvp(sys.argv[1], sys.argv[1:])
    except OSError:
        os._exit(127)
_, _, ru = os.wait4(pid, 0)
print(ru.ru_maxrss)
