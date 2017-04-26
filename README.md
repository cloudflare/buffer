buffer
------

This package provides a circular buffer implementation backed by an MMAP'd
file.  It's purpose is to provide a simple interface for storing arbitrary
records backed by a file which can handle failures and is suitable for
concurrent access.

Specifically, this is a fork of ashishgandhi's buffer implementation which
suits our needs more effectively.  Namely, insertions into a full buffer do not
overwrite existing records.  Instead an error is returned to the caller forcing
a decision on how to handle.

At this time, Cloudflare provides *no guarantees* about the stability of this
pacakge.  Please use vendoring to maintain this dependency in your project.
