# Crash-consistency testing for the 3FS distributed file system, inspired by
# CrashMonkey/ACE (bounded black-box crash testing, OSDI'18, Mohan et al.).
# The harness is the single-pass probe gate under crash/; the original
# two-pass record-replay structure has been removed.
#
# Background on the 3FS semantics this relies on:
#   - 3FS stores file data in chunk storage (CRAQ replication) and all
#     metadata, including file length, in FoundationDB.
#   - fsync() flushes buffered data across the chain AND pins the file
#     length into FDB; fdatasync() (by default) only does the former.
#   - Metadata operations (create/rename/unlink/...) are FoundationDB
#     transactions and are durable the moment the syscall returns.
#   - After a client dies, file lengths stay at the last successfully
#     committed sync; chunk data beyond that is unreachable but present.
