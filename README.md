# cb2bleve

A utility which uses DCP to stream the contents of a bucket into a bleve index.  This is mostly useful for reproducing problems first observed in cbft in a pure bleve environment.

NOTE: currently it does not stop after doing a one-shot replication, instead you must wait for the number of updates to stop, then ctrl-C the program.