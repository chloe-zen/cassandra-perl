#!/bin/sh -x
#
# Run this by hand if cassandra.thrift has changed.
# You'll need the thrift binary installed on your system.
#

thrift --gen cpp:dense cassandra.thrift
