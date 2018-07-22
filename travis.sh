#!/bin/bash

set -e
dub test
dub build

pushd test
# checking that the perf suite can build
dub build -c unittest -b unittest
popd
