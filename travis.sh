#!/bin/bash

set -e
dub test
dub build

pushd test
# checking that the perf suite can build
dub build -c unittest -b unittest

pushd standalone
# test building the standalone apps
dub build -c graspan1 -b release
popd

popd
