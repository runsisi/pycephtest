#!/usr/bin/env bash

CUR=$(cd -P $(dirname $0) && pwd -P)

ROOT=${CUR}
SRC=${ROOT}/src
TEST=${SRC}/test

export PYTHONPATH=${SRC}:${TEST}:${PYTHONPATH}
