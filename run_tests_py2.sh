#!/usr/bin/env bash

CUR=$(cd -P $(dirname $0) && pwd -P)

. ${CUR}/run_tests_common.sh

python2 -m pytest ${CUR}/src/test/ -rs -v "$@"
