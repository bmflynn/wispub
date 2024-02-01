#!/bin/bash
set -e
export VER=`git describe --dirty`
if [[ $VER == "" ]]; then
    VER="<notset>"
fi
export CGO_ENABLED=0
go build -a -ldflags "-s -w -X main.version=${VER} --extldflags '-static'"
