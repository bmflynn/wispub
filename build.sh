#!/bin/bash
set -e
export VER=$(cat version.txt)
if [[ $VER == "" ]]; then
    VER="<notset>"
fi
export CGO_ENABLED=0
go build -a -ldflags "-s -w -X main.version=${VER} --extldflags '-static'"
