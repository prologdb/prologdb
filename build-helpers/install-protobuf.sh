#!/bin/sh

# thanks to https://github.com/travis-ci/container-example/blob/master/install-protobuf.sh

set -e

if [ ! -d "$HOME/protobuf-3.6.1" ]; then
  wget https://github.com/protocolbuffers/protobuf/archive/v3.6.1.tar.gz
  tar -xzvf protobuf-3.6.1.tar.gz
  cd protobuf-3.6.1 && ./configure --prefix=$HOME/protobuf-3.6.1 && make && make install
else
  echo "Using cached protoc 3.6.1"
fi