#!/bin/sh

cargo build
cd `dirname "$0"`
./bats/bats pijul.bats
./bats/bats record.bats
