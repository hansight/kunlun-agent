#!/bin/sh -
home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
$home/bin/kunlun.sh stop agent
