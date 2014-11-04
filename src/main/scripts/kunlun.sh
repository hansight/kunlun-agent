#!/bin/sh -
home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
startStop=$1
command=$2

. "$home/bin/func.sh"

case $command in
    forwarder)
	main="com.hansight.kunlun.collector.forwarder.Forwarder"
	;;
    agent)
	main="com.hansight.kunlun.collector.agent.Agent"
	;;
esac
case  $startStop in
    start)
	check
	start
	;;
    stop)
	stop
	;;
esac