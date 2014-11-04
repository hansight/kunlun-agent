#!/bin/sh -
function classpath() {
    arr=("$( ls $1 )")
    first="1"
    cp=""
    for jar in $arr; do
        if [[ "$first" = "1" ]]; then
            first="0"
            cp="$1/$jar"
        else
            cp="$cp:$1/$jar"
        fi
    done;
    echo $cp;
}

function check() {
    if [ -f $pid ]; then
	if kill -0 `cat $pid` > /dev/null 2>&1; then
	    echo $command running as process `cat $pid`.  Stop it first.
	    exit 1
	fi
    fi
}

function start() {
    java -cp $conf:$cp $main $home > $out 2>&1 &
    echo $! > $pid
}

function stop() {
    if [ -f $pid ]; then
	if kill -0 `cat $pid` > /dev/null 2>&1; then
            echo stopping $command
            kill `cat $pid`
	else
            echo no $command to stop
	fi
    else
	echo no $command to stop
    fi
}

#dir
conf="$home/conf"
pids="$home/pids"
logs="$home/logs"

#file
out="$logs/$command.out"
pid="$pids/$command.pid"

#java
cp="$( classpath "$home/libs" )"

#init
mkdir -p $conf $logs $pids
