#!/bin/sh

if [ $# != 1 ]
then
    echo "Usage : clean_fs.sh <nbRuns>"
else
	for c in $(seq 1 $1)
    do
            hadoop fs -rmr "/pagerank/iter$c"
    done
fi
