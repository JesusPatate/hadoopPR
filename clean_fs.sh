#!/bin/bash

if [ $# != 1 ]
then
    echo "Usage : clean_fs.sh <nbRuns>"
else
    for (( c=1; c<=$1; c++ ))
    do
            hadoop fs -rmr "/pagerank/iter$c"
    done
fi
