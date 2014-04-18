#!/bin/bash
i=0
while read line; do
    machines[$i]="$line"
    i=$i+1
done < machines

for (( i = 0; i < $2; i++ )); do
    echo -n $1'ing' ${machines[$i]}
    if [[ $1 == 'start' ]]; then
	for (( j = 0; j < $3; j++ )); do
	    echo ' jvm #'${j+1}
	    ssh -n -f ${machines[$i]} 'cd research/h2o; java -Xmx8g -jar target/h2o.jar 1>mc0'${i+1}'_'${j+1}'.log &'
	done
    elif [[ $1 == 'stop' ]]; then
	ssh ${machines[$i]} 'pkill -f h2o.jar'
	ssh ${machines[$i]} 'rm -rf /tmp/h2o-temp* /tmp/hsperfdata_reese5*'
	echo ''

    elif [[ $1 == 'clean' ]]; then
	ssh ${machines[$i]} 'pkill -f h2o.jar'
	ssh ${machines[$i]} 'rm -rf /tmp/h2o* /tmp/hsperfdata_reese5*'
	echo ''
    fi
done
echo 
if [[ $1 == 'size' ]]; then
    >size.log
    for (( i = 0; i < $2; i++ )); do
	ssh -n -f ${machines[$i]} 'grep "Cloud of" /tmp/h2ologs/*1.log' >> size.log
    done
fi
