#!/bin/bash
args=("$@")

# usage
if [ ${#args[@]} -ne "2" ]; then
	echo "usage: runner.sh <javaClassName> <inputFileLocation>"
	exit 1;
fi

# make output dir if doesnt exist
if [ -d output ]; then
	rm -rf output
fi

mkdir output



jarfile="target/a1-0.0.1-SNAPSHOT.jar"

if [ ! -f "$jarfile" ]; then
	echo "jar file does not exist please execute runner with -i option"
	exit 1
fi

if echo "$1" | grep -q "seq"; then
	echo "running Sequential JAVA"
	java -Xmx2048M -cp "$jarfile" "$1" "$2" output
else

	echo "adding input file to HDFS"
	hdfs dfs -mkdir -p /user/hduser
	hdfs dfs -copyFromLocal $2 /user/hduser/
	hdfs dfs -ls /user/hduser
	echo "removing hdfs output directory \[/tmp/\]"
	hdfs dfs -rm -r /tmp
	
	filename=basename $2
	echo "preparing to run $1"
	`hadoop jar "$jarfile" "$1" "/user/hduser/$filename" /tmp/out` > /dev/null 2>&1
	
	hdfs dfs -getmerge /tmp/out "output/$1.out"

fi
