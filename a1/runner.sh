#!/bin/bash
args=("$@")


# usage


if [ ${#args[@]} -ne "1" ]; then
        echo "usage: runner.sh <inputFileLocation>"
        exit 1;
fi

# make output dir if doesnt exist
if [ -d output ]; then
        rm -rf output
fi

mkdir output

echo "List of versions"

i=0
aname=""

function printVersions () {
	local e
	for e in "${@:1}"; do echo $e; done
	return 0
}

function getVersion () {
	local e
	for e in "${@:2}"; do
		if echo "$e" | grep -q "$1"; then
			echo $e
			return 0
		fi
	done
	return 1 
}

versions=("a1.seq.v1.MedianPriceEvaluator" "a1.mapred.v2.MedianPriceEvaluator" "a1.mapred.v3.MedianPriceEvaluator" "a1.mapred.v4.MedianPriceEvaluator")
ver=""

while [ $i -eq 0 ]
do
	printVersions "${versions[@]}"

	read -p "enter version to run [v1|v2|v3|v4] or exit: " aname 


	if [ ! -z $aname ]; then
		 
		if [ $aname = "exit" ]; then
			exit 0
		fi

		ver=`getVersion "$aname" "${versions[@]}"`
		if [ $? -eq 0 ]; then
			echo "executing $ver"
			i=1;
		else
			echo "no such version: $aname"
		fi
	
	fi
done


./run-hadoop.sh $ver $1

echo "executed $ver"
