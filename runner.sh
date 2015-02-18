#!/bin/bash
args=("$@")

containsElement () {
  local e
  for e in "${@:2}"; do [[ "$e" == "$1" ]] && return 0; done
  return 1
}

# usage
if [ ${#args[@]} -eq "0" ]; then
        echo "usage: runner.sh [-i] <inputFileLocation>"
        exit 1;
fi

# make output dir if doesnt exist
if [ -d output ]; then
        rm -rf output
fi

mkdir output

containsElement "-i" "${args[@]}"

if [ $? -eq 0 ]; then
	mvn clean install;

	if [ $? -ne 0 ]; then
		echo "[ERROR] install maven first"
		exit $?
	fi

	if [ ! -d "jar" ]; then
		mkdir jar
	fi

	cp a1/target/a1-0.0.1-SNAPSHOT.jar jar/
	cp a2/target/a2-0.0.1-SNAPSHOT.jar jar/
fi

echo "list of assignments"

i=0
aname=""

while [ $i -eq 0 ]
do
	ls -d */ | grep "^a[0-9]*\/$"
	read -p "Type the assignment name to run or say exit: " aname


        if [ ! -z $aname ]; then

		if [ $aname = "exit" ]; then
			exit 0
		fi
		if [ -d $aname ]; then
                        echo "executing $aname"
                        i=1;

		else
			echo "no such assignment: $aname"
		fi
     	fi
done
cd $aname
./runner.sh $1
cp output/* ../output/
cp log/* ../output/
cd -

echo "Output files are served in a directory inside  current directory named 'output'"
echo "If the output files are unavailable in output directory, please check the HDFS /tmp/ directory"
echo "OR run> hdfs dfd -getmerge /tmp/* output/out"
echo "Run this tool again to execute another Version"

