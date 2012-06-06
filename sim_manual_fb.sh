#!/bin/bash

rm -rf storage
rm -f *.log
rm -f *.replay

if [ "$1" == "" ]; then
    A=0
else
    A=$1
fi
if [ "$2" == "" ]; then
    B=0
else
    B=$2
fi
if [ "$3" == "" ]; then
    C=0
else
    C=$3
fi
if [ "$4" == "" ]; then
    D=0
else
    D=$4
fi

./execute.pl -s -n FacebookNode 0 -f 0 -L totallyOrderedLog -l partiallyOrderedLog -w $A -x $B -y $C -z $D
