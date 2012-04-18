#!/bin/bash

rm -rf storage
rm -f *.log
rm -f *.replay

./execute.pl -s -n RPCNode -f 0 -L totallyOrderedLog -l partiallyOrderedLog
