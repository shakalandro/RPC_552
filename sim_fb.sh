#!/bin/bash

rm -rf storage
rm -f *.log
rm -f *.replay
./execute.pl -s -n FacebookTestNode -f 0 -L totallyOrderedLog -l partiallyOrderedLog -c scripts/FBTestScript $1
