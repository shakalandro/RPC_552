echo "Testing basic RPC functionality assumes fail/drop/delay rates all = 0 and
manually controls node failure"

cd ..

./sim_rpc.sh scripts/RPCNode 0 0 0 0 > testRPC/out.txt

cd testRPC


ls ../storage/0 >> files.txt
diff files.txt expectedFiles.txt >> diff1.txt
if [ -s diff1.txt ]; then
  echo "FAIL :("
  echo "files listed don't match, diff:"
  cat diff1.txt
else
  echo "PASS!!!"
fi

diff happyFile ../storage/0/happyFile >> diff2.txt
if [ -s diff2.txt ]; then
  echo "FAIL :("
  echo "happyFile files don't match, diff:"
  cat diff2.txt
else
  echo "PASS!!!"
fi

diff tester ../storage/0/tester >> diff3.txt
if [ -s diff3.txt ]; then
  echo "FAIL :("
  echo "tester files don't match, diff:"
  cat diff3.txt
else
  echo "PASS!!!"
fi


rm files.txt
rm diff1.txt
rm diff2.txt
rm diff3.txt
