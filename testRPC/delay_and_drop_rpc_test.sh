echo "Testing basic RPC functionality with 10% delay/drop rates and fail rate = 0"

cd ..

./sim_rpc.sh scripts/RPCNodeDelayDrop 0 0 10 10 > testRPC/out.txt

cd testRPC

ls ../storage/0 >> files.txt
diff files.txt expectedDDFiles.txt >> diff1.txt
if [ -s diff1.txt ]; then
  echo "FAIL :("
  echo "ls did not match expected"
else
  echo "PASS!!!"
fi

diff happyFile ../storage/0/happyFile >> diff2.txt
if [ -s diff2.txt ]; then
  echo "FAIL :("
  echo "happy file contents did not match"
else
  echo "PASS!!!"
fi

diff tester ../storage/0/tester >> diff3.txt
if [ -s diff3.txt ]; then
  echo "FAIL :("
  echo "tester file contents did not match"
else
  echo "PASS!!!"
fi

rm files.txt
rm diff1.txt
rm diff2.txt
rm diff3.txt

