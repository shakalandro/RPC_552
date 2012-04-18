echo "Testing basic RPC functionality with 10% delay/drop rates and fail rate = 0"

cd ..

./sim_rpc.sh scripts/RPCNodeDelayDrop 0 0 10 10 > testRPC/out.txt

cd testRPC

ls ../storage/0 >> files.txt
diff files.txt expectedDDFiles.txt >> diff.txt
diff happyFile ../storage/0/happyFile >> diff.txt
diff tester ../storage/0/tester >> diff.txt

if [ -s diff.txt ]; then
  echo "FAIL :("
else
  echo "PASS!!!"
fi

rm files.txt
rm diff.txt

