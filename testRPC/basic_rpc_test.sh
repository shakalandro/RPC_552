echo "Testing basic RPC functionality assumes fail/drop/delay rates all = 0 and
manually controls node failure"

cd ..

./sim_rpc.sh scripts/RPCNode 0 0 0 0 > testRPC/out.txt

cd testRPC

ls ../storage/0 >> files.txt
diff files.txt expectedFiles.txt >> diff.txt
diff happyFile ../storage/0/happyFile >> diff.txt
diff tester ../storage/0/tester >> diff.txt

if [ -s diff.txt ]; then
  echo "FAIL :("
else
  echo "PASS!!!"
fi

rm files.txt
rm diff.txt
