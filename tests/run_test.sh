insert_test_num="$(find . -maxdepth 1 -type f -name 'crashtest_insert_??.sh' | wc -l)";
update_test_num="$(find . -maxdepth 1 -type f -name 'crashtest_update_??.sh' | wc -l)";
delete_test_num="$(find . -maxdepth 1 -type f -name 'crashtest_delete_??.sh' | wc -l)";
mydb="/mnt/psmem_0/";

while getopts ":m:i:u:d:a" opt; do
  case $opt in
    m)
      mydb=$OPTARG;
      ;;
    i)
      if (($OPTARG > 0)) && (($OPTARG <= $insert_test_num)); then
        test_group="insert";
        test_num=$OPTARG;
      else
        echo "Invalid test number in insert test group."
	    exit 1
      fi
      ;;
    u)
      if (($OPTARG > 0)) && (($OPTARG <= $update_test_num)); then
        test_group="update";
        test_num=$OPTARG;
      else
        echo "Invalid test number in update test group."
        exit 1
      fi
      ;;
    d)
      if (($OPTARG > 0)) && (($OPTARG <= $delete_test_num)); then
        test_group="delete";
        test_num=$OPTARG;
      else
        echo "Invalid test number in delete test group."
        exit 1
      fi
      ;;
    a)
      test_num=1;
      while ((test_num<=$insert_test_num)); do
        ./run_test.sh -m "$mydb" -i "$test_num"
        ((test_num++))
      done
      test_num=1;
      while ((test_num<=$update_test_num)); do
        ./run_test.sh -m "$mydb" -u "$test_num"
        ((test_num++))
      done
      test_num=1;
      while ((test_num<=$delete_test_num)); do
        ./run_test.sh -m "$mydb" -d "$test_num"
        ((test_num++))
      done
      exit 1
      ;;
    \?)
      echo "Invalid option: -$OPTARG, please use: -i[nsert] x, -u[pdate] x, -d[elete] x, a[ll], where x = test number." >&2
      exit 1
      ;;
    :)
      echo "Test group -$OPTARG requires an argument with test number." >&2
      exit 1
      ;;
  esac
done

if ((test_num < 10)); then
    test_num="0"$test_num;
fi

echo "Running crashtest_"$test_group"_"$test_num"...";

killall mongod > /dev/null 2>&1
rm -rf /mnt/psmem_0/ > /dev/null 2>&1
cd ../../../../../../

gdb --batch --command=src/mongo/db/modules/pmse/tests/gdb/crashtest_"$test_group"_"$test_num".gdb --args ./mongod --dbpath="$mydb" --storageEngine=pmse --bind_ip 127.0.0.1 > src/mongo/db/modules/pmse/tests/log/crashtest_"$test_group"_"$test_num"_gdb_log.txt 2>&1 &

mongo_port=0;

while ((mongo_port < 1)); do
    mongo_port="$(netstat -l | grep 27017 | wc -l)";
done

if ./mongo src/mongo/db/modules/pmse/tests/js/crashtest_"$test_group"_"$test_num".js > src/mongo/db/modules/pmse/tests/log/crashtest_"$test_group"_"$test_num"_shell_log.txt 2>&1; then
    echo "Success"
else
    echo "Fail!"
fi
killall mongod > /dev/null 2>&1
