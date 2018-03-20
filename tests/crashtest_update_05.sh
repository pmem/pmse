echo "Running crashtest_update_05...";

mydb="/mnt/psmem_0/";

while getopts ":m:" opt; do
  case $opt in
    m)
      mydb=$OPTARG;
      ;;
    \?)
      echo "Invalid option: -$OPTARG, please use: -m[mongoDB] xyz, where xyz = dbpath." >&2
      exit 1
      ;;
    :)
      echo "-$OPTARG requires an argument with dbpath." >&2
      exit 1
      ;;
  esac
done

killall mongod > /dev/null 2>&1
rm -rf /mnt/psmem_0/ > /dev/null 2>&1
cd ../../../../../../

gdb --batch --command=src/mongo/db/modules/pmse/tests/gdb/crashtest_update_05.gdb --args ./mongod --dbpath="$mydb" --storageEngine=pmse --bind_ip 127.0.0.1 > src/mongo/db/modules/pmse/tests/log/crashtest_update_05_gdb_log.txt 2>&1 &

mongo_port=0;

while ((mongo_port < 1)); do
    mongo_port="$(netstat -l | grep 27017 | wc -l)";
done

if ./mongo src/mongo/db/modules/pmse/tests/js/crashtest_update_05.js > src/mongo/db/modules/pmse/tests/log/crashtest_update_05_shell_log.txt 2>&1; then
    echo "Success"
else
    echo "Fail!"
fi
killall mongod > /dev/null 2>&1
