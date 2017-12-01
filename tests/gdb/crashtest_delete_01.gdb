set verbose off
set confirm off
set breakpoint pending on

b mongo::PmseRecordStore::deleteRecord
command 1
stop
end
run
quit