set verbose off
set confirm off
set breakpoint pending on

b mongo::PmseRecordStore::insertRecord
command 1
stop
end
run
cont 49
quit