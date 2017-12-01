set verbose off
set confirm off
set breakpoint pending on

b mongo::PmseSortedDataInterface::insert
command 1
stop
end
run
cont 6
quit