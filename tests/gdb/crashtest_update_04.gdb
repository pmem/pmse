set verbose off
set confirm off
set breakpoint pending on

b mongo::PmseSortedDataInterface::unindex
command 1
stop
end
run
cont 3
quit