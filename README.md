# parallel_read
Simple parallel file read benchmark. Requires gcc-g++ and make.

### build
make

### usage
./parallel_read \<file list\> \<number of thread\> \<buffer size\>

e.g.

./parallel_read /tmp/file.txt 32 128
