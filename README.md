# parallel_read

Simple parallel file read benchmark. Requires gcc-g++ and make. User can specify list of files, number of threads and buffer size. Default buffer size is 1MB. Format of file list is one file per line, e.g.:

```
/fuse/cluster/tmp/1.gz
/fuse/cluster/tmp/2.gz
/fuse/cluster/tmp/3.gz
```
... etc.

### build
make

### usage
./parallel_read \<file list\> \<number of thread\> \<buffer size in kbytes\>

e.g.

./parallel_read /tmp/file.txt 32 128
