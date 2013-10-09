external_sort
=============

Sorting files on disk with limited memory

Input arguments are the file name and memory limit in bytes.
Note that file size must be divisible by 4 since 4-byte blocks are soted.

Usage: external_sort.exe <filename> <memory_limit_bytes>

Sorted file will have the name <filename>.sorted
