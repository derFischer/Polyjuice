#!/usr/bin/env python

# number of txn types
TXN_TYPE = 0
# number of total access
ACCESSES = 0
# [placeholder, number of type1 accesses, number of type2 accesses]
TXN_ACCESS_NUM = [ 0, 0 ]

READ_ACCS = [0]

# tpcc workload info
TPCC_TXN_TYPE = 3
TPCC_ACCESSES = 18
TPCC_TXN_ACCESS_NUM = [ 0, 9, 4, 5 ]
TPCC_READS = [0, 2, 8, 13]

# tpce workload info
TPCE_TXN_TYPE = 4
TPCE_ACCESSES = 105
TPCE_TXN_ACCESS_NUM = [ 0, 30, 48, 10, 17 ]
TPCE_READS =[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 29, 30, 31, 32, 33, 38, 39, 52, 53, 67, 68, 69, 70, 71, 79, 80, 81, 82, 89, 90, 91, 92, 93, 94, 95, 97, 98, 99, 100, 101, 102, 103]

# ycsb workload info
YCSB_TXN_TYPE = 3
YCSB_ACCESSES = 3
YCSB_TXN_ACCESS_NUM = [ 0, 1, 1, 1 ]
YCSB_READS = [0]