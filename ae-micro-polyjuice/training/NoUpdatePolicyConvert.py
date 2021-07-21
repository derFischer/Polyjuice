# 

import numpy as np
import sys

# input file, defaut
filepath = ''
file_input = filepath + 'input_RL_ic3_tpcc.txt'

# output file, defaut
file_output = filepath + 'output_RL_tpcc.txt'

ACCESSES = 18
TXN_TYPE = 3
CONTENTION_LEVEL = 2

# convert RL policy to genetic policy
def policy_convert(f):

    def access_id_convert(data,i):
        if data[TXN_TYPE] == 0:
            data[TXN_TYPE] = 0
        elif data[TXN_TYPE] == 1:
            data[TXN_TYPE] = 1
        elif data[TXN_TYPE] == 2:
            data[TXN_TYPE] = 3
        elif data[TXN_TYPE] == 3:
            data[TXN_TYPE] = 4
        elif data[TXN_TYPE] == 4:
            data[TXN_TYPE] = 6
        elif data[TXN_TYPE] == 5:
            data[TXN_TYPE] = 7
        elif data[TXN_TYPE] == 6:
            data[TXN_TYPE] = 8
        elif data[TXN_TYPE] == 7:
            data[TXN_TYPE] = 9
        elif data[TXN_TYPE] == 8:
            data[TXN_TYPE] = 10
        elif data[TXN_TYPE] == 9:
            data[TXN_TYPE] = 11

        if data[TXN_TYPE + 1] == 0:
            data[TXN_TYPE + 1] = 0
        elif data[TXN_TYPE + 1] == 1:
            data[TXN_TYPE + 1] = 2
        elif data[TXN_TYPE + 1] == 2:
            data[TXN_TYPE + 1] = 4
        elif data[TXN_TYPE + 1] == 3:
            data[TXN_TYPE + 1] = 6
        elif data[TXN_TYPE + 1] == 4:
            data[TXN_TYPE + 1] = 7


        if data[TXN_TYPE + 2] == 0:
            data[TXN_TYPE + 2] = 0
        elif data[TXN_TYPE + 2] == 1:
            data[TXN_TYPE + 2] = 1
        elif data[TXN_TYPE + 2] == 2:
            data[TXN_TYPE + 2] = 2
        elif data[TXN_TYPE + 2] == 3:
            data[TXN_TYPE + 2] = 4
        elif data[TXN_TYPE + 2] == 4:
            data[TXN_TYPE + 2] = 6
        elif data[TXN_TYPE + 2] == 5:
            data[TXN_TYPE + 2] = 8

        return data

    def txn_final_wait_convert(data):
        if data[1] == 0:
            data[1] = 0
        elif data[1] == 1:
            data[1] = 1
        elif data[1] == 2:
            data[1] = 3
        elif data[1] == 3:
            data[1] = 4
        elif data[1] == 4:
            data[1] = 6
        elif data[1] == 5:
            data[1] = 7
        elif data[1] == 6:
            data[1] = 8
        elif data[1] == 7:
            data[1] = 9
        elif data[1] == 8:
            data[1] = 10
        elif data[1] == 9:
            data[1] = 11

        if data[1 + 1] == 0:
            data[1 + 1] = 0
        elif data[1 + 1] == 1:
            data[1 + 1] = 2
        elif data[1 + 1] == 2:
            data[1 + 1] = 4
        elif data[1 + 1] == 3:
            data[1 + 1] = 6
        elif data[1 + 1] == 4:
            data[1 + 1] = 7


        if data[1 + 2] == 0:
            data[1 + 2] = 0
        elif data[1 + 2] == 1:
            data[1 + 2] = 1
        elif data[1 + 2] == 2:
            data[1 + 2] = 2
        elif data[1 + 2] == 3:
            data[1 + 2] = 4
        elif data[1 + 2] == 4:
            data[1 + 2] = 6
        elif data[1 + 2] == 5:
            data[1 + 2] = 8

        return data


    buffer_size = []
    const_backoff = []
    backoff = []
    txn_final_commit_wait = []

    normal_access = []
    
    # pick up RL policy
    # txn buffer size
    f.readline()
    line = f.readline().replace(' \n', '')
    buffersize = np.array(tuple([int(line[idx]) for idx in range(1)]), dtype=int)
    # const backoff
    f.readline()
    for i in range(CONTENTION_LEVEL):
        line = f.readline().replace(' \n', '')
        const_backoff.append(np.array(tuple([int(line[idx]) for idx in range(TXN_TYPE)]), dtype=int))
    
    # retry backoff
    f.readline()
    for i in range(CONTENTION_LEVEL):
        # backoff bound
        line = f.readline().replace(' \n', '')
        backoff.append(np.array(tuple([int(i) for i in line.replace('\n', '').split(' ')]), dtype=int))

    # txn final commit wait
    f.readline()
    for i in range(CONTENTION_LEVEL * TXN_TYPE):
        # backoff bound
        line = f.readline().replace(' \n', '')
        txn_final_commit_wait.append(np.array(tuple([int(i) for i in line.replace('\n', '').split(' ')]), dtype=int))

    # normal_access
    f.readline()
    for i in range(CONTENTION_LEVEL * 2 * ACCESSES):
        # backoff bound
        line = f.readline().replace(' \n', '')
        normal_access.append(np.array(tuple([int(i) for i in line.replace('\n', '').split(' ')]), dtype=int))
    
    
    # write policy to file
    def write_info(fw):
        # buffersize
        fw.write("txn buffer size\n")
        fw.write(str(buffersize[0]) + "\n")
        # const retyr backoff
        fw.write("const retry backoff\n")
        for i in range(len(const_backoff)):
            for j in range(TXN_TYPE):
                fw.write(str(const_backoff[i][j]))
            fw.write("\n")
        # retry backoff
        fw.write("retry backoff\n")
        for i in range(len(backoff)):
            for j in range(TXN_TYPE - 1):
                fw.write(str(backoff[i][j]) + " ")
            fw.write(str(backoff[i][TXN_TYPE-1]) + "\n")
        # txn final commit wait
        fw.write("txn final commit wait\n")
        for i in range(len(txn_final_commit_wait)):
            data = txn_final_wait_convert(txn_final_commit_wait[i])
            for j in range(TXN_TYPE):
                fw.write(str(data[j]) + " ")
            fw.write(str(data[TXN_TYPE]) + "\n")

        # txn final commit wait
        fw.write("normal access\n")
        def access_write(fw, i):
            for k in range(2 * CONTENTION_LEVEL):
                data = access_id_convert(normal_access[i * 2 * CONTENTION_LEVEL + k], i)
                for j in range(TXN_TYPE + 2):
                    fw.write(str(data[j]) + " ")
                fw.write(str(data[TXN_TYPE + 2]) + "\n")
        
        def access_write_twice(fw, i):
            for k in range(2 * CONTENTION_LEVEL):
                data = normal_access[i * 2 * CONTENTION_LEVEL + k]
                for j in range(TXN_TYPE + 2):
                    fw.write(str(data[j]) + " ")
                fw.write(str(data[TXN_TYPE + 2]) + "\n")

        def write_twice(i):
            if i == 1 or i == 3 or i == 9 or i == 10 or i == 11 or i == 15 or i == 16 or i == 17:
                return True
            else:
                return False
        
        for i in range(ACCESSES):
            access_write(fw, i)
            if write_twice(i):
                access_write_twice(fw, i)

    open(file_output, 'w+')
    with open(file_output, 'w') as fw:
        write_info(fw)


if __name__ == '__main__':
    if(len(sys.argv) > 1):
        file_input = sys.argv[1]
    if (len(sys.argv) > 2):
        file_output = sys.argv[2]

    f = open(file_input)
    policy_convert(f)
    print("convertion finished")