# This script needs 5 argvs
# eg. 
# python prop_analize.py erl2020-05-09_10-57-16/ 60 23 -1
# erl2020-05-09_10-57-16/    the file directory of  Disrtibution.txt and Baseline.txt
# 60    the iter number of distribution 1
# 23    the iter number of distribution 2
# -1    the iter number of baseline
# if iter == -1, this script will not analyze it
# pay attention: if you want to compare the difference of two policies,
# one of the three iter number must be -1
# if all of them are bigger than -1, only the policy of distribution 1 and distribution 2 will be compared

#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import os
import re
import numpy as np

ACCESSES = 18
OP_TYPE = 3 
CONTENTION_LEVEL = 2
TXN_TYPE = 3
ACTION_DIMENSION = 3

INPUT_SPACE = CONTENTION_LEVEL * OP_TYPE * (ACCESSES)
COMMIT_WAIT_LENGTH = CONTENTION_LEVEL * TXN_TYPE

ACCESSE_SPACE = INPUT_SPACE
PIECE_SPACE = INPUT_SPACE
WAIT_SPACE = INPUT_SPACE + COMMIT_WAIT_LENGTH

# distribution_path1 = "iter70.txt"
# distribution_path2 = "iter75.txt"
# baseline_path = "baseline5.txt"

threshold = 0.90


class Policy:
    def __init__(self):
        self.access_policy = []
        self.wait_policy = []
        self.piece_policy = []
        self.waitinfo1_policy = []
        self.waitinfo2_policy = []
        self.waitinfo3_policy = []
    
    def pick_up(self, policy_path, iter):
        iter_str = "RL at iter " + str(iter)+ ","
        with open(policy_path, 'r') as f:
            line = f.readline().strip()
            while line.find(iter_str) < 0:
                line = f.readline().strip()
            # str access
            f.readline()
            line = f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip()
            line = line.replace('[', '').replace(']', '')
            self.access_policy =  np.array(tuple([int(i) for i in line.split(' ')]), dtype=int) 
            # str wait
            f.readline()
            line = f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip()
            line = line.replace('[', '').replace(']', '')
            self.wait_policy =  np.array(tuple([int(i) for i in line.split(' ')]), dtype=int)
            # str piece
            f.readline()
            line = f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip()
            line = line.replace('[', '').replace(']', '')
            self.piece_policy =  np.array(tuple([int(i) for i in line.split(' ')]), dtype=int) 
            # str wait_info1
            f.readline()
            line = f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip()
            line = line.replace('[', '').replace(']', '')
            self.waitinfo1_policy =  np.array(tuple([int(i) for i in line.split(' ')]), dtype=int) 
            # str wait_info2
            f.readline()
            line = f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip()
            line = line.replace('[', '').replace(']', '')
            self.waitinfo2_policy =  np.array(tuple([int(i) for i in line.split(' ')]), dtype=int) 
            # str wait_info3
            f.readline()
            line = f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip() + " " + f.readline().strip()
            line = line.replace('[', '').replace(']', '')
            self.waitinfo3_policy =  np.array(tuple([int(i) for i in line.split(' ')]), dtype=int) 

    def printpolicy(self):
        print(self.access_policy)
        print(self.wait_policy)
        print(self.piece_policy)
        print(self.waitinfo1_policy)
        print(self.waitinfo2_policy)
        print(self.waitinfo3_policy)


class Distribution_input:
    def __init__(self, path, iter):
        self.access_input= []
        self.wait_input = []
        self.piece_input = []
        self.wait_info1_input = []
        self.wait_info2_input = []
        self.wait_info3_input = []
        
        # policy from distribution
        self.policy = Policy()

        self.distribution_path = path
        self.iter = iter

    def get_policy(self, data, len):
        policy = []
        for i in range(len):
            length = 0
            for num in data[0]:
                length += 1
            max = 0
            max_id = -1
            for j in range(length):
                if data[i][j] > max:
                    max = data[i][j]
                    max_id = j
            policy.append(max_id)
        return policy

    def setpolicy(self):
        self.policy.access_policy =  self.get_policy(self.access_input, ACCESSE_SPACE)
        self.policy.wait_policy =  self.get_policy(self.wait_input, WAIT_SPACE)
        self.policy.piece_policy =  self.get_policy(self.piece_input, ACCESSE_SPACE)
        self.policy.waitinfo1_policy = self.get_policy(self.wait_info1_input, WAIT_SPACE)
        self.policy.waitinfo2_policy = self.get_policy(self.wait_info2_input, WAIT_SPACE)
        self.policy.waitinfo3_policy =  self.get_policy(self.wait_info3_input, WAIT_SPACE)
    
    def write_policy_to_file(self):
        policy_path = "iter" + str(self.iter) + "_policy.txt"
        open(policy_path, 'w+')
        with open(policy_path, 'a+') as f:
            f.write("txn final commit wait\n")
            for i in range(COMMIT_WAIT_LENGTH): 
                f.write(str(self.policy.wait_policy[i+ACCESSE_SPACE])+" ")
                f.write(str(self.policy.waitinfo1_policy[i+ACCESSE_SPACE])+" ")
                f.write(str(self.policy.waitinfo2_policy[i+ACCESSE_SPACE])+" ")
                f.write(str(self.policy.waitinfo3_policy[i+ACCESSE_SPACE])+" ")
                f.write("\n")
            f.write("normal access\n")
            for i in range(ACCESSE_SPACE):  
                f.write(str(self.policy.access_policy[i])+" ")
                f.write(str(self.policy.wait_policy[i])+" ")
                f.write(str(self.policy.piece_policy[i])+" ")
                f.write(str(self.policy.waitinfo1_policy[i])+" ")
                f.write(str(self.policy.waitinfo2_policy[i])+" ")
                f.write(str(self.policy.waitinfo3_policy[i])+" ")
                f.write("\n")

    def line_pro(self, line):
        line = line.replace('[[', '').replace(' [', '').replace(']]', ']')
        line = line.replace('      ]', '').replace('     ]', '').replace('    ]', '').replace('   ]', '').replace('  ]', '').replace(' ]', '')
        line = line.replace('  ', ' ').replace('   ', ' ').replace('    ', ' ').replace('     ', ' ').replace('      ', ' ').replace('       ', ' ')
        line = line.replace('\n', '').replace('[', '').replace(']', '')
        line = line.replace('  ', ' ').replace('   ', ' ').replace('    ', ' ').replace('     ', ' ').replace('      ', ' ').replace('       ', ' ')
        return line

    def pick_up(self):
        iter_str = "RL at iter " + str(self.iter)
        with open(self.distribution_path, 'r') as f:
            # iter idx
            line = f.readline().strip()
            while line != iter_str:
                line = f.readline().strip()
            # str access_prop
            f.readline()
            for i  in range (ACCESSE_SPACE):
                line = f.readline().strip()
                if line.find("]") < 0:
                    line2 = f.readline().strip()
                    line += " "
                    line += line2
                line = self.line_pro(line)
                curline =  np.array(tuple([float(i) for i in line.split(' ')]), dtype=float) 
                self.access_input.append(curline)
            # str wait_prop
            f.readline()
            for i  in range (WAIT_SPACE):
                line = f.readline().strip()
                if line.find("]") < 0:
                    line2 = f.readline().strip()
                    line += " "
                    line += line2
                line = self.line_pro(line)
                curline =  np.array(tuple([float(i) for i in line.split(' ')]), dtype=float) 
                self.wait_input.append(curline)
            # str piece_prop
            f.readline()
            for i  in range (PIECE_SPACE):
                line = f.readline().strip()
                if line.find("]") < 0:
                    line2 = f.readline().strip()
                    line += " "
                    line += line2
                line = self.line_pro(line)
                curline =  np.array(tuple([float(i) for i in line.split(' ')]), dtype=float) 
                self.piece_input.append(curline)
            # str wait_info1
            f.readline()
            for i  in range (WAIT_SPACE):
                line = f.readline().strip()
                if line.find("]") < 0:
                    line2 = f.readline().strip()
                    line += " "
                    line += line2
                    if line2.find("]") < 0:
                        line3 = f.readline().strip()
                        line += " "
                        line += line3
                line = self.line_pro(line)
                curline =  np.array(tuple([float(i) for i in line.split(' ')]), dtype=float) 
                self.wait_info1_input.append(curline)
            # for i in range(13):
            #     f.readline()
            # str wait_info2
            f.readline()
            for i  in range (WAIT_SPACE):
                line = f.readline().strip()
                if line.find("]") < 0:
                    line2 = f.readline().strip()
                    line += " "
                    line += line2
                line = self.line_pro(line)
                curline =  np.array(tuple([float(i) for i in line.split(' ')]), dtype=float) 
                self.wait_info2_input.append(curline)
            # str wait_info3
            f.readline()
            for i  in range (WAIT_SPACE):
                line = f.readline().strip()
                if line.find("]") < 0:
                    line2 = f.readline().strip()
                    line += " "
                    line += line2
                line = self.line_pro(line)
                curline =  np.array(tuple([float(i) for i in line.split(' ')]), dtype=float) 
                self.wait_info3_input.append(curline)


# compare two distributions
def distribution_compare(distirbution1, distirbution2):
    def compare_each(data1, data2, len, threshold):
        same_cnt = 0
        for i in range(len):
            flag1 = -1
            flag2 = -1
            length = 0
            for num in data1[0]:
                length += 1
            # print("length:"+str(length))
            for j in range(length):
                if data1[i][j] > threshold:
                     flag1 = j
            for j in range(length):
                if data2[i][j] > threshold:
                     flag2 = j   
            if flag1 == flag2:
                same_cnt += 1
        return same_cnt

    access_same    = compare_each(distribution1.access_input, distribution2.access_input, ACCESSE_SPACE, threshold)
    wait_same      = compare_each(distribution1.wait_input, distribution2.wait_input, WAIT_SPACE, threshold)
    piece_same     = compare_each(distribution1.piece_input, distribution2.piece_input, PIECE_SPACE, threshold)
    waitinfo1_same = compare_each(distribution1.wait_info1_input, distribution2.wait_info1_input, WAIT_SPACE, threshold)
    waitinfo2_same = compare_each(distribution1.wait_info2_input, distribution2.wait_info2_input, WAIT_SPACE, threshold)
    waitinfo3_same = compare_each(distribution1.wait_info3_input, distribution2.wait_info3_input, WAIT_SPACE, threshold)

    total = access_same + wait_same + piece_same + waitinfo1_same + waitinfo2_same + waitinfo3_same
    action_total = ACCESSE_SPACE + WAIT_SPACE + PIECE_SPACE + WAIT_SPACE * 3

    print("access_same,"+str(access_same))
    print("wait_same," + str(wait_same))
    print("piece_same," + str(piece_same))
    print("waitinfo1_same," + str(waitinfo1_same))
    print("waitinfo2_same," + str(waitinfo2_same))
    print("waitinfo3_same," + str(waitinfo3_same))
    print("same_total:"+str(total)+", " + "action_total:" + str(action_total) +", " + "same_rate:" + str((total) / action_total))



# compare two policies
def policy_compare(policy1, policy2):
    policy_path_diff = "policy_diff.txt"
    open(policy_path_diff, 'w+')
    def compare_each(data1, data2, length, name):
        for i in range(length):
            if data1[i] != data2[i]:
                with open(policy_path_diff, 'a+') as f:
                    f.write(name+", "+str(i)+", "+str(data1[i])+", "+str(data2[i])+"\n")

    compare_each(policy1.access_policy,    policy2.access_policy, ACCESSE_SPACE, "access")
    compare_each(policy1.wait_policy,      policy2.wait_policy, WAIT_SPACE, "wait")
    compare_each(policy1.piece_policy,     policy2.piece_policy, PIECE_SPACE, "piece")
    compare_each(policy1.waitinfo1_policy, policy2.waitinfo1_policy, WAIT_SPACE, "waitinfo1")
    compare_each(policy1.waitinfo2_policy, policy2.waitinfo2_policy, WAIT_SPACE, "waitinfo2")
    compare_each(policy1.waitinfo3_policy, policy2.waitinfo3_policy, WAIT_SPACE, "waitinfo3")
    


if len(sys.argv) < 5:
    print("error, the number of argvs is less than 5")
    os._exit()

DirectoryPath = sys.argv[1]
distribution_path = DirectoryPath + "Distribution.txt"
baseline_path = DirectoryPath + "Baseline.txt"
# get distribution 1, get policy from this ditribution and write it to file
if int(sys.argv[2]) >= 0:
    distribution1 = Distribution_input(distribution_path, int(sys.argv[2]))
    distribution1.pick_up()
    distribution1.setpolicy()
    distribution1.write_policy_to_file()
# get distribution 2, get policy from this ditribution and write it to file
if int(sys.argv[3]) >= 0:
    distribution2 = Distribution_input(distribution_path, int(sys.argv[3]))
    distribution2.pick_up()
    distribution2.setpolicy()
    distribution2.write_policy_to_file()
# get baseline policy
if int(sys.argv[4]) >= 0:
    baseline_policy = Policy()
    baseline_policy.pick_up(baseline_path, int(sys.argv[4]))

# compare two distributions
if int(sys.argv[2]) >= 0 and int(sys.argv[3]) >= 0:
    distribution_compare(distribution1, distribution2)

# compare two polices
if int(sys.argv[2]) >= 0 and int(sys.argv[3]) >= 0:
    policy_compare(distribution1.policy, distribution2.policy)
elif int(sys.argv[2]) >= 0 and int(sys.argv[4]) >= 0:
    policy_compare(distribution1.policy, baseline_policy)
elif int(sys.argv[3]) >= 0 and int(sys.argv[4]) >= 0:
    policy_compare(distribution2.policy, baseline_policy)

