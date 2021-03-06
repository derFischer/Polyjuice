import numpy as np
import copy
import os
import sys
import time
import shutil
import re
import signal
import subprocess
import numpy as np
import math
from random import randint, sample
from Policy import *

SWITCH_STAGE_THRESHOLD = 10

MAX_TXN_BUF_SIZE = 32

class record:
    def __init__(self, idx, reward):
        self.policy = idx
        self.reward = reward

class EA:
    def __init__(self, log_dir, kid_dir, selection, pop_size, bfactor, mutate_decay, mut_frac):
        self.log_dir = log_dir
        self.kid_dir = kid_dir
        self.selection = selection          # selection methodology
        self.pop_size = pop_size            # population size
        self.pops = []                      # population array
        self.bfactor = bfactor              # how many children generated by one parent
        self.mutate_decay = mutate_decay    # whether mutate frac decays with iterations
        self.max_mut_frac = mut_frac        # EA initial mutate fraction
        self.mut_frac = mut_frac            # EA actually used value, will be decaying

        self.best_seen = 0
        self.round_best = 0
        self.round_mean = 0
        self.round_worst = 0

        self.best_seen_sample = None

        self.round_update = False
        self.no_update_count = 0

        # stage 0 - wait mutation, stage 1 - full mutation(include access & piece_end & different acc_type's wait_info)
        self.training_stage = 0 # initial state
        
        self.access_act, self.wait_act, self.piece_act = [], [], []
        self.wait_info_act1, self.wait_info_act2, self.wait_info_act3 = [], [], []
        self.reward_buffer = []

        self.pops_record = []

    def clear_round_info(self):
        self.round_best = 0
        self.round_mean = 0
        self.round_worst = 0
        self.round_update = False

    def initialize_pops(self, sample):
        if isinstance(sample, Sample):
            self.pops.append(sample)
        elif isinstance(sample, list):
            self.pops.extend(sample)
        else:
            print('Unknown initialized EA type')
            assert False

    def mutate(self, sample, decay_rate):
        # txn_buf_size - [1, 32]
        step_width = int(8)     # assign txn_buf_size step width as 8
        mut_txn_buf_pace = ((np.random.rand(1) * step_width) + 1).astype(int) * ((np.random.rand(1) < .5) * 2 - 1).astype(int)
        mut_txn_buf_size = (np.random.rand(1) < self.mut_frac).astype(int)
        if mut_txn_buf_size[0] == 1:
            txn_buf_size = int((sample.txn_buf_size + mut_txn_buf_pace - 1).astype(int) % MAX_TXN_BUF_SIZE + 1)
        else:
            txn_buf_size = sample.txn_buf_size
        
        # backoff
        backoff_choices = ['0', '1', '2', '4', '8']
        backoff_step_width = int(len(backoff_choices))
        backoff_idx = []
        for i in range(len(sample.backoff)):
            for idx in range(len(backoff_choices)):
                if backoff_choices[idx] == sample.backoff[i]:
                    backoff_idx.append(idx)
                    break
            if len(backoff_idx) != (i + 1):
                assert False
        mut_backoff_pace = ((np.random.rand(len(sample.np_backoff)) * (backoff_step_width - 1)) + 1).astype(int)
        mut_backoff = (np.random.rand(len(sample.np_backoff)) < self.mut_frac).astype(int)
        backoff_idx = (backoff_idx + mut_backoff * mut_backoff_pace) % backoff_step_width
        backoff = []
        for i in range(len(backoff_idx)):
            backoff.append(backoff_choices[backoff_idx[i]])
        
        # access mutation
        access_mut = (np.random.rand(len(sample.np_access)) < self.mut_frac).astype(int)
        access = sample.np_access ^ access_mut
        # access mutation only for stage >= 1
        if self.training_stage < 1:
            access = sample.np_access

        # wait mutation
        wait_mut = (np.random.rand(len(sample.np_wait)) < self.mut_frac).astype(int)
        wait = sample.np_wait ^ wait_mut

        # piece mutation
        piece_mut = (np.random.rand(len(sample.np_piece)) < self.mut_frac).astype(int)
        piece = sample.np_piece ^ piece_mut
        # piece mutation only for stage >= 1
        if self.training_stage < 1:
            piece = sample.np_piece

        # wait_info1 mutation
        parent_wait_info1 = sample.np_wait_info1
        wait_info1_mut = (np.random.rand(len(parent_wait_info1)) < self.mut_frac).astype(int)
        # step width
        wait_info1_step_width = int(decay_rate * wait_info_act_count[0])
        wait_info1_step = ((np.random.rand(len(parent_wait_info1)) * wait_info1_step_width) + 1).astype(int) * ((np.random.rand(len(parent_wait_info1)) < .5) * 2 - 1).astype(int)
        # do mutation
        wait_info1 = (parent_wait_info1 + wait_info1_mut * wait_info1_step) % (wait_info_act_count[0])

        # wait_info2 mutation
        parent_wait_info2 = sample.np_wait_info2
        wait_info2_mut = (np.random.rand(len(parent_wait_info2)) < self.mut_frac).astype(int)
        # step width
        wait_info2_step_width = int(decay_rate * wait_info_act_count[1])
        wait_info2_step = ((np.random.rand(len(parent_wait_info2)) * wait_info2_step_width) + 1).astype(int) * ((np.random.rand(len(parent_wait_info2)) < .5) * 2 - 1).astype(int)
        # do mutation
        wait_info2 = (parent_wait_info2 + wait_info2_mut * wait_info2_step) % (wait_info_act_count[1])

        # wait_info3 mutation
        parent_wait_info3 = sample.np_wait_info3
        wait_info3_mut = (np.random.rand(len(parent_wait_info3)) < self.mut_frac).astype(int)
        # step width
        wait_info3_step_width = int(decay_rate * wait_info_act_count[2])
        wait_info3_step = ((np.random.rand(len(parent_wait_info3)) * wait_info3_step_width) + 1).astype(int) * ((np.random.rand(len(parent_wait_info3)) < .5) * 2 - 1).astype(int)
        # do mutation
        wait_info3 = (parent_wait_info3 + wait_info3_mut * wait_info3_step) % (wait_info_act_count[2])

        # pruning - rollback some of the wait_info - no_wait action do not need wait_info mutation
        for i in range(WAIT_SPACE):
            if wait[i] == 0: # this is a no_wait action
                wait_info1[i] = parent_wait_info1[i]
                wait_info2[i] = parent_wait_info2[i]
                wait_info3[i] = parent_wait_info3[i]
        # pruning - rollback some of the wait & wait_info - no early validation no waiting
        # count_per_access = CONTENTION_LEVEL * OP_TYPE
        # for i in range(ACCESSES):
        #    for j in range(CONTENTION_LEVEL):
        #        if piece[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j] == 0:
        #            wait[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j] = sample.np_wait[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j]
        #            wait_info1[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j] = parent_wait_info1[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j]
        #            wait_info2[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j] = parent_wait_info2[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j]
        #            wait_info3[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j] = parent_wait_info3[i * count_per_access + (OP_TYPE - 1) * CONTENTION_LEVEL + j]

        mutate_res =  Sample(access, wait, piece, wait_info1, wait_info2, wait_info3, txn_buf_size, backoff)

        return mutate_res

    def store_actions(self, access, wait, piece, wait_info1, wait_info2, wait_info3):
        self.access_act.extend(access)
        self.wait_act.extend(wait)
        self.piece_act.extend(piece)

        self.wait_info_act1.extend(wait_info1)
        self.wait_info_act2.extend(wait_info2)
        self.wait_info_act3.extend(wait_info3)

    # store corresponding reward
    def record_reward(self, round_id, reward, previous_samples, idx, sample):
        # process some global records
        if reward > self.best_seen:
            self.best_seen = reward
            self.best_seen_sample = copy.deepcopy(sample)
            self.round_update = True
            # save genetic best seen result
            print('Update genetic best seen sample - {}'.format(reward))
            kid_path = os.path.join(os.getcwd(), self.kid_dir + '/kid_' + str(idx) + '.txt')
            log_path = os.path.join(os.getcwd(), self.log_dir + '/genetic_best.txt')
            shutil.copy(kid_path, log_path)
            # save genetic best seen result for every round
            old_path = os.path.join(os.getcwd(), self.log_dir + '/genetic_best.txt')
            new_path = os.path.join(os.getcwd(), self.log_dir + '/genetic_best_iter_' + str(round_id) + '.txt')
            shutil.copy(old_path, new_path)
        if reward > self.round_best:
            self.round_best = reward
            kid_path = os.path.join(os.getcwd(), self.kid_dir + '/kid_' + str(idx) + '.txt')
            log_path = os.path.join(os.getcwd(), self.log_dir + '/round_best_' + str(round_id) + '.txt')
            shutil.copy(kid_path, log_path)
        if self.round_worst == 0:
            self.round_worst = reward
        if reward < self.round_worst:
            self.round_worst = reward
        self.round_mean = (self.round_mean * previous_samples + reward)/(previous_samples + 1)

        # store reward for each sample
        self.reward_buffer.append(reward)

    def Evaluate(self, round_id, generations, command, load_per_sample):

        # mutate fraction decay
        decay_rate = (1 - round_id / generations) if (self.mutate_decay and generations > 0) else 1
        self.mut_frac = decay_rate * self.max_mut_frac

        self.access_act, self.wait_act, self.piece_act = [], [], []
        self.wait_info_act1, self.wait_info_act2, self.wait_info_act3 = [], [], []
        self.reward_buffer = []

        self.pops_record = []
        
        sample_buffer = []
        for parent in self.pops:
            sample_buffer.append(parent)
            for i in range(self.bfactor):
                sample_buffer.append(self.mutate(parent, decay_rate))

        def save_children_file(sample_buffer):
            if not os.path.exists(self.kid_dir):
                os.mkdir(self.kid_dir)
            base_path = os.path.join(os.getcwd(), self.kid_dir)
            for idx in range(len(sample_buffer)):
                recent_path = os.path.join(base_path, 'kid_{}.txt'.format(idx))
                if os.path.exists(recent_path):
                    os.remove(recent_path)
                sample_buffer[idx].write_to_file(recent_path)
            return

        print('There are {} children in iter {}'.format(len(sample_buffer), round_id))
        for _, child in enumerate(sample_buffer):
            self.store_actions(child.access, child.wait, child.piece, \
                               child.wait_info1, child.wait_info2, child.wait_info3)

        save_children_file(sample_buffer)
        population_res = samples_eval(command, len(sample_buffer), load_per_sample)

        fail_to_exe = 0
        for idx in range(len(sample_buffer)):
            # if the execution has failed, rollback the stored actions, continue the training
            if population_res[idx][0] == 0.0 and population_res[idx][1] == 1.0:
                print('EA child No.{}: failed to execute'.format(idx))
                self.rollback(idx, fail_to_exe)
                fail_to_exe += 1
                continue
            # otherwise, store the throughput
            self.record_reward(round_id, population_res[idx][0], idx - fail_to_exe, idx, sample_buffer[idx])
            self.pops_record.append(record(idx = idx, reward = population_res[idx][0]))
            print('EA child No.{}: reward - {}'.format(idx, population_res[idx][0]))

        self.pops = []
        self.pops_record = sorted(self.pops_record, key =lambda i: i.reward, reverse = True)
        pop_i = 0
        weakest_reward = 0
        # child selection
        if self.selection == 'truncation':
            while len(self.pops) < self.pop_size and pop_i < len(self.pops_record):
                weakest_reward = self.pops_record[pop_i].reward
                self.pops.append(sample_buffer[self.pops_record[pop_i].policy])
                pop_i += 1
        elif self.selection == 'tournament':
            tags = range(len(self.pops_record))
            used_set = set()
            to_keep = self.pop_size if len(self.pops_record) >= self.pop_size else len(self.pops_record)
            for _ in range(to_keep):
                candidates = set()
                while len(candidates) == 0:
                    candidates = set(sample(tags, to_keep)) - used_set
                winner = min(candidates)
                used_set.add(winner)
            # store selected ones, since set is in ascending order, sample_buffer[-1] will have the weakest one
            # print('Tournament winners are {}'.format(str(used_set)))
            for i in used_set:
                self.pops.append(sample_buffer[self.pops_record[i].policy])
            # tournament done
            # print('Weakest polic is no.{} with reward {}'.format(max(used_set), self.pops_record[max(used_set)].reward))
            weakest_reward = self.pops_record[max(used_set)].reward if len(used_set) !=0 else .0
        else:
            print('Unknown selection methodology for genetic')
            assert False

        if len(self.pops) == 0:
            print('Failed to select any sample into next EA generation')
            assert False

        # EA stage control
        if self.round_update == True:
            self.no_update_count = 0
        else:
            self.no_update_count += 1

        if self.no_update_count > SWITCH_STAGE_THRESHOLD:
            if self.training_stage == 0:
                # save Stage 0 result
                base_path = os.path.join(os.getcwd(), self.log_dir)
                switch_path = os.path.join(base_path, 'switch_log.txt')
                with open(switch_path, 'a+') as f:
                    f.write('Switch to Stage 1 at iter {}'.format(round_id) + '\n')
                old_path = os.path.join(base_path, 'genetic_best.txt')
                new_path = os.path.join(base_path, 'stage_0_genetic.txt')
                shutil.copy(old_path, new_path)
                print('EA changing to stage 1')
                self.training_stage = 1
                self.no_update_count = 0
            # elif self.training_stage == 1:
            #     # save Stage 1 result
            #     base_path = os.path.join(os.getcwd(), self.log_dir)
            #     switch_path = os.path.join(base_path, 'switch_log.txt')
            #     with open(switch_path, 'a+') as f:
            #         f.write('Switch to Stage 2 at iter {}'.format(round_id) + '\n')
            #     old_path = os.path.join(base_path, 'genetic_best.txt')
            #     new_path = os.path.join(base_path, 'stage_1_genetic.txt')
            #     shutil.copy(old_path, new_path)
            #     print('EA changing to stage 2')
            #     self.training_stage = 2
            #     self.no_update_count = 0
        
        return self.access_act, self.wait_act, self.piece_act, \
               self.wait_info_act1, self.wait_info_act2, self.wait_info_act3, \
               self.reward_buffer, weakest_reward, self.best_seen_sample

    def replace_weakest_sample(self, sample):
        # replace the weakest sample
        self.pops[-1] = sample
    
    def rollback(self, index, fail_to_exe):
        self.access_act = self.access_act[:(index - fail_to_exe) * ACCESSE_SPACE] + self.access_act[(index + 1 - fail_to_exe) * ACCESSE_SPACE :]
        self.wait_act = self.wait_act[:(index - fail_to_exe) * WAIT_SPACE] + self.wait_act[(index + 1 - fail_to_exe) * WAIT_SPACE :]
        self.piece_act = self.piece_act[:(index - fail_to_exe) * PIECE_SPACE] + self.piece_act[(index + 1 - fail_to_exe) * PIECE_SPACE :]
        self.wait_info_act1 = self.wait_info_act1[:(index - fail_to_exe) * WAIT_SPACE] + self.wait_info_act1[(index + 1 - fail_to_exe) * WAIT_SPACE :]
        self.wait_info_act2 = self.wait_info_act2[:(index - fail_to_exe) * WAIT_SPACE] + self.wait_info_act2[(index + 1 - fail_to_exe) * WAIT_SPACE :]
        self.wait_info_act3 = self.wait_info_act3[:(index - fail_to_exe) * WAIT_SPACE] + self.wait_info_act3[(index + 1 - fail_to_exe) * WAIT_SPACE :]
