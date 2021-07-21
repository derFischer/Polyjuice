import os
import sys
import math
import time
import shutil
import itertools
import functools
import re
import signal
import subprocess
import numpy as np

from collections import defaultdict
from random import randint, sample
import tensorflow.compat.v1 as tf
import global_var as gv

# in waitAccessInfo, WAIT_DEP_COMMIT means waiting for the specific txn to commit
WAIT_DEP_COMMIT = 99
WAIT_DEP_COMMIT_STR = 'c'

MAX_TXN_BUF_SIZE = 32
MAX_BACKOFF_BOUND_STATIC = 17
MAX_BACKOFF_BOUND_DYNAMIC = 63

REGEX_THPT = re.compile('agg_throughput\(([^)]+)\)')
REGEX_ABRT = re.compile('agg_abort_throughput\(([^)]+)\)')

def parse(return_string):
    if return_string is None: return (0.0, 0.0)
    parse_thpt = re.search(REGEX_THPT, return_string)
    parse_abrt = re.search(REGEX_ABRT, return_string)
    if parse_thpt is None or parse_abrt is None:
        return (float(.0), float(.0))
    thpt = parse_thpt.groups()[0]
    abrt = parse_abrt.groups()[0]
    return (float(thpt), float(abrt))

def parse_kid(pop_res, i_kid):
    start_pos = pop_res.find(str('./training/kids/kid_{}.txt'.format(i_kid)))
    if start_pos == -1:
        return -1
    end_pos = pop_res.find(str('./training/kids/kid_{}.txt'.format(i_kid + 1)))
    if end_pos == -1:
        end_pos = len(pop_res)
    return parse(pop_res[start_pos:end_pos])

def run(command, die_after=0):
    extra = {} if die_after == 0 else {'preexec_fn': os.setsid}
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        shell=True, **extra)
    for _ in range(die_after if die_after > 0 else 600):
        if process.poll() is not None:
            break
        time.sleep(1)

    out_code = -1 if process.poll() is None else process.returncode

    if out_code < 0:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except Exception as e:
            print('{}, but continueing'.format(e))
        assert die_after != 0, 'Should only time out with die_after set'
        print('Failed with return code {}'.format(process.returncode))
        process.stdout.flush()
        return process.stdout.read().decode('utf-8')
        # return None # Timeout
    elif out_code > 0:
        print('Failed with return code {}'.format(process.returncode))
        process.stdout.flush()
        return process.stdout.read().decode('utf-8')
        # print(process.communicate())
        # return None

    process.stdout.flush()
    return process.stdout.read().decode('utf-8')

def hash_list(target):
    hash_value = 17
    for i in target:
        hash_value = hash_value * 31 + int(i)
    return hash_value

class State(object):
    def __init__(self, low_contention_state, high_contention_state, txn_buf_size):
        self._low_contention_state = low_contention_state
        self._high_contention_state = high_contention_state
        self._txn_buf_size = txn_buf_size
        # hash caculation
        self._hash = 17
        self._hash = self._hash * 31 + hash(self._low_contention_state)
        self._hash = self._hash * 31 + hash(self._high_contention_state)
        self._hash = self._hash * 31 + hash(self._txn_buf_size)

    @classmethod
    def occ(cls):
        return cls(SubState.occ(), SubState.occ(), 1)

    @classmethod
    def waitCommit(cls):
        return cls(SubState.waitCommit(), SubState.waitCommit(), 32)

    # pick up a learned policy and continue learning
    def pick_up(self, policy_path):
        with open(policy_path, 'r') as f:
            # txn buffer size
            f.readline()
            txn_buf_size_str = f.readline()

            # low contention state
            f.readline()
            self._low_contention_state.pick_up(f)

            # high contention state
            f.readline()
            self._high_contention_state.pick_up(f)

        self._txn_buf_size = int(txn_buf_size_str)

        # hash caculation
        self._hash = 17
        self._hash = self._hash * 31 + hash(self._low_contention_state)
        self._hash = self._hash * 31 + hash(self._high_contention_state)
        self._hash = self._hash * 31 + hash(self._txn_buf_size)
        return

    def write_to_file(self, f):
        f.write("txn buffer size:\n")
        f.write(str(self._txn_buf_size))
        f.write("\n")
        f.write("------------------------low contention------------------------\n")
        self._low_contention_state.write_to_file(f)
        f.write("------------------------high contention------------------------\n")
        self._high_contention_state.write_to_file(f)

    @property
    def txn_buf_size(self):
        return self._txn_buf_size

    @property
    def low_contention_state(self):
        return self._low_contention_state

    @property
    def high_contention_state(self):
        return self._high_contention_state

    def __str__(self):
        return str(self._txn_buf_size) + '\n' + \
               str('low contention part') + '\n' + \
               str(self._low_contention_state) + '\n' + \
               str('high contention part') + '\n' + \
               str(self._high_contention_state)

    def __len__(self):
        return len(self._txn_buf_size) + \
               len(self._low_contention_state) + \
               len(self._high_contention_state)

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return self._txn_buf_size == other._txn_buf_size and \
                self._low_contention_state == other._low_contention_state and \
                self._high_contention_state == other._high_contention_state


class SubState(object):
    """
        Note:
        access_inf - access inference, occ or ic3
        length: ACCESSES
                agent_clean_private = 0,
                agent_dirty_public = 1,
        wait_inf - wait inference, noWait or waitCommit ot waitAccess
        length: ACCESSES
                agent_noWait = 2
                agent_waitAccess = 3
        wait_acc_info - waitAccess info (which target internal access it should wait for), 0 denotes for noWait:
        length: ACCESSES * TXN_TYPE
            sceranio - access_0 depend on type1
            sceranio - access_0 depend on type2
            sceranio - access_1 depend on type1
    """
    def __init__(self, 
                access_inf, 
                wait_inf, 
                before_commit_piece_wait_inf, 
                wait_acc_info, 
                piece_end_info, 
                final_commit_wait_decisions, 
                final_commit_wait_access_info, 
                const_backoff_bound, 
                backoff_bound):
        self._access_inf = list(access_inf)
        self._wait_inf = list(wait_inf)
        self._before_commit_piece_wait_inf = list(before_commit_piece_wait_inf)
        self._wait_acc_info = list(wait_acc_info)
        self._piece_end_info = list(piece_end_info)
        self._final_commit_wait_decisions = list(final_commit_wait_decisions)
        self._final_commit_wait_access_info = list(final_commit_wait_access_info)
        self._const_backoff_bound = list(const_backoff_bound)
        self._backoff_bound = list(backoff_bound)

        self.align_standard_algo()

        # hash caculation
        self._hash = 17
        self._hash = self._hash * 31 + hash(tuple(self._access_inf))
        self._hash = self._hash * 31 + hash(tuple(self._wait_inf))
        self._hash = self._hash * 31 + hash(tuple(self._before_commit_piece_wait_inf))
        self._hash = self._hash * 31 + hash(tuple(self._wait_acc_info))
        self._hash = self._hash * 31 + hash(tuple(self._piece_end_info))
        self._hash = self._hash * 31 + hash(tuple(self._final_commit_wait_decisions))
        self._hash = self._hash * 31 + hash(tuple(self._final_commit_wait_access_info))
        self._hash = self._hash * 31 + hash_list(tuple(self._const_backoff_bound))
        self._hash = self._hash * 31 + hash_list(tuple(self._backoff_bound))

    @classmethod
    def occ(cls):
        return cls([0] * gv.ACCESSES, 
                [2] * gv.ACCESSES, 
                [2] * gv.ACCESSES, 
                [0] * gv.ACCESSES * gv.TXN_TYPE, 
                [0] * gv.ACCESSES, 
                [2] * gv.TXN_TYPE, 
                [0] * gv.TXN_TYPE * gv.TXN_TYPE, 
                [0] * gv.TXN_TYPE,
                [MAX_BACKOFF_BOUND_DYNAMIC] * gv.TXN_TYPE)

    @classmethod
    def waitCommit(cls):
        return cls([1] * gv.ACCESSES, 
                [3] * gv.ACCESSES, 
                [3] * gv.ACCESSES, 
                [WAIT_DEP_COMMIT] * gv.ACCESSES * gv.TXN_TYPE, 
                [0] * gv.ACCESSES, 
                [3] * gv.TXN_TYPE, 
                [1] * gv.TXN_TYPE * gv.TXN_TYPE, 
                [0] * gv.TXN_TYPE,
                [MAX_BACKOFF_BOUND_DYNAMIC] * gv.TXN_TYPE)

    def align_standard_algo(self):
        for idx in range(gv.TXN_TYPE):
            self._const_backoff_bound[idx] = 0
        for idx in range(gv.ACCESSES):
            if idx not in gv.READ_ACCS:
                self._access_inf[idx] = 1

    def pick_up(self, f):
        # acc_inf
        f.readline()
        line_acc_inf = f.readline()
        self._access_inf = list([int(line_acc_inf[idx]) for idx in range(gv.ACCESSES)])
        
        # wait_inf
        f.readline()
        line_wait_inf = f.readline()
        self._wait_inf = list([int(line_wait_inf[idx]) for idx in range(gv.ACCESSES)])

        # before_commit_piece_wait_inf
        f.readline()
        line_before_commit_piece_wait_inf = f.readline()
        self._before_commit_piece_wait_inf = list([int(line_before_commit_piece_wait_inf[idx]) for idx in range(gv.ACCESSES)])
        
        # wait_acc_info
        f.readline()
        line_wait_acc_info = f.readline()
        self._wait_acc_info = list([WAIT_DEP_COMMIT if i == 'c' else int(i) for i in line_wait_acc_info.replace('\n', '').split(' ')])

        # piece_end_inf
        f.readline()
        line_piece_end_inf = f.readline()
        self._piece_end_info = list([int(line_piece_end_inf[idx]) for idx in range(gv.ACCESSES)])
        
        # final_commit_wait_decisions
        f.readline()
        line_final_commit_wait_decisions = f.readline()
        self._final_commit_wait_decisions = list([int(line_final_commit_wait_decisions[idx]) for idx in range(gv.TXN_TYPE)])
        
        # final_commit_wait_access_info
        f.readline()
        line_final_commit_wait_access_info = f.readline()
        self._final_commit_wait_access_info = list([WAIT_DEP_COMMIT if i == 'c' else int(i) for i in line_final_commit_wait_access_info.replace('\n', '').split(' ')])

        # different txn type's const backoff bound
        f.readline()
        const_backoff_bound_str = f.readline()
        self._const_backoff_bound = list([int(const_backoff_bound_str[idx]) for idx in range(gv.TXN_TYPE)])

        # different txn type's backoff bound
        f.readline()
        backoff_bound_str = f.readline()
        self._backoff_bound = list([int(i) for i in backoff_bound_str.replace('\n', '').split(' ')])

        self.align_standard_algo()

        # hash caculation
        self._hash = 17
        self._hash = self._hash * 31 + hash(tuple(self._access_inf))
        self._hash = self._hash * 31 + hash(tuple(self._wait_inf))
        self._hash = self._hash * 31 + hash(tuple(self._before_commit_piece_wait_inf))
        self._hash = self._hash * 31 + hash(tuple(self._wait_acc_info))
        self._hash = self._hash * 31 + hash(tuple(self._piece_end_info))
        self._hash = self._hash * 31 + hash(tuple(self._final_commit_wait_decisions))
        self._hash = self._hash * 31 + hash(tuple(self._final_commit_wait_access_info))
        self._hash = self._hash * 31 + hash_list(tuple(self._const_backoff_bound))
        self._hash = self._hash * 31 + hash_list(tuple(self._backoff_bound))
        return

    def write_to_file(self, f):
        f.write("access decision:\n")
        f.writelines([str(i) for i in self._access_inf])
        f.write("\n")
        f.write("wait decisions(before access):\n")
        f.writelines([str(i) for i in self._wait_inf])
        f.write("\n")
        f.write("wait decisions(before commit piece):\n")
        f.writelines([str(i) for i in self._before_commit_piece_wait_inf])
        f.write("\n")
        f.write("wait access info(txn execution):\n")
        f.writelines(str(' '.join([(WAIT_DEP_COMMIT_STR if i == WAIT_DEP_COMMIT else str(i)) for i in self._wait_acc_info])))
        f.write("\n")
        f.write("piece end decision:\n")
        f.writelines([str(i) for i in self._piece_end_info])
        f.write("\n")
        f.write("wait decisions(txn final commit):\n")
        f.writelines([str(i) for i in self._final_commit_wait_decisions])
        f.write("\n")
        f.write("wait access info(txn final commit):\n")
        f.writelines(str(' '.join([(WAIT_DEP_COMMIT_STR if i == WAIT_DEP_COMMIT else str(i)) for i in self._final_commit_wait_access_info])))
        f.write("\n")
        f.write("whether use const backoff bound:\n")
        f.writelines([str(i) for i in self._const_backoff_bound])
        f.write("\n")
        f.write("different txn type's backoff bound:\n")
        f.writelines(str(' '.join(self.backoff_bound)))
        f.write("\n")

    @property
    def access_inf(self):
        return self._access_inf

    @property
    def wait_inf(self):
        return self._wait_inf

    @property
    def before_commit_piece_wait_inf(self):
        return self._before_commit_piece_wait_inf

    @property
    def wait_acc_info(self):
        return self._wait_acc_info

    @property
    def piece_end_info(self):
        return self._piece_end_info
    
    @property
    def final_commit_wait_decisions(self):
        return self._final_commit_wait_decisions
    
    @property
    def final_commit_wait_access_info(self):
        return self._final_commit_wait_access_info

    @property
    def const_backoff_bound(self):
        return list([str(i) for i in self._const_backoff_bound])

    @property
    def backoff_bound(self):
        return list([str(i) for i in self._backoff_bound])

    @property
    def np_access_inf(self):
        return np.array(self._access_inf, dtype=int)

    @property
    def np_wait_inf(self):
        return np.array(self._wait_inf, dtype=int)

    @property
    def np_before_commit_piece_wait_inf(self):
        return np.array(self._before_commit_piece_wait_inf, dtype=int)

    @property
    def np_wait_acc_info(self):
        return np.array(self._wait_acc_info, dtype=int)

    @property
    def np_piece_end_info(self):
        return np.array(self._piece_end_info, dtype=int)
    
    @property
    def np_final_commit_wait_decisions(self):
        return np.array(self._final_commit_wait_decisions, dtype=int)
    
    @property
    def np_final_commit_wait_access_info(self):
        return np.array(self._final_commit_wait_access_info, dtype=int)

    @property
    def np_const_backoff_bound(self):
        return np.array(self._const_backoff_bound, dtype=int)
        
    @property
    def np_backoff_bound(self):
        return np.array(self._backoff_bound, dtype=int)

    def __str__(self):
        return str('_access_inf:') + str(self._access_inf) + '\n' + \
               str('_wait_inf:') + str(self._wait_inf) + '\n' + \
               str('_before_commit_piece_wait_inf:') + str(self._before_commit_piece_wait_inf) + '\n' + \
               str('_wait_acc_info:') + str(self._wait_acc_info) + '\n' + \
               str('_piece_end_info:') + str(self._piece_end_info) + '\n' + \
               str('_final_commit_wait_access_info:') + str(self._final_commit_wait_access_info) + '\n' + \
               str('_final_commit_wait_access_info:') + str(self._final_commit_wait_access_info) + '\n' + \
               str('_const_backoff_bound:') + str(self._const_backoff_bound) + '\n' + \
               str('_backoff_bound:') + str(self._backoff_bound)

    def __len__(self):
        return len(self._access_inf) + \
               len(self._wait_inf) + \
               len(self._before_commit_piece_wait_inf) + \
               len(self._wait_acc_info) + \
               len(self._piece_end_info) + \
               len(self._final_commit_wait_decisions) + \
               len(self._final_commit_wait_access_info) + \
               len(self._const_backoff_bound) + \
               len(self._backoff_bound)

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return self._access_inf == other._access_inf and \
                self._wait_inf == other._wait_inf and \
                self._before_commit_piece_wait_inf == other._before_commit_piece_wait_inf and \
                self._wait_acc_info == other._wait_acc_info and \
                self._piece_end_info == other._piece_end_info and \
                self._final_commit_wait_decisions == other._final_commit_wait_decisions and \
                self._final_commit_wait_access_info == other._final_commit_wait_access_info and \
                self._const_backoff_bound == other._const_backoff_bound and \
                self._backoff_bound == other._backoff_bound


class StateCache(object):
    def __init__(self, keep_cache, min_ep):
        self._min_ep = min_ep
        self._keep_cache = keep_cache
        self._newitem = lambda: {'count': 0, 'average': 0}
        self._cache = defaultdict(self._newitem)

    # Longer an item has survived in the population, the smaller change there
    # is to re-measure it.
    def run_or_query(self, key, f, is_lambda):
        item = self._cache[key]
        if item['count'] < self._min_ep or np.random.rand() < 1./item['count']:
            if is_lambda:
                new_val = f()
            else:
                new_val = f
            item['count'] += 1
            item['average'] = item['average'] + (new_val - item['average'])/item['count']
        return item['average']

    def evict(self, keys):
        for key in (keys if self._keep_cache else self._cache.keys()):
            self._cache[key] = self._newitem()


def mutate_sub(state, percentage, decay_rate, allow_all_mut):
    # mution of access_inf: candidate {0, 1}
    mut_access_inf = (np.random.rand(len(state.access_inf)) < percentage).astype(int)
    mut_pace_access_inf = (np.random.rand(len(state.access_inf)) > 0.0).astype(int)
    after_access_inf = (state.np_access_inf + mut_access_inf * mut_pace_access_inf) % 2
    if not allow_all_mut:
        after_access_inf = state.np_access_inf

    # mution of wait_inf: candidate {2, 3}
    mut_wait_inf = (np.random.rand(len(state.wait_inf)) < percentage).astype(int)
    mut_pace_wait_inf = (np.random.rand(len(state.wait_inf)) > 0.0).astype(int)
    after_wait_inf = (state.np_wait_inf + mut_wait_inf * mut_pace_wait_inf - 2) % 2 + 2

    # mution of before_commit_piece_wait_inf: candidate {2, 3}
    mut_before_commit_piece_wait_inf = (np.random.rand(len(state.before_commit_piece_wait_inf)) < percentage).astype(int)
    mut_pace_before_commit_piece_wait_inf = (np.random.rand(len(state.before_commit_piece_wait_inf)) > 0.0).astype(int)
    after_before_commit_piece_wait_inf = (state.np_before_commit_piece_wait_inf + mut_before_commit_piece_wait_inf * mut_pace_before_commit_piece_wait_inf - 2) % 2 + 2

    # mution of wait_acc_info
    # 0 is also a candidate, in spite of it will make no difference w/ noWait
    # WAIT_DEP_COMMIT is also a candidate
    ingredients = []
    wait_acc_info = state.np_wait_acc_info
    for i in range(gv.TXN_TYPE):
        single_type_info = []
        for j in range(gv.ACCESSES):
            single_type_info.append(wait_acc_info[j * gv.TXN_TYPE + i])
        # Change form from WAIT_DEP_COMMIT into gv.TXN_ACCESS_NUM[gv.TXN_TYPE] + 1
        for k in range(len(single_type_info)):
            if single_type_info[k] == WAIT_DEP_COMMIT:
                single_type_info[k] = gv.TXN_ACCESS_NUM[i + 1]
        mut_single_type_info = (np.random.rand(len(single_type_info)) < percentage).astype(int)
        # step mutation width
        step_width = int(decay_rate * gv.TXN_ACCESS_NUM[i + 1])
        mut_pace_single_type_info = ((np.random.rand(len(single_type_info)) * step_width) + 1).astype(int) * ((np.random.rand(len(single_type_info)) < .5) * 2 - 1).astype(int)
        # do mutation
        after_single_type_info = (single_type_info + mut_single_type_info * mut_pace_single_type_info) % (gv.TXN_ACCESS_NUM[i + 1] + 1)
        # # Change form from gv.TXN_ACCESS_NUM[gv.TXN_TYPE] + 1 into WAIT_DEP_COMMIT
        # for k in range(len(after_single_type_info)):
        #     if after_single_type_info[k] == (gv.TXN_ACCESS_NUM[i + 1] + 1):
        #         after_single_type_info[k] = WAIT_DEP_COMMIT
        ingredients.append(after_single_type_info)
    after_wait_acc_info = []
    for i in range(gv.ACCESSES):
        for j in range(gv.TXN_TYPE):
            after_wait_acc_info.append(ingredients[j][i])
    # rollback some of the mutation - noWait & waitCommit do not mutate on related waitAccessInfo
    for i in range(gv.ACCESSES):
        if after_wait_inf[i] != 3 and after_before_commit_piece_wait_inf[i] != 3:
            for j in range(gv.TXN_TYPE): 
                after_wait_acc_info[i * gv.TXN_TYPE + j] = wait_acc_info[i * gv.TXN_TYPE + j]

    # mution of piece end information
    mut_piece_end_inf = (np.random.rand(len(state.piece_end_info)) < percentage).astype(int)
    mut_pace_piece_end_inf = (np.random.rand(len(state.piece_end_info)) > 0.0).astype(int)
    after_piece_end_inf = (state.np_piece_end_info + mut_piece_end_inf * mut_pace_piece_end_inf) % 2

    # mution of final_commit_wait_decisions
    mut_final_commit_wait_decisions_inf = (np.random.rand(len(state.final_commit_wait_decisions)) < percentage).astype(int)
    mut_pace_final_commit_wait_decisions_inf = (np.random.rand(len(state.final_commit_wait_decisions)) > 0.0).astype(int)
    after_final_commit_wait_decisions = (state.np_final_commit_wait_decisions + mut_final_commit_wait_decisions_inf * mut_pace_final_commit_wait_decisions_inf - 2) % 2 + 2

    # mution of final_commit_wait_access_info
    # 0 is also a candidate, in spite of it will make no difference w/ noWait
    # WAIT_DEP_COMMIT is also a candidate
    ingredients = []
    final_commit_wait_access_info = state.np_final_commit_wait_access_info
    for i in range(gv.TXN_TYPE):
        single_type_info = []
        for j in range(gv.TXN_TYPE):
            single_type_info.append(final_commit_wait_access_info[j * gv.TXN_TYPE + i])
        # Change form from WAIT_DEP_COMMIT into gv.TXN_ACCESS_NUM[gv.TXN_TYPE] + 1
        for k in range(len(single_type_info)):
            if single_type_info[k] == WAIT_DEP_COMMIT:
                single_type_info[k] = gv.TXN_ACCESS_NUM[i + 1]
        mut_single_type_info = (np.random.rand(len(single_type_info)) < percentage).astype(int)
        # step mutation width
        step_width = int(decay_rate * gv.TXN_ACCESS_NUM[i + 1])
        mut_pace_single_type_info = ((np.random.rand(len(single_type_info)) * step_width) + 1).astype(int) * ((np.random.rand(len(single_type_info)) < .5) * 2 - 1).astype(int)
        # do mutation
        after_single_type_info = (single_type_info + mut_single_type_info * mut_pace_single_type_info) % (gv.TXN_ACCESS_NUM[i + 1] + 1)
        # # Change form from gv.TXN_ACCESS_NUM[gv.TXN_TYPE] + 1 into WAIT_DEP_COMMIT
        # for k in range(len(after_single_type_info)):
        #     if after_single_type_info[k] == (gv.TXN_ACCESS_NUM[i + 1] + 1):
        #         after_single_type_info[k] = WAIT_DEP_COMMIT
        ingredients.append(after_single_type_info)
    after_final_commit_wait_access_info = []
    for i in range(gv.TXN_TYPE):
        for j in range(gv.TXN_TYPE):
            after_final_commit_wait_access_info.append(ingredients[j][i])
    # rollback some of the mutation - noWait & waitCommit do not mutate on related waitAccessInfo
    for i in range(gv.TXN_TYPE):
        if after_final_commit_wait_decisions[i] != 3:
            for j in range(gv.TXN_TYPE): 
                after_final_commit_wait_access_info[i * gv.TXN_TYPE + j] = final_commit_wait_access_info[i * gv.TXN_TYPE + j]

    # mution of const_backoff_bound 2 bool candidates: {0, 1}
    mut_const_backoff_bound = (np.random.rand(len(state.const_backoff_bound)) < percentage).astype(int)
    mut_pace_const_backoff_bound = (np.random.rand(len(state.const_backoff_bound)) > 0.0).astype(int)
    after_const_backoff_bound = (state.np_const_backoff_bound + mut_const_backoff_bound * mut_pace_const_backoff_bound) % 2

    # assign backoff step width as 16
    step_width = int(16)
    mut_backoff_pace = ((np.random.rand(len(state.backoff_bound)) * step_width) + 1).astype(int) * ((np.random.rand(1) < .5) * 2 - 1).astype(int)
    # mution of backoff_bound, int [1, MAX_BACKOFF_BOUND_DYNAMIC]
    mut_backoff_bound = (np.random.rand(len(state.backoff_bound)) < percentage).astype(int)
    after_backoff_bound = (state.np_backoff_bound + mut_backoff_bound * mut_backoff_pace - 1) % MAX_BACKOFF_BOUND_DYNAMIC + 1

    # when using const_backoff_bound, backoff value should not exceed MAX_BACKOFF_BOUND_STATIC
    for i in range(gv.TXN_TYPE):
        if after_const_backoff_bound[i] == 1:
            after_backoff_bound[i] = (after_backoff_bound[i] - 1) % MAX_BACKOFF_BOUND_STATIC + 1

    if not allow_all_mut:
        return SubState(state.np_access_inf, 
                        after_wait_inf, 
                        after_before_commit_piece_wait_inf, 
                        after_wait_acc_info, 
                        state.np_piece_end_info, 
                        after_final_commit_wait_decisions, 
                        after_final_commit_wait_access_info, 
                        after_const_backoff_bound, 
                        after_backoff_bound)
    else:
        return SubState(after_access_inf, 
                        after_wait_inf, 
                        after_before_commit_piece_wait_inf, 
                        after_wait_acc_info, 
                        after_piece_end_inf, 
                        after_final_commit_wait_decisions, 
                        after_final_commit_wait_access_info, 
                        after_const_backoff_bound, 
                        after_backoff_bound)


def mutate(state, percentage, decay_rate, stage=0):
    """
        Note:
        Stage0: do waiting, backoff, txn_buf_size mutation
        Stage1: do access, piece mutation
        Stage2: do different contention level mutation
    """
    allow_all_mut = (True if stage != 0 else False)
    after_low = mutate_sub(state.low_contention_state, percentage, decay_rate, allow_all_mut)
    if stage == 2:
        after_high = mutate_sub(state.high_contention_state, percentage, decay_rate, allow_all_mut)
    else:
        after_high = after_low

    # assign txn_buf_size step width as 8
    step_width = int(8)
    mut_txn_buf_pace = ((np.random.rand(1) * step_width) + 1).astype(int) * ((np.random.rand(1) < .5) * 2 - 1).astype(int)
    # mution of txn_buf_size, int [1, 32]
    mut_txn_buf_size = (np.random.rand(1) < percentage).astype(int)
    if mut_txn_buf_size[0] == 1:
        after_txn_buf_size = int((state.txn_buf_size + mut_txn_buf_pace - 1).astype(int) % MAX_TXN_BUF_SIZE + 1)
    else:
        after_txn_buf_size = state.txn_buf_size

    return State(after_low, after_high, after_txn_buf_size)


def policy_crossover(state1, state2, mask):

    def mix(a, b, mask):
        result = np.zeros(len(mask), dtype=int)
        for i in range(len(mask)):
            if(mask[i] == 1):
                result[i] = a[i]
            else:
                result[i] = b[i]
        return result

    after_access_inf = mix(state1.np_access_inf, state2.np_access_inf, mask[0:gv.ACCESSES])
    after_wait_inf = mix(state1.np_wait_inf, state2.np_wait_inf, mask[gv.ACCESSES:gv.ACCESSES*2])
    after_wait_acc_info = mix(state1.np_wait_acc_info, state2.np_wait_acc_info, mask[gv.ACCESSES*2:])
    # todo - fix crossover w/ txn_buf_size & backoff_bound
    # current just keep w/ state1's content
    return State(after_access_inf, after_wait_inf, after_wait_acc_info, state1.txn_buf_size, state1.backoff_bound)


def shuf(li):
    np.random.shuffle(li)
    return li


def learn(workload_type, psize, bfactor, mutate_rate, crossover, ep_per_sample,
          mutate_decay, command, selection, use_cache=False, log_dir=None,
          kid_dir=None, log_rate=4, max_iters=0, pickup=None,
          no_default=False, load_per_pol=False):

    if workload_type == 'tpcc':
        gv.TXN_TYPE = gv.TPCC_TXN_TYPE
        gv.ACCESSES = gv.TPCC_ACCESSES
        gv.TXN_ACCESS_NUM = gv.TPCC_TXN_ACCESS_NUM
        gv.READ_ACCS = gv.TPCC_READS
    elif workload_type == 'tpce':
        gv.TXN_TYPE = gv.TPCE_TXN_TYPE
        gv.ACCESSES = gv.TPCE_ACCESSES
        gv.TXN_ACCESS_NUM = gv.TPCE_TXN_ACCESS_NUM
        gv.READ_ACCS = gv.TPCE_READS
    elif workload_type == 'ycsb':
        gv.TXN_TYPE = gv.YCSB_TXN_TYPE
        gv.ACCESSES = gv.YCSB_ACCESSES
        gv.TXN_ACCESS_NUM = gv.YCSB_TXN_ACCESS_NUM
        gv.READ_ACCS = gv.YCSB_READS
    else:
        print('There is no such workload type')
        assert False

    tf.disable_eager_execution()
    sess = tf.Session()
    if log_dir:
        assert log_rate > 0, 'must have positive log rate when logging'
        merged = tf.summary.merge_all()
        writer = tf.summary.FileWriter(log_dir, sess.graph)

    iter_ = 0
    total_samples = 0

    mutate_select = mutate_rate[0]
    mut_p = ((lambda: mutate_rate[1] * (1 - iter_/max_iters))
             if (mutate_decay and max_iters > 0) else
             (lambda: mutate_rate[1]))

    cross_prob, cross_count = crossover[0], int(crossover[1])

    best_model_throughput = 0
    cache = StateCache(use_cache, ep_per_sample)

    # population initialization
    population = []
    half_range = math.ceil((psize - 2) / 2)
    if no_default:
        if pickup is None:
            print('Pickup policy can not be null, if not using default policies for initialization')
            assert False
    else:
        population = population + [State.waitCommit()] + [mutate(State.waitCommit(), mut_p(), 1) for _ in range(half_range)]
        population = population + [State.occ()] + [mutate(State.occ(), mut_p(), 1) for _ in range(half_range)]

    # add designated policy into parents
    if pickup is not None:
        p_policy = State.occ()
        p_policy.pick_up(pickup)
        population.append(p_policy)
        population = population + [mutate(p_policy, mut_p(), 1) for _ in range(half_range)]

    def population_eval(kid_count):
        dict_res = {}
        pos = 0
        while pos < kid_count:
            command.append('--kid-start {} --kid-end {}'.format(pos, kid_count))
            sys.stdout.flush()
            run_results = run(' '.join(command), die_after=900)
            # pop the command tail which is --policy parameter
            command.pop()
            while pos < kid_count:
                kid_res = parse_kid(run_results, pos)
                if kid_res == -1:
                    dict_res[pos] = (float(0), float(0))
                    pos = pos + 1
                    break
                dict_res[pos] = kid_res
                pos = pos + 1
        return dict_res

    def kid_eval(kid_idx):
        command.append('--policy ./training/kids/kid_{}.txt'.format(kid_idx))
        sys.stdout.flush()
        run_results = parse(run(' '.join(command), die_after=180))
        print(' commit throughput {}, abort throughput {}'.format(*run_results))
        # pop the command tail which is --policy parameter
        command.pop()
        # return the commit throughput
        return run_results[0]

    def kid_save_args_file(kids_to_save):
        if not os.path.exists(kid_dir):
            os.mkdir(kid_dir)
        base_path = os.path.join(os.getcwd(), kid_dir)
        for i_kid, kid in enumerate(kids_to_save):
            recent_path = os.path.join(base_path, 'kid_{}.txt'.format(i_kid))
            if os.path.exists(recent_path):
                os.remove(recent_path)
            save_policy(recent_path, kid)
        return

    no_update_count = 0
    training_stage = 0

    for iter_ in itertools.count() if max_iters == 0 else range(max_iters):

        # calculate decay rate
        decay_rate = (1 - iter_/max_iters) if (mutate_decay and max_iters > 0) else 1

        # Population growth
        if bfactor > 0:
            # Random walk
            kids = [mutate(parent, mut_p(), decay_rate, training_stage) for parent in population
                    for _ in range(bfactor)]
            # also keep the previous population is this round of iteration
            # maybe it will give them possibility to generate some better kid policy
            kids = list(set(kids)) + population
        else:
            # Mutate + crossover: a more traditional genetic algorithm
            mutated = set(
                [mutate(parent, mut_p(), decay_rate, training_stage) for parent in population
                 if np.random.rand() < mutate_select] + population)
            pop = mutated
            for a, b in itertools.combinations(mutated, 2):
                if a == b:
                    continue
                if np.random.rand() < cross_prob:
                    # a, b = a.np, b.np
                    points = np.random.choice(range(State.size), size=cross_count)
                    mask = np.zeros(State.size, dtype=int)
                    # Make mask of the form: [1,1,1,0,0,0,0,0,0,1,1,0,0,1,1,1,1]
                    # with cross_count change points
                    for p in points:
                        m = np.zeros(State.size, dtype=int)
                        m[:p] = 1
                        mask ^= m
                    if np.random.rand() < .5:
                        pop.add(policy_crossover(a, b, mask))
                    else:
                        pop.add(policy_crossover(b, a, mask))
            kids = list(pop)

        print("There are {} unique kids in iter {}".format(len(kids), iter_))
        # Save kids as parameter files
        kid_save_args_file(kids)

        # Measure each in the grown population, and update their tracked
        # values incase we have seen them before
        kids_to_value = defaultdict(lambda: 0)
        for ep_sample_num in range(ep_per_sample):
            if load_per_pol:
                for i_kid, kid in enumerate(kids):
                    hash_count = (1+i_kid)*60//len(kids)
                    print("[{}{}]".format('#'*hash_count, ' '*(60 - hash_count)), end='\r')
                    kids_to_value[kid] = cache.run_or_query(kid, lambda: kid_eval(i_kid), True)
            else:
                pop_res = population_eval(len(kids))
                for i_kid, kid in enumerate(kids):
                    # hash_count = (1+i_kid)*60//len(kids)
                    # print("[{}{}]".format('#'*hash_count, ' '*(60 - hash_count)), end='\r')
                    print('kid {}, commit throughput {}, abort throughput {}'.format(i_kid, pop_res[i_kid][0], pop_res[i_kid][1]))
                    kids_to_value[kid] = cache.run_or_query(kid, pop_res[i_kid][0], False)
        print() # Finish \r line

        # Note that the number of unique kids could fall below psize even
        # though there were > psize of them as mutations can lead to
        # duplicates
        sorted_ = sorted(
            kids_to_value,
            key=lambda i: kids_to_value[i])
        to_keep = min(psize, len(kids_to_value))
        if selection == 'tournament':
            tags = range(len(sorted_))
            population.clear()
            used_set = set()
            # do tournaments
            for _ in range(to_keep):
                not_used_in_tournament = set()
                while len(not_used_in_tournament) == 0:
                    each_tournament = sample(tags, to_keep)
                    not_used_in_tournament = set(each_tournament) - used_set
                winner = max(not_used_in_tournament)
                population.append(sorted_[winner])
                used_set.add(winner)
            cache.evict(set(sorted_) - set(population))
        elif selection == 'truncation':
            population = sorted_[-to_keep:]
            cache.evict(sorted_[:-to_keep])
        else:
            assert False

        print('********** Iteration {:,} ({:,} episodes) ************'
                   .format(iter_, total_samples))
        total_samples += len(kids)

        pop_best = kids_to_value[sorted_[-1]]
        pop_mean = np.mean(list(kids_to_value.values())) # Slightly unfair to equal kids
        if iter_ % log_rate == 0:
            print("PopBest", pop_best)
            print("PopMean", pop_mean)

            if log_dir and pop_best > best_model_throughput:
                save_model(log_dir, sorted_[-1], 'best')
                save_model(log_dir, sorted_[-1], 'best_' + str(iter_))
                best_model_throughput = pop_best
                no_update_count = 0
            else:
                no_update_count = no_update_count + 1
                if no_update_count == 10 and training_stage == 0:
                    print('Change to Stage 1!')
                    training_stage = 1
                    no_update_count = 0
                    # save Stage 0 result
                    base_path = os.path.join(os.getcwd(), log_dir)
                    old_path = os.path.join(base_path, 'best_new.txt')
                    new_path = os.path.join(base_path, 'stage_0.txt')
                    shutil.copy(old_path, new_path)
                elif no_update_count == 10 and training_stage == 1:
                    print('Change to Stage 2!')
                    training_stage = 2
                    no_update_count = 0
                    # save Stage 1 result
                    base_path = os.path.join(os.getcwd(), log_dir)
                    old_path = os.path.join(base_path, 'best_new.txt')
                    new_path = os.path.join(base_path, 'stage_1.txt')
                    shutil.copy(old_path, new_path)

        if log_dir and iter_ % log_rate == 0:
            sc = tf.Summary()
            sc.value.add(tag='population-best',
                         simple_value=pop_best)
            sc.value.add(tag='population-mean',
                         simple_value=pop_mean)
            sc.value.add(tag='best-seen',
                         simple_value=best_model_throughput)
            writer.add_summary(sc, iter_)
            writer.flush()

            if iter_ % (10*log_rate) == 0:
                save_model(log_dir, sorted_[-1], 'conv')


def save_model(log_dir, policy, name):
    print('Saving model!!!!!!!')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    base_path = os.path.join(os.getcwd(), log_dir)
    recent_path = os.path.join(base_path, '{}_new.txt'.format(name))
    last_path = os.path.join(base_path, '{}_old.txt'.format(name))
    if os.path.exists(last_path):
        os.remove(last_path)
    if os.path.exists(recent_path):
        os.rename(recent_path, last_path)
    save_policy(recent_path, policy)
    print('Model saved in path: {}'.format(recent_path))


def save_policy(path, policy):
    with open(path, 'w') as f:
        policy.write_to_file(f)