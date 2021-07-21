#coding=utf-8

import numpy as np
import tensorflow as tf
import os
import sys
import time
import shutil
import re
import signal
import subprocess
import numpy as np
import math
from Policy import *
np.set_printoptions(threshold=np.inf)

BASELINES = 1
HIGH_LR_LEASE = 5

class MultiBaseline:
    def __init__(self, baseline_number):
        self.baseline_number = baseline_number
        self.baselines = [Baseline() for _ in range(baseline_number)]

        self.reward_signal_access, self.reward_signal_wait, self.reward_signal_piece = [], [], []
        self.reward_signal_wait_info1 = []

    def __str__(self):
        stri = ''
        for i in range(self.baseline_number):
            stri = stri + 'baseline number ' + str(i) + ' has reward ' + str(self.baselines[i].reward) + '\n'
            stri = stri + str(self.baselines[i].sample) + '\n'
        return stri

    def insert_baseline(self, baseline):
        if baseline > self.baselines[0]:
            self.baselines[0].SetSampleWithAnotherBaseline(baseline)
            self.baselines.sort()

    def store_reward_signal(self, result):
        self.reward_signal_access.extend(result[0])
        self.reward_signal_wait.extend(result[1])
        self.reward_signal_piece.extend(result[2])
        self.reward_signal_wait_info1.extend(result[3])

    def samples_different_action(self, access, wait, piece, waitinfo1):
        # get a all True form
        result = Sample.default_different_action()
        # get different actions
        for j in range(self.baseline_number):
            diff = self.baselines[j].different_action(\
                                access, wait, piece, waitinfo1)
            for i in range(len(result)):
                result[i] = result[i] & np.array(diff[i])
        self.store_reward_signal(result)

    def get_ratio(self, avg_reward):
        rewards = []
        for i in range(self.baseline_number):
            reward_ = self.baselines[i].reward - avg_reward
            if reward_ > 0:
                rewards.append(reward_)
            else:
                rewards.append(0)
        rewards = np.array(rewards)
        if np.sum(rewards) == 0:
            return [1 / self.baseline_number] * self.baseline_number
        else:
            return rewards / np.sum(rewards)

    def calculate_reward(self, reward):

        ratio = self.get_ratio(np.mean(reward))

        access_rs, wait_rs, piece_rs = \
        [0] * (len(reward) * ACCESSE_SPACE), [0] * (len(reward) * WAIT_SPACE), [0] * (len(reward) * PIECE_SPACE)

        waitinfo1_rs = \
        [0] * (len(reward) * WAIT_SPACE)

        for i in range(self.baseline_number):

            # calculate discount_reward for each slot
            access_dr, wait_dr, piece_dr = [], [], []
            waitinfo1_dr = []
            for j in range(len(reward)):
                for _ in range(ACCESSE_SPACE):
                    access_dr.append(reward[j])
                for _ in range(PIECE_SPACE):
                    piece_dr.append(reward[j])
                for _ in range(WAIT_SPACE):
                    wait_dr.append(reward[j])
                    waitinfo1_dr.append(reward[j])
            
            access_dr = np.array(access_dr) - self.baselines[i].reward
            wait_dr = np.array(wait_dr) - self.baselines[i].reward
            piece_dr = np.array(piece_dr) - self.baselines[i].reward
            waitinfo1_dr = np.array(waitinfo1_dr) - self.baselines[i].reward

            access_rs = access_rs + ratio[i] * access_dr * ((access_dr > 0) | self.reward_signal_access)
            wait_rs = wait_rs + ratio[i] * wait_dr * ((wait_dr > 0) | self.reward_signal_wait)
            piece_rs = piece_rs + ratio[i] * piece_dr * ((piece_dr > 0) | self.reward_signal_piece)
            waitinfo1_rs = waitinfo1_rs + ratio[i] * waitinfo1_dr * ((waitinfo1_dr > 0) | self.reward_signal_wait_info1)

        return access_rs, wait_rs, piece_rs, waitinfo1_rs

    def clear_signal(self):
        self.reward_signal_access, self.reward_signal_wait, self.reward_signal_piece = [], [], []
        self.reward_signal_wait_info1 = []

            
class Baseline:
    def __init__(self, access = [], wait = [], piece = [], \
                 waitinfo1 = [],  \
                 reward = 0):
        if access == []:
            self.set = False
        else:
            self.set = True

        self.sample = Sample(access, wait, piece, waitinfo1)
        self.reward = reward

    def setSample(self, access, wait, piece, waitinfo1, reward):
        self.set = True
        self.sample.set_sample(access, wait, piece, waitinfo1, )
        self.reward = reward

    def SetSampleWithAnotherBaseline(self, baseline):
        self.setSample(baseline.sample.access, baseline.sample.wait, baseline.sample.piece, \
                       baseline.sample.wait_info1, \
                       baseline.reward)

    def __lt__(self, r):
        return self.reward < r.reward

    def different_action(self, access, wait, piece, waitinfo1):
        if self.set == False:
            return Sample.default_different_action()

        return self.sample.different_action(access, wait, piece, \
                                            waitinfo1)

class PolicyGradient:
    # initialize
    def __init__(self, log_dir, kid_dir, learning_rate,rd,output_graph=False):
        self.log_dir = log_dir
        self.kid_dir = kid_dir
        self.lr = learning_rate
        self.reward_decay = rd
        self.best_seen = 0
        self.round_best = 0
        self.round_mean = 0
        self.round_worst = 0
        self.round_std = 0

        self.round_best_sample = None
        self.best_seen_sample = None

        self.baselines = MultiBaseline(BASELINES)
        self.high_lr_lease = 0

        # to store observations, actions and corresponding rewards

        self.access_p, self.wait_p, self.piece_p = [], [], []
        self.wait_info1_p = []

        self.ep_access_rs, self.ep_wait_rs, self.ep_piece_rs = [], [], []
        self.ep_waitinfo1_rs = []

        self.ep_access_act, self.ep_wait_act, self.ep_piece_act = [], [], []
        self.ep_wait_info_act1 = []

        self.samples_count = 0

        self.policy = Policy()
        self._build_net()

        self.sess = tf.Session()

        if output_graph:
            tf.summary.FileWriter("logs/", self.sess.graph)

        self.sess.run(tf.global_variables_initializer())
        self.update_policy()

    def clear_round_info(self):
        self.round_best = 0
        self.round_mean = 0
        self.round_worst = 0
        self.round_std = 0
        self.round_best_sample = None

    def _build_net(self):
        with tf.name_scope('inputs'):
            self.tf_access_vt = tf.placeholder(tf.float32, [None, ], name="access_value")
            self.tf_wait_vt = tf.placeholder(tf.float32, [None, ], name="wait_value")
            self.tf_piece_vt = tf.placeholder(tf.float32, [None, ], name="piece_value")
            self.tf_wait_info_vt1 = tf.placeholder(tf.float32, [None, ], name="wait_info_value1")

            self.tf_access_act = tf.placeholder(tf.int32, [None, ], name="access_act")
            self.tf_wait_act = tf.placeholder(tf.int32, [None, ], name="wait_act")
            self.tf_piece_act = tf.placeholder(tf.int32, [None, ], name="piece_act")
            self.tf_wait_info_act1 = tf.placeholder(tf.int32, [None, ], name="wait_info_act1")

            self.tf_samples_count = tf.placeholder(tf.float32, name='samples_count')
            self.learning_rate = tf.placeholder(tf.float32, name='learning_rate')
        
        self.access_action = tf.nn.softmax(tf.Variable(tf.random_normal(shape=[ACCESSE_SPACE, 2], mean=0, stddev=1), name='access_action'), axis = 1)
        self.wait_action = tf.nn.softmax(tf.Variable(tf.random_normal(shape=[WAIT_SPACE, 2], mean=0, stddev=1), name='wait_action'), axis = 1)
        self.piece_action = tf.nn.softmax(tf.Variable(tf.random_normal(shape=[PIECE_SPACE, 2], mean=0, stddev=1), name='piece_action'), axis = 1)
        self.wait_info_action1 = tf.nn.softmax(tf.Variable(tf.random_normal(shape=[WAIT_SPACE, wait_info_act_count[0]], mean=0, stddev=1), name='wait_info_action1'), axis = 1)

        with tf.name_scope('reward'):
            # add a very small number to the probability in case of logging a very small number and then ouputting NAN
            self.access_action = tf.add(self.access_action, 0.000001)
            self.wait_action = tf.add(self.wait_action, 0.000001)
            self.piece_action = tf.add(self.piece_action, 0.000001)
            self.wait_info_action1 = tf.add(self.wait_info_action1, 0.000001)

            access_act = tf.reshape(tf.one_hot(self.tf_access_act, 2), [-1, ACCESSE_SPACE * 2])
            access_act_prob = tf.reshape((access_act * (tf.reshape(self.access_action, [ACCESSE_SPACE * 2]))), [-1 ,2])
            access_act_prob = -tf.log(tf.reduce_sum(access_act_prob, axis = 1))

            wait_act = tf.reshape(tf.one_hot(self.tf_wait_act, 2), [-1, WAIT_SPACE * 2])
            wait_act_prob = tf.reshape((wait_act * (tf.reshape(self.wait_action, [WAIT_SPACE * 2]))), [-1 ,2])
            wait_act_prob = -tf.log(tf.reduce_sum(wait_act_prob, axis = 1))

            piece_act = tf.reshape(tf.one_hot(self.tf_piece_act, 2), [-1, PIECE_SPACE * 2])
            piece_act_prob = tf.reshape((piece_act * (tf.reshape(self.piece_action, [PIECE_SPACE * 2]))), [-1 ,2])
            piece_act_prob = -tf.log(tf.reduce_sum(piece_act_prob, axis = 1))

            wait_info_act1 = tf.reshape((tf.one_hot(self.tf_wait_info_act1, wait_info_act_count[0])), [-1, WAIT_SPACE * wait_info_act_count[0]])
            wait_info_act1_prob = tf.reshape((wait_info_act1 * (tf.reshape(self.wait_info_action1, [WAIT_SPACE * wait_info_act_count[0]]))), [-1, wait_info_act_count[0]])
            wait_info_act1_prob = -tf.log(tf.reduce_sum(wait_info_act1_prob, axis = 1))


            self.reward = tf.divide(tf.reduce_sum(access_act_prob * self.tf_access_vt) + \
                                    tf.reduce_sum(piece_act_prob * self.tf_piece_vt) + \
                                    tf.reduce_sum(wait_act_prob * self.tf_wait_vt) + \
                                    tf.reduce_sum(wait_info_act1_prob * self.tf_wait_info_vt1), self.tf_samples_count)

        with tf.name_scope('train'):
            self.train_op = tf.train.GradientDescentOptimizer(learning_rate = self.learning_rate).minimize(self.reward)

    def update_policy(self):
        access_p, wait_p, piece_p, wait_info1_p = \
        self.sess.run([self.access_action, self.wait_action, self.piece_action, \
                       self.wait_info_action1])

        self.policy.set_prob(access_p, wait_p, piece_p, \
                             wait_info1_p)

    # store corresponding reward
    def record_reward(self, round_id, reward, previous_samples, idx):
        access = self.ep_access_act[previous_samples * ACCESSE_SPACE : (previous_samples + 1) * ACCESSE_SPACE]
        wait = self.ep_wait_act[previous_samples * WAIT_SPACE : (previous_samples + 1) * WAIT_SPACE]
        piece = self.ep_piece_act[previous_samples * PIECE_SPACE : (previous_samples + 1) * PIECE_SPACE]
        waitinfo1 = self.ep_wait_info_act1[previous_samples * WAIT_SPACE : (previous_samples + 1) * WAIT_SPACE]

        if reward > self.baselines.baselines[0].reward:
            baseline_ = Baseline(access, wait, piece, waitinfo1, reward)
            self.baselines.insert_baseline(baseline_)
            self.high_lr_lease = HIGH_LR_LEASE

        if reward > self.best_seen:
            self.best_seen = reward
            self.best_seen_sample = Sample(access, wait, piece, \
                                           waitinfo1)
            # save RL best seen result
            print('Update rl best seen sample - {}'.format(reward))
            kid_path = os.path.join(os.getcwd(), self.kid_dir + '/kid_' + str(idx) + '.txt')
            log_path = os.path.join(os.getcwd(), self.log_dir + '/rl_best.txt')
            shutil.copy(kid_path, log_path)
            # save RL best seen result for every round
            old_path = os.path.join(os.getcwd(), self.log_dir + '/rl_best.txt')
            new_path = os.path.join(os.getcwd(), self.log_dir + '/rl_best_iter_' + str(round_id) + '.txt')
            shutil.copy(old_path, new_path)
        if reward > self.round_best:
            self.round_best = reward
            # store round_best sample for EA future use
            self.round_best_sample = Sample(access, wait, piece, \
                                           waitinfo1)
        if self.round_worst == 0:
            self.round_worst = reward
        if reward < self.round_worst:
            self.round_worst = reward
        self.round_mean = (self.round_mean * previous_samples + reward)/(previous_samples + 1)

        # store reward for each sample
        self.ep_rs.append(reward)

    def Evaluate(self, command, round_id, samples_per_distribution, load_per_sample):

        base_path = os.path.join(os.getcwd(), self.log_dir)
        policy_path = os.path.join(base_path, 'Distribution.txt')
        with open(policy_path, 'a+') as f:
            f.write('RL at iter {}'.format(round_id) + '\n')
            f.write(str(self.policy) + '\n')
        
        self.ep_rs = []

        self.ep_access_act, self.ep_wait_act, self.ep_piece_act, \
        self.ep_wait_info_act1, \
        = self.policy.table_sample_batch(self.kid_dir, samples_per_distribution)
        
        policies_res = samples_eval(command, samples_per_distribution, load_per_sample)

        reward_ = 0
        fail_to_exe = 0
        for idx in range(samples_per_distribution):
            # if the execution has failed, rollback the ep_obs and ep_as, continue the training
            if policies_res[idx][0] == 0.0 and policies_res[idx][1] == 1.0:
                print("continue")
                self.rollback(idx, fail_to_exe)
                fail_to_exe += 1
                continue
            print("RL sample:" + str(idx) + " throughput:" + str(policies_res[idx][0]))
            self.record_reward(round_id, policies_res[idx][0], idx - fail_to_exe, idx)

    def set_baseline(self, access, wait, piece, \
                     wait_info1,  \
                     reward_buffer):
        samples = int(len(access) / ACCESSE_SPACE)

        for i in range(samples):
            r = reward_buffer[i]
            if r > self.baselines.baselines[0].reward:
                access_t = access[i * ACCESSE_SPACE : (i + 1) * ACCESSE_SPACE]
                wait_t = wait[i * WAIT_SPACE : (i + 1) * WAIT_SPACE]
                piece_t = piece[i * PIECE_SPACE : (i + 1) * PIECE_SPACE]
                waitinfo1_t = wait_info1[i * WAIT_SPACE : (i + 1) * WAIT_SPACE]
                baseline_ = Baseline(access_t, wait_t, piece_t, \
                                     waitinfo1_t, \
                                     r)
                self.baselines.insert_baseline(baseline_)
                self.high_lr_lease = HIGH_LR_LEASE

    # preprocess the reward
    def get_reward(self, access, wait, piece, \
                   wait_info1, \
                   reward_buffer):
        
        samples = int(len(access) / ACCESSE_SPACE)

        for i in range(samples):
            access_t = access[i * ACCESSE_SPACE : (i + 1) * ACCESSE_SPACE]
            wait_t = wait[i * WAIT_SPACE : (i + 1) * WAIT_SPACE]
            piece_t = piece[i * PIECE_SPACE : (i + 1) * PIECE_SPACE]
            waitinfo1_t = wait_info1[i * WAIT_SPACE : (i + 1) * WAIT_SPACE]
            self.baselines.samples_different_action(access_t, wait_t, piece_t, \
                                                    waitinfo1_t)

        self.ep_access_rs, self.ep_wait_rs, self.ep_piece_rs, \
        self.ep_waitinfo1_rs \
        = self.baselines.calculate_reward(reward_buffer)

        self.baselines.clear_signal()

    def learn(self, idx, lr, generations):
        if (len(self.ep_access_act) == 0):
            print("useless round")
            return

        base_path = os.path.join(os.getcwd(), self.log_dir)
        baseline_path = os.path.join(base_path, 'Baseline.txt')
        with open(baseline_path, 'a+') as f:
            f.write('RL at iter {}'.format(idx) + ', ')
            f.write(str(self.baselines) + '\n')

        self.get_reward(self.ep_access_act, self.ep_wait_act, self.ep_piece_act, \
                        self.ep_wait_info_act1, \
                        self.ep_rs)

        self.lr = 0.5 * lr * (1 + math.cos(math.pi * idx / generations))

        self.samples_count = len(self.ep_rs)

        self.sess.run(self.train_op, feed_dict={
             self.tf_access_act: self.ep_access_act,
             self.tf_wait_act: self.ep_wait_act,
             self.tf_piece_act: self.ep_piece_act,
             self.tf_wait_info_act1: self.ep_wait_info_act1,
             self.tf_access_vt: self.ep_access_rs,
             self.tf_wait_vt: self.ep_wait_rs,
             self.tf_piece_vt: self.ep_piece_rs,
             self.tf_wait_info_vt1: self.ep_waitinfo1_rs,

             self.tf_samples_count: self.samples_count,
             self.learning_rate: self.lr,
        })

        self.update_policy()
        
    # tool functions:
    def get_prob(self):
        self.access_p, self.wait_p, self.piece_p, \
        self.wait_info1_p, \
        = self.sess.run([self.access_action, self.wait_action, self.piece_action, \
                         self.wait_info_action1])
        self.print_prob()

    def print_prob(self):
        stri = ""
        stri += str(self.access_p) + " "
        stri += str(self.wait_p) + " "
        stri += str(self.piece_p) + " "
        stri += str(self.wait_info1_p) + " "
        print(stri + "\n")

    def rollback(self, index, fail_to_exe):
        self.ep_access_act = self.ep_access_act[:(index - fail_to_exe) * ACCESSE_SPACE] + self.ep_access_act[(index + 1 - fail_to_exe) * ACCESSE_SPACE :]
        self.ep_wait_act = self.ep_wait_act[:(index - fail_to_exe) * WAIT_SPACE] + self.ep_wait_act[(index + 1 - fail_to_exe) * WAIT_SPACE :]
        self.ep_piece_act = self.ep_piece_act[:(index - fail_to_exe) * PIECE_SPACE] + self.ep_piece_act[(index + 1 - fail_to_exe) * PIECE_SPACE :]
        self.ep_wait_info_act1 = self.ep_wait_info_act1[:(index - fail_to_exe) * WAIT_SPACE] + self.ep_wait_info_act1[(index + 1 - fail_to_exe) * WAIT_SPACE :]
        