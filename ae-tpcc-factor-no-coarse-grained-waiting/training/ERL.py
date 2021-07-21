import numpy as np
import tensorflow as tf
import os
import sys
import time
import re
import signal
import subprocess
import numpy as np
from PG import PolicyGradient
from EA import EA
from Policy import Sample

class ERL:
    def __init__(self, log_dir, log_rate, kid_dir, draw_graph, command, sync_period, 
                 learning_rate, rd, selection, pop_size, bfactor, mutate_decay, 
                 mut_frac):
        self.log_dir = log_dir
        self.log_rate = log_rate
        self.kid_dir = kid_dir
        self.draw_graph = draw_graph
        self.command = command
        self.sync_period = sync_period
        self.RL_agent = PolicyGradient(
            log_dir = self.log_dir, 
            kid_dir = self.kid_dir, 
            learning_rate = learning_rate, 
            rd = rd)
        self.EA_agent = EA(
            log_dir = self.log_dir, 
            kid_dir = self.kid_dir, 
            selection = selection, 
            pop_size = pop_size, 
            bfactor = bfactor, 
            mutate_decay = mutate_decay, 
            mut_frac = mut_frac)

    def training(self, generations, learning_rate, samples_per_distribution, initial_policy, load_per_sample):
        if self.log_dir and self.draw_graph:
            merged = tf.summary.merge_all()
            writer = tf.summary.FileWriter(self.log_dir, self.RL_agent.sess.graph)

        print('Experiment logging saving at ' + self.log_dir)

        # EA initial sample
        if initial_policy is None:
            self.EA_agent.initialize_pops(self.RL_agent.policy.samples(self.EA_agent.pop_size))
        else:
            for one_pol in initial_policy.split(' '):
                sample = Sample()
                sample.pick_up(one_pol)
                self.EA_agent.initialize_pops(sample)
        
        # actual learning
        for i in range(generations):
            # EA evaluation
            print('Iter {} - EA'.format(i))
            EA_access, EA_wait, EA_piece, \
            EA_wait_info1, EA_wait_info2, EA_wait_info3, \
            EA_reward_buffer, weakest_reward, best_seen_sample \
            = self.EA_agent.Evaluate(i, generations, self.command, load_per_sample)

            # logging
            if self.log_dir and self.draw_graph and i % self.log_rate == 0:
                sc = tf.Summary()
                # EA part logging
                sc.value.add(tag='EA-best-seen',
                            simple_value=self.EA_agent.best_seen)
                sc.value.add(tag='EA-round-best',
                            simple_value=self.EA_agent.round_best)
                sc.value.add(tag='EA-round-mean',
                            simple_value=self.EA_agent.round_mean)
                sc.value.add(tag='EA-round-worst',
                            simple_value=self.EA_agent.round_worst)
                writer.add_summary(sc, i)
                writer.flush()

            self.EA_agent.clear_round_info()
        
