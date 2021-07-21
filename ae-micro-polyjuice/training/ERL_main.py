#!/usr/bin/env python
import argparse
import numpy as np
from ERL import ERL
import utils as utils

def main(args):
    cfg = utils.setup(args)

    command = ['./out-perf.masstree/benchmarks/dbtest --bench {} --retry-aborted-transactions --db-type ndb-ic3'
               ' --backoff-aborted-transactions --scale-factor {} --bench-opt "{}" --num-threads {} --runtime {} '.format(
                args.workload_type, args.scale_factor, args.bench_opt, args.nworkers, args.eval_time)]

    erl = ERL(
        log_dir = cfg.get('log_directory'), 
        log_rate = args.log_rate, 
        kid_dir = cfg.get('kid_directory'), 
        draw_graph = args.graph, 
        command = command, 
        sync_period = args.sync_period, 
        learning_rate = args.learning_rate, 
        rd = args.reward_decay, 
        selection = args.selection, 
        pop_size = args.psize, 
        bfactor = args.random_branch, 
        mutate_decay = args.mutate_decay, 
        mut_frac = args.mutate_rate)
    
    erl.training(
        generations = args.max_iterations, 
        learning_rate = args.learning_rate, 
        samples_per_distribution = args.samples_per_distribution, 
        initial_policy = args.pickup_policy, 
        load_per_sample = args.load_per_sample)
    
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # ERL training profile
    parser.add_argument('--expr-name', type=str, default='erl-genetic',
                        help='experiment name')
    parser.add_argument('--base-log-dir', type=str, default='./training/saved_model',
                        help='model save location')
    parser.add_argument('--base-kid-dir', type=str, default='./training/kids',
                        help='kid policy save location')
    parser.add_argument('--log-rate', type=int, default=1,
                        help='how many steps per tensorboard log. 0 for off')
    parser.add_argument('--graph', action='store_true',
                        help='whether to draw graph')
    parser.add_argument('--load-per-sample', action='store_true',
                        help='whether to load data for each sample')
    
    # ERL algorithm arguments
    parser.add_argument('--max-iterations', type=int, default=300,
                        help='terminate after this many iterations')
    parser.add_argument('--eval-time', type=float, default=1.,
                        help='number of seconds for evaluation')
    parser.add_argument('--sync-period', type=int, default=2,
                        help='how often is RL & Genetic\'s communication' )
    
    # RL algorithm arguments
    parser.add_argument('--samples-per-distribution', type=int, default=32,
                        help='number of samples per distribution')
    parser.add_argument("--learning-rate", type=float, default=0.0003,
                        help='learning rate of the RL model')
    parser.add_argument('--reward-decay', type=float, default=0.3,
                        help='parameter to adjust the baseline')

    # Genetic algorithm arguments
    parser.add_argument('--selection', type=str, default='truncation',
                        choices=['tournament', 'truncation'],
                        help='selection methodology for each genetic iteration')
    parser.add_argument('--psize', type=int, default=8,
                        help='size of genetic population')
    parser.add_argument('--random-branch', type=int, default=4,
                        help='branching factor for population for random exploration')
    parser.add_argument('--mutate-decay', action='store_true',
                        help='whether mutate frac decays with iterations')
    parser.add_argument('--mutate-rate', type=float, default=.1,
                        help='mutation fraction')
    parser.add_argument('--pickup-policy', type=str, default=None,
                        help='designated policy to be included in initial parent policys')

    # Workload arguments
    parser.add_argument('--workload-type', type=str, default='tpcc',
                        choices=['tpcc', 'tpce', 'ycsb', 'microbench'],
                        help='number of database workers')
    parser.add_argument('--nworkers', type=int, default=32,
                        help='number of database workers')
    parser.add_argument('--scale-factor', type=int, default=1,
                        help='scale factor')
    parser.add_argument('--bench-opt', type=str, default='-w 45,43,4,4,4',
                        help='benchmark info, e.g. workload mix ratio')

    main(parser.parse_args())
