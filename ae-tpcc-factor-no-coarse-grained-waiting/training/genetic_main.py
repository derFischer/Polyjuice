#!/usr/bin/env python
import argparse
import numpy as np
import utils as utils

from genetic import learn

def main(args):
    np.random.seed(args.seed)

    cfg = utils.setup(args)

    command = ['./out-perf.masstree/benchmarks/dbtest --bench {} --retry-aborted-transactions --parallel-loading --db-type ndb-ic3'
               ' --backoff-aborted-transactions --scale-factor {} --bench-opt "{}" --num-threads {} --runtime {}'.format(
                args.workload_type, args.scale_factor, args.bench_opt, args.nworkers, args.eval_time)]

    learn(args.workload_type, args.psize, args.random_branch, args.mutate_rate, args.crossover,
          args.ep_per_sample, args.mutate_schedule == 'linear', command,
          args.selection, args.cache, cfg.get('log_directory'), cfg.get('kid_directory'),
          args.log_rate, args.max_iterations, args.pickup_policy, args.no_default, args.load_per_pol)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Training progress arguments
    parser.add_argument('--base-log-dir', type=str, default='./training/saved_model',
                        help='model save location')
    parser.add_argument('--base-kid-dir', type=str, default='./training/kids',
                        help='kid policy save location')
    parser.add_argument('--expr-name', type=str, default='rlcc',
                        help='experiment name')
    parser.add_argument('--log-rate', type=int, default=1,
                        help='how many steps per tensorboard log. 0 for off')
    parser.add_argument('--max-iterations', type=int, default=10000,
                        help='terminate after this many iterations')
    parser.add_argument('--eval-time', type=float, default=1.,
                        help='number of seconds for evaluation')

    # Genetic algorithm arguments
    parser.add_argument('--psize', type=int, default=5,
                        help='size of genetic population')
    parser.add_argument('--random-branch', type=int, default=0,
                        help='branching factor for population for random exploration')
    parser.add_argument('--mutate-rate', nargs=2, type=float, default=[0.1, 0.01],
                        metavar=('SELECT_PERCENT', 'MUTATE_PERCENT'),
                        help='action mutate percent')
    parser.add_argument('--crossover', nargs=2, type=float, default=[0.9, 3],
                        metavar=('SELECT_PERCENT', 'CROSS_POINTS'),
                        help='crossover rate and number of points')
    parser.add_argument('--mutate-schedule', choices=['linear', 'constant'],
                        default='constant', help='decay schedule for mutate')
    parser.add_argument('--ep-per-sample', type=int, default=3,
                        help='number of episodes to measure each member')
    parser.add_argument('--selection', type=str, default='truncation',
                        choices=['tournament', 'truncation'],
                        help='number of database workers')
    parser.add_argument('--cache', action='store_true',
                        help='whether to cache population evaluations')
    parser.add_argument('--pickup-policy', type=str, default=None,
                        help='designated policy to be included in initial parent policys')
    parser.add_argument('--no-default', action='store_true',
                        help='whether initiate population with default policies, --pickup-policy can not be null')

    # Misc
    parser.add_argument('--seed', help='RNG seed', type=int, default=42)

    # Workload arguments
    parser.add_argument('--load-per-pol', action='store_true',
                        help='whether to load data for each policy')
    parser.add_argument('--workload-type', type=str, default='tpcc',
                        choices=['tpcc', 'tpce', 'ycsb'],
                        help='number of database workers')
    parser.add_argument('--nworkers', type=int, default=32,
                        help='number of database workers')
    parser.add_argument('--scale-factor', type=int, default=1,
                        help='scale factor')
    parser.add_argument('--bench-opt', type=str, default='',
                        help='benchmark info, e.g. workload mix ratio')

    main(parser.parse_args())
