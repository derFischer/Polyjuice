import argparse

TXN_TYPES = 3
CONTENTION_LEVEL = 2
OP_TYPE = 2
TXN_ACCESS = [11, 7, 8]

BUFFERS_SIZE = 2
COMMIT_BACKOFF = 4
ABORT_BACKOFF = 4

ACCESS_ACTION = 0
WAIT_ACTION = 1
PIECE_ACTION = 2
WAIT_INFO = 3

COMMIT_WAIT = TXN_TYPES * CONTENTION_LEVEL + 1

def construct_line(access_action, wait_action, piece_action, wait_info):
    line = ""
    line += str(access_action) + " "
    line += str(wait_action) + " "
    line += str(piece_action) + " "
    for i in range(TXN_TYPES):
        line += str(wait_info[i]) + " "
    line += "\n"
    return line

def transfer_policy(policy_file, new_policy_file):
    policy = open(policy_file, 'r')
    new_policy = open(new_policy_file, 'w')

    # copy buffer size
    for i in range(2):
        line = policy.readline()
        new_policy.write(line)

    # copy commit/abort backoff
    for i in range(COMMIT_BACKOFF + ABORT_BACKOFF):
        line = policy.readline()
        new_policy.write(line)

    # ignore commit wait
    for i in range(COMMIT_WAIT):
        policy.readline()

    # copy normal access
    line = policy.readline()
    new_policy.write(line)
    access_actions = []
    piece_actions = []
    for i in range(TXN_TYPES):
        for j in range(TXN_ACCESS[i]):
            access_action = (policy.readline().split('\n')[0]).split( )
            access_action = list(map(int, access_action))
            access_actions.append(access_action)
            for m in range(CONTENTION_LEVEL - 1):
                policy.readline()
            piece_action = (policy.readline().split('\n')[0]).split( )
            piece_action = list(map(int, piece_action))
            piece_actions.append(piece_action)
            for m in range(CONTENTION_LEVEL - 1):
                policy.readline()

    base = 0
    for i in range(TXN_TYPES):
        for j in range(TXN_ACCESS[i]):
            access_action = access_actions[base + j][ACCESS_ACTION]
            wait_action = access_actions[base + j][WAIT_ACTION]
            piece_action = piece_actions[base + j][PIECE_ACTION]
            
            wait_info = []
            if j != 0:
                prev_validation_wait_action = piece_actions[base + j - 1][WAIT_ACTION]
                current_wait = access_actions[base + j][WAIT_INFO:]
                prev_wait = piece_actions[base + j - 1][WAIT_INFO:]
                for m in range(TXN_TYPES):
                    wait = 0
                    if wait_action and prev_validation_wait_action:
                        wait = max(current_wait[m], prev_wait[m])
                    else:
                        if wait_action:
                            wait = current_wait[m]
                        else:
                            wait = prev_wait[m]
                    wait_info.append(wait)
                wait_action = wait_action | prev_validation_wait_action
            else:
                for m in range(TXN_TYPES):
                    wait_info.append(access_actions[base + j][WAIT_INFO + m])
            line = construct_line(access_action, wait_action, piece_action, wait_info)
            new_policy.write(line)
        base += TXN_ACCESS[i]

def main(args):
    
    transfer_policy(args.policy, args.new)
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--policy', type=str, 
                        help='policy file')
    parser.add_argument('--new', type=str, default='./new_policy',
                        help='new policy file')

    main(parser.parse_args())







