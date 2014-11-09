# Static utility functions. All logs are base 2!

from collections import defaultdict
from math import log

############################################################################

# Given two numbers log(x) and log(y), returns log(x+y). If both log(x) and log(y) are negative,
# alculates the log of the sum in the following manner to reduce the risk of underflow:
# Finds log(z) = min(log(x), log(y)). Then adds log(z) to both log(x) and log(y) before
# exponentiating, adds the two exponentiations, takes the log of the sum, and then
# subtracts log(z). This calculation works properly because z*x + z*y = z*(x+y),
# so 2^(log(z)+log(x)) + 2^(log(z)+log(y)) = 2^(log(z)+log(x+y)).
# Thus, log(2^(log(z)+log(x)) + 2^(log(z)+log(y))) = log(2^(log(z)+log(x+y))) = log(z)+log(x+y).
#
# Note: If logx (logy) is passed as None, then x (y) is interpreted as 0. If both parameters
# are passed as None, then None is returned.
def calc_log_sum_of_logs(logx, logy):
    if logx is None and logy is None:
        return None
    elif logx is None:
        return logy
    elif logy is None:
        return logx

    if logx >= 0 or logy >= 0:
        x = 2 ** logx
        y = 2 ** logy
        sum_of_x_and_y = x + y
        log_sum_of_x_and_y = log(sum_of_x_and_y, 2)
        return log_sum_of_x_and_y

    logz = min(logx, logy)
    scaled_logx = logx + logz
    scaled_logy = logy + logz

    scaled_x = 2 ** scaled_logx
    scaled_y = 2 ** scaled_logy

    scaled_sum_of_x_and_y = scaled_x + scaled_y
    log_scaled_sum_of_x_and_y = log(scaled_sum_of_x_and_y, 2)
    log_sum_of_x_and_y = log_scaled_sum_of_x_and_y - logz

    return log_sum_of_x_and_y

############################################################################

# Returns logx + logy = log(x*y).

# Note: If logx (logy) is passed as None, then x (y) is interpreted as 0. If either x or y is 0,
# then None is returned to indicate that x*y = 0.
def calc_log_prod_of_logs(logx, logy):
    if logx is None or logy is None:
        return None

    return logx + logy

############################################################################

# Parses the given transition probabilities file for the hidden markov model.
# Each line of the file must be of the form "<from_state> <to_state> <prob>". The from_state
# of the first line must be a token representing the start of an emission sequence. Returns
# (a dictionary mapping each (from_state, to_state) tuple to the probability of the transition,
# the set of all states, the emission sequence start token).
# Transitions not present in the file will be given 0 probability.
def parse_trans_prob_file(trans_prob_file):
    trans_prob_dict = defaultdict(int)
    state_set = set()
    start_token = None

    for i, unstripped_line in enumerate(trans_prob_file):
        line = unstripped_line.rstrip()
        if line:
            tokens = line.split()

            if len(tokens) != 3:
                raise BaseException("Line %d of the transition probabilities file has an invalid format.\n" % (i+1))

            from_state = tokens[0]
            to_state = tokens[1]

            if i == 0:
                start_token = from_state
            else:
                if to_state is start_token:
                    raise BaseException("Line %d of the transition probabilities file has a transition to the start token, which is invalid.\n" % (i+1))

            try:
                prob = float(tokens[2])
            except ValueError:
                raise BaseException("On line %d of the transition probabilities file, the probability %s is not a valid float.\n" % (i+1, tokens[2]))

            trans_prob_dict[(from_state, to_state)] = prob
            state_set.add(from_state)
            state_set.add(to_state)

    if start_token is None:
        raise BaseException("The transition probabilities file is empty.\n")

    state_set.remove(start_token)
    return (trans_prob_dict, state_set, start_token)

############################################################################

# Parses the given emission probabilities file for the hidden markov model.
# Each line of the file must be of the form "<state> <emitted_token> <prob>". Returns
# a dictionary mapping each (state, token) tuple to the probability of the emission.
# Emissions not present in the file will be given 0 probability.
def parse_emis_prob_file(emis_prob_file):
    emis_prob_dict = defaultdict(int)
    empty = True

    for i, unstripped_line in enumerate(emis_prob_file):
        line = unstripped_line.rstrip()
        if line:
            empty = False

            tokens = line.split()

            if len(tokens) != 3:
                raise BaseException("Line %d of the emission probabilities file has an invalid format.\n" % (i+1))

            state = tokens[0]
            emitted_token = tokens[1]

            try:
                prob = float(tokens[2])
            except ValueError:
                raise BaseException("On line %d of the emission probabilities file, the probability %s is not a valid float.\n" % (i+1, tokens[2]))

            emis_prob_dict[(state, emitted_token)] = prob

    if empty:
        raise BaseException("The emission probabilities file is empty.\n")

    return emis_prob_dict

############################################################################

# Returns a list of dictionaries, where each dictionary at index i corresponds to the observation
# w_i. Each entry in each dictionary maps a state X to the log sum over all possible previous state
# taggings of the probability of w_i given this state X.
def calculate_forward_matrix(trans_prob_dict, emis_prob_dict, state_set, start_token, obs_list):
    forward_log_prob_dicts = []

    for i, obs in enumerate(obs_list):
        forward_log_prob_dict = defaultdict(lambda: None)
        forward_log_prob_dicts.append(forward_log_prob_dict)

        prev_state = start_token
        for state in state_set:
            # P(state|prev_state) * P(obs|state)
            log_prob_state_given_prevstate = trans_prob_dict[(prev_state, state)]



############################################################################
