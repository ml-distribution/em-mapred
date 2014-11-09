# Tests for StaticUtil.py.

import StaticUtil
import sys

THRESHOLD = 10 ** -10

############################################################################

def test_calc_log_sum_of_logs():
    # Test case 1: logx = 0, logy = 0
    logx = 0
    logy = 0
    log_sum = StaticUtil.calc_log_sum_of_logs(logx, logy)
    target_log_sum = 1
    assert(abs(target_log_sum - log_sum) < THRESHOLD)

    # Test case 2: logx = 0, logy = -123
    logx = 0
    logy = -123
    log_sum = StaticUtil.calc_log_sum_of_logs(logx, logy)
    target_log_sum = 0
    assert(abs(target_log_sum - log_sum) < THRESHOLD)

    # Test case 3: logx = -234, logy = -234
    logx = -234
    logy = -234
    log_sum = StaticUtil.calc_log_sum_of_logs(logx, logy)
    target_log_sum = -233
    assert(abs(target_log_sum - log_sum) < THRESHOLD)

    # Test case 3: logx = -678, logy = -678
    logx = -478
    logy = -478
    log_sum = StaticUtil.calc_log_sum_of_logs(logx, logy)
    target_log_sum = -477
    assert(abs(target_log_sum - log_sum) < THRESHOLD)

############################################################################

def test_calc_log_prod_of_logs():
    # Test case 1: logx = None, logy = None
    logx = None
    logy = None
    log_prod = StaticUtil.calc_log_prod_of_logs(logx, logy)
    assert(log_prod is None)

    # Test case 2: logx = None, logy = -5
    logx = None
    logy = -5
    log_prod = StaticUtil.calc_log_prod_of_logs(logx, logy)
    assert(log_prod is None)

    # Test case 3: logx = 13, logy = None
    logx = 13
    logy = None
    log_prod = StaticUtil.calc_log_prod_of_logs(logx, logy)
    assert(log_prod is None)

    # Test case 4: logx = -5, logy = 13
    logx = -5
    logy = 13
    log_prod = StaticUtil.calc_log_prod_of_logs(logx, logy)
    assert(log_prod == 8)

############################################################################

def test_parse_trans_prob_file():
    failure = False

    # Test case 1: Invalid format in line 2
    trans_prob_file_path = "test_input_files/trans_prob_file_1"
    trans_prob_file = open(trans_prob_file_path, "r")
    try:
        StaticUtil.parse_trans_prob_file(trans_prob_file)
        failure = True
    except BaseException: {}
    trans_prob_file.close()

    # Test case 2: Transition to start token in line 3
    trans_prob_file_path = "test_input_files/trans_prob_file_2"
    trans_prob_file = open(trans_prob_file_path, "r")
    try:
        StaticUtil.parse_trans_prob_file(trans_prob_file)
        failure = True
    except BaseException: {}
    trans_prob_file.close()

    # Test case 3: Invalid probability format in line 1
    trans_prob_file_path = "test_input_files/trans_prob_file_3"
    trans_prob_file = open(trans_prob_file_path, "r")
    try:
        StaticUtil.parse_trans_prob_file(trans_prob_file)
        failure = True
    except BaseException: {}
    trans_prob_file.close()

    # Test case 4: Empty file
    trans_prob_file_path = "test_input_files/trans_prob_file_4"
    trans_prob_file = open(trans_prob_file_path, "r")
    try:
        StaticUtil.parse_trans_prob_file(trans_prob_file)
        failure = True
    except BaseException: {}
    trans_prob_file.close()

    # Test case 5: Valid transition probabilities file
    trans_prob_file_path = "test_input_files/trans_prob_file_5"

    trans_prob_file = open(trans_prob_file_path, "r")
    (trans_prob_dict, state_set, start_token) = StaticUtil.parse_trans_prob_file(trans_prob_file)
    trans_prob_file.close()

    correct_state_set = set()
    correct_state_set.add("V")
    correct_state_set.add("C")

    assert(correct_state_set.issubset(state_set))
    assert(state_set.issubset(correct_state_set))

    assert(start_token is "#")

    assert(trans_prob_dict[("#", "V")] == .3)
    assert(trans_prob_dict[("#", "C")] == .7)
    assert(trans_prob_dict[("V", "V")] == .2)
    assert(trans_prob_dict[("V", "C")] == .8)
    assert(trans_prob_dict[("C", "V")] == .6)
    assert(trans_prob_dict[("C", "C")] == .4)

    assert(trans_prob_dict[("C", "#")] == 0)

    assert(not failure)

############################################################################

def test_parse_emis_prob_file():
    failure = False

    # Test case 1: Invalid format in line 2
    emis_prob_file_path = "test_input_files/emis_prob_file_1"
    emis_prob_file = open(emis_prob_file_path, "r")
    try:
        StaticUtil.parse_emis_prob_file(emis_prob_file)
        sys.stderr.write("test_parse_emis_prob_file: Error in test case 1.\n")
        failure = True
    except BaseException: {}
    emis_prob_file.close()

    # Test case 2: Invalid probability format in line 1
    emis_prob_file_path = "test_input_files/emis_prob_file_2"
    emis_prob_file = open(emis_prob_file_path, "r")
    try:
        StaticUtil.parse_emis_prob_file(emis_prob_file)
        sys.stderr.write("test_parse_emis_prob_file: Error in test case 2.\n")
        failure = True
    except BaseException: {}
    emis_prob_file.close()

    # Test case 3: Empty file
    emis_prob_file_path = "test_input_files/emis_prob_file_3"
    emis_prob_file = open(emis_prob_file_path, "r")
    try:
        StaticUtil.parse_emis_prob_file(emis_prob_file)
        sys.stderr.write("test_parse_emis_prob_file: Error in test case 3.\n")
        failure = True
    except BaseException: {}
    emis_prob_file.close()

    # Test case 4: Valid emission probabilities file
    emis_prob_file_path = "test_input_files/emis_prob_file_4"

    emis_prob_file = open(emis_prob_file_path, "r")
    emis_prob_dict = StaticUtil.parse_emis_prob_file(emis_prob_file)
    emis_prob_file.close()

    assert(emis_prob_dict[("V", "a")] == .3)
    assert(emis_prob_dict[("V", "b")] == .7)
    assert(emis_prob_dict[("C", "a")] == .2)
    assert(emis_prob_dict[("C", "b")] == .8)

    assert(not failure)

############################################################################

### Main Procedure ###

test_calc_log_sum_of_logs()

test_calc_log_prod_of_logs()

test_parse_trans_prob_file()

test_parse_emis_prob_file()

print "SUCCESS"
