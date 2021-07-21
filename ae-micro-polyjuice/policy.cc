#include <algorithm>
#include <fstream>
#include <iostream>
#include <regex>

#include "macros.h"
#include "policy.h"
#include "PolicyGradient.h"

Policy::Policy(std::string policy_file)
  : identifier(policy_file) {
    // if (policy_file == "occ") {
    //   txn_buf_size = 1;
    //   sub_policies = new SubPolicy*[contention_ranges];
    //   for (int i = 0; i < contention_ranges; ++i) {
    //     sub_policies[i] = new SubPolicy();
    //     sub_policies[i]->init_occ();
    //   }
    //   // print_policy();
    //   return;
    // }

    // std::ifstream pol_file;
    // pol_file.open(policy_file);

    // if (!pol_file.is_open()) {
    //   std::cerr << "Could not open file: " << policy_file << ". "
    //             << "Please specify correct policy file with the --policy option"
    //             << std::endl;
    //   ALWAYS_ASSERT(false);
    // }

    // std::string not_using;
    // std::string txn_buf_size_str;
    // std::getline(pol_file, not_using);
    // std::getline(pol_file, txn_buf_size_str);
    // txn_buf_size = std::stoi(txn_buf_size_str);

    // sub_policies = new SubPolicy*[contention_ranges];
    // for (int i = 0; i < contention_ranges; ++i) {
    //   // sub_policies[i] = new SubPolicy();
    //   // std::getline(pol_file, not_using);
    //   // sub_policies[i]->init(&pol_file);
    // }
    // pol_file.close();
    // print_policy();
  }

void Policy::print_policy() {
  std::cout << "Txn buffer size:" << txn_buf_size << std::endl;
  for (int i = 0; i < contention_ranges; ++i) {
    std::cerr << "CONTENTION " << i << std::endl;
    sub_policies[i]->print_policy();
  }
}

SubPolicy::SubPolicy() {
  acc_decisions = new bool[ACCESSES];
  wait_decisions = new AgentDecision[ACCESSES];
  before_commit_piece_wait_decisions = new AgentDecision[ACCESSES];
  piece_end_decisions = new bool[ACCESSES];
  wait_access_info = new uint32_t[WAIT_ACCESS_INFO_LENGTH]();
}

void SubPolicy::init_occ() {
  for (int i = 0; i < ACCESSES; i++) {
    acc_decisions[i] = true;
    wait_decisions[i] = agent_noWait;
    before_commit_piece_wait_decisions[i] = agent_noWait;
    piece_end_decisions[i] = false;
  }
  for (int i = 0; i < WAIT_ACCESS_INFO_LENGTH; i++)
    wait_access_info[i] = 0;
  for (int i = 1; i <= TXN_TYPE; i++) {
    const_backoff_shift_bound[i] = 0;
    backoff_shift_bound[i] = 63;
  }
  for (int i = 1; i <= TXN_TYPE; i++)
    final_commit_wait_decisions[i] = agent_noWait;
  for (int i = 0; i < COMMIT_PHASE_WAIT_ACCESS_INFO_LENGTH; i++)
    final_commit_wait_access_info[i] = 0;
  init_conflicts();
}

void SubPolicy::init(std::ifstream *pol_file) {
  std::string not_using;
  std::string acc_decisions_str;
  std::string wait_decisions_str;
  std::string before_commit_piece_wait_decisions_str;
  std::string wait_access_info_str;
  std::string piece_end_info_str;
  std::string final_commit_wait_decisions_str;
  std::string final_commit_wait_access_info_str;
  std::string const_backoff_str;
  std::string backoff_bound_str;
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, acc_decisions_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, wait_decisions_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, before_commit_piece_wait_decisions_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, wait_access_info_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, piece_end_info_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, final_commit_wait_decisions_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, final_commit_wait_access_info_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, const_backoff_str);
  std::getline(*pol_file, not_using);
  std::getline(*pol_file, backoff_bound_str);

  // ------------- line: access decision -------------
  std::vector<AgentDecision> acc_decisions_vec;
  acc_decisions_vec.resize(ACCESSES);
  std::transform(acc_decisions_str.begin(), acc_decisions_str.end(), acc_decisions_vec.begin(),
                  [](char ac) -> AgentDecision {
                    if (ac == '0') return agent_clean_private;
                    else if (ac == '1') return agent_dirty_public;
                    else ALWAYS_ASSERT(false);
                  });
  for (int i = 0; i < ACCESSES; ++i) {
    acc_decisions[i] = (acc_decisions_vec[i] == agent_clean_private);
  }

  // ------------- line: before access wait decisions -------------
  std::vector<AgentDecision> wait_decisions_vec;
  wait_decisions_vec.resize(ACCESSES);
  std::transform(wait_decisions_str.begin(), wait_decisions_str.end(), wait_decisions_vec.begin(),
                  [](char ac) -> AgentDecision {
                    if (ac == '2') return agent_noWait;
                    else if (ac == '3') return agent_waitAccess;
                    else ALWAYS_ASSERT(false);
                  });
  for (int i = 0; i < ACCESSES; ++i) {
    wait_decisions[i] = wait_decisions_vec[i];
  }

  // ------------- line: before commit piece wait decisions -------------
  std::vector<AgentDecision> before_commit_piece_wait_decisions_vec;
  before_commit_piece_wait_decisions_vec.resize(ACCESSES);
  std::transform(before_commit_piece_wait_decisions_str.begin(), before_commit_piece_wait_decisions_str.end(), before_commit_piece_wait_decisions_vec.begin(),
                  [](char ac) -> AgentDecision {
                    if (ac == '2') return agent_noWait;
                    else if (ac == '3') return agent_waitAccess;
                    else ALWAYS_ASSERT(false);
                  });
  for (int i = 0; i < ACCESSES; ++i) {
    before_commit_piece_wait_decisions[i] = before_commit_piece_wait_decisions_vec[i];
  }

  // ------------- line: waiting info -------------
  {
    std::regex re(" ");
    std::sregex_token_iterator p(wait_access_info_str.begin(), wait_access_info_str.end(), re, -1);
    std::sregex_token_iterator end;
    int i = 0;
    while(p != end) {
      std::string cur = *p++;
      if (cur == "c") wait_access_info[i++] = WAIT_DEP_COMMIT;
      else wait_access_info[i++] = std::stoi(cur);
    }
    ALWAYS_ASSERT(i == WAIT_ACCESS_INFO_LENGTH);
  }

  // ------------- line: whether is a piece end -------------
  std::vector<bool> piece_end_decisions_vec;
  piece_end_decisions_vec.resize(ACCESSES);
  std::transform(piece_end_info_str.begin(), piece_end_info_str.end(), piece_end_decisions_vec.begin(),
                  [](char ac) -> bool {
                    if (ac == '0') return false;
                    else if (ac == '1') return true;
                    else ALWAYS_ASSERT(false);
                  });
  for (int i = 0; i < ACCESSES; ++i) {
    piece_end_decisions[i] = piece_end_decisions_vec[i];
  }

  // ------------- line: wait decisions for final commit phase -------------
  std::vector<AgentDecision> final_commit_wait_decisions_vec;
  final_commit_wait_decisions_vec.resize(TXN_TYPE);
  std::transform(final_commit_wait_decisions_str.begin(), final_commit_wait_decisions_str.end(), final_commit_wait_decisions_vec.begin(),
                  [](char ac) -> AgentDecision {
                    if (ac == '2') return agent_noWait;
                    else if (ac == '3') return agent_waitAccess;
                    else ALWAYS_ASSERT(false);
                  });
  for (int i = 0; i < TXN_TYPE; ++i) {
    final_commit_wait_decisions[i + 1] = final_commit_wait_decisions_vec[i];
  }

  {
    std::regex re(" ");
    std::sregex_token_iterator p(final_commit_wait_access_info_str.begin(), final_commit_wait_access_info_str.end(), re, -1);
    std::sregex_token_iterator end;
    int i = 0;
    while(p != end) {
      std::string cur = *p++;
      if (cur == "c") final_commit_wait_access_info[i++] = WAIT_DEP_COMMIT;
      else final_commit_wait_access_info[i++] = std::stoi(cur);
    }
    ALWAYS_ASSERT(i == COMMIT_PHASE_WAIT_ACCESS_INFO_LENGTH);
  }

  // ------------- line: whether to use const backoff bound -------------
  std::vector<bool> const_backoff_vec;
  const_backoff_vec.resize(TXN_TYPE);
  std::transform(const_backoff_str.begin(), const_backoff_str.end(), const_backoff_vec.begin(),
                  [](char ac) -> bool {
                    if (ac == '0') return false;
                    else if (ac == '1') return true;
                    else ALWAYS_ASSERT(false);
                  });
  for (int i = 0; i < TXN_TYPE; ++i) {
    const_backoff_shift_bound[i + 1] = const_backoff_vec[i];
  }

  // ------------- line: retry backoff bound -------------
  {
    std::regex re(" ");
    std::sregex_token_iterator p(backoff_bound_str.begin(), backoff_bound_str.end(), re, -1);
    std::sregex_token_iterator end;
    int type = 1;
    while(p != end) {
      backoff_shift_bound[type++] = std::stoi(*p++);
    }
  }

  init_conflicts();
}

void SubPolicy::init_conflicts() {
  conflicts = new uint32_t*[TXN_TYPE + 1];

  for(uint32_t i = 1; i <= TXN_TYPE; i++) {
    conflicts[i] = new uint32_t[(txn_access_num[i] + 1) * TXN_TYPE]();
    for(uint32_t j = 1; j <= txn_access_num[i]; j++) {
      // calculate j's global access id
      uint32_t j_acc_id = 0;
      for (int type_idx = 1; type_idx < i; ++type_idx)
        j_acc_id += txn_access_num[type_idx];
      j_acc_id += j - 1;
      for(uint32_t k = 0; k < TXN_TYPE; k++) {
        conflicts[i][j * TXN_TYPE + k] = wait_access_info[j_acc_id * TXN_TYPE + k];
      }
    }
  }
}

void SubPolicy::print_policy() {
  std::cout << "-----------------------------------------------------" << std::endl;
  std::cout << "Txn backoff shift bound size:" << std::endl;
  for (int i = 1; i <= TXN_TYPE; i++)
    std::cout << "type" << i << " - " << backoff_shift_bound[i] << "  is const - " << const_backoff_shift_bound[i] << std::endl;
  std::string string_info[6] =  {"agent_clean_private", "agent_dirty_public", "agent_noWait", "agent_waitAccess"};
  std::cout << "Inference_access_contention_low:" << std::endl;
  for (int i = 0; i < ACCESSES; i++)
    std::cout << "Low contention Action at access id " << i << " is occ or not: "
              << acc_decisions[i]
              << std::endl;
  std::cout << "Inference_before_access_wait_low:" << std::endl;
  for (int i = 0; i < ACCESSES; i++)
    std::cout << "Action at access id " << i << " is "
              << string_info[wait_decisions[i]]
              << std::endl;
  std::cout << "Inference_before_commit_piece_wait:" << std::endl;
  for (int i = 0; i < ACCESSES; i++)
    std::cout << "Action at access id " << (i + ACCESSES) << " is "
              << string_info[before_commit_piece_wait_decisions[i]]
              << std::endl;
  std::cout << "Inference_piece_end:" << std::endl;
  for (int i = 0; i < ACCESSES; i++)
    std::cout << "Piece end at access id " << (i + ACCESSES) << " is "
              << piece_end_decisions[i]
              << std::endl;
  std::cout << "Wait Decision at final commit phase:" << std::endl;
  for (int i = 1; i <= TXN_TYPE; i++)
    std::cout << "type" << i << " - " << string_info[final_commit_wait_decisions[i]] << std::endl;
  std::cout << "Inference_final_commit_wait_access_info:" << std::endl;
  for (int i = 1; i <= TXN_TYPE; i++)
    for (int j = 1; j <= TXN_TYPE; j++)
    std::cout << "My Type " << i << "  "
              << "Dep Type " << j << "  "
              << final_commit_wait_access_info[(i - 1) * TXN_TYPE + j - 1]
              << std::endl;
  std::cout << "Wait access info:" << std::endl;
  for (int i = 0; i < ACCESSES; i++)
    for (int j = 1; j <= TXN_TYPE; j++) {
      uint32_t target = wait_access_info[i * TXN_TYPE + j - 1];
      std::cout << "access id: " << i << " should " << (target == 0 ? "not " : "") << "wait for txn type " << j;
      if (target == WAIT_DEP_COMMIT)
        std::cout << " to commit ";
      else if (target != 0)
        std::cout << " 's " << target << "th access ";
      std::cout << std::endl;
    }
  std::cout << "Conflicts:" << std::endl;
  for(uint32_t i = 1; i <= TXN_TYPE; i++) {
    for(uint32_t j = 1; j <= txn_access_num[i]; j++) {
      for(uint32_t k = 0; k < TXN_TYPE; k++) {
        uint32_t target = conflicts[i][j * TXN_TYPE + k];
        std::cout << "type: " << i << " 's " << j << "th access should " << (target == 0 ? "not " : "") << "wait for txn type " << k + 1;
        if (target == WAIT_DEP_COMMIT)
          std::cout << " to commit ";
        else if (target != 0)
          std::cout << " 's " << target << "th access ";
        std::cout << std::endl;
      }
    }
  }
}

void SubPolicy::set_conflict(const uint32_t acc_id_1, const uint32_t acc_id_2) {
  INVARIANT(acc_id_1 < ACCESSES && acc_id_2 < ACCESSES);
  uint32_t type_1 = 1;
  uint32_t txn_inside_acc_id_1 = acc_id_1;
  for (int i = 1; i <= TXN_TYPE; ++i) {
    if (txn_inside_acc_id_1 < txn_access_num[i]) break;
    else {
      txn_inside_acc_id_1 -= txn_access_num[i];
      type_1++;
    }
  }
  uint32_t type_2 = 1;
  uint32_t txn_inside_acc_id_2 = acc_id_2;
  for (int i = 1; i <= TXN_TYPE; ++i) {
    if (txn_inside_acc_id_2 < txn_access_num[i]) break;
    else {
      txn_inside_acc_id_2 -= txn_access_num[i];
      type_2++;
    }
  }
  wait_access_info[acc_id_1 * TXN_TYPE + type_2 - 1] = txn_inside_acc_id_2 + 1;
  wait_access_info[acc_id_2 * TXN_TYPE + type_1 - 1] = txn_inside_acc_id_1 + 1;
}

Policy::~Policy() {
  // for (int i = 0; i < contention_ranges; ++i) {
  //   delete sub_policies[i];
  // }
  // delete[] sub_policies;
}

SubPolicy::~SubPolicy() {
  delete[] acc_decisions;
  delete[] wait_decisions;
  delete[] before_commit_piece_wait_decisions;
  delete[] piece_end_decisions;
  delete[] wait_access_info;
  for(uint32_t i = 1; i <= TXN_TYPE; i++) {
    delete[] conflicts[i];
  }
  delete[] conflicts;
}

PolicySelector::PolicySelector(Policy* policy)
  : default_policy(policy),
    state(initial_state),
    last_commit_tps(0),
    last_abort_tps(0) {

}

PolicySelector::~PolicySelector() {
  std::map<Policy *, std::pair<double, double>>::iterator iter;
  for (iter = policy_map.begin(); iter != policy_map.end(); iter++) {
    delete iter->first;
  }
  delete default_policy;
}

void PolicySelector::add_policy(Policy *policy, double commit_tps, double abort_tps) {
  name_map.insert(std::pair<std::string, Policy *>(policy->get_identifier(), policy));
  policy_map.insert(std::pair<Policy *, std::pair<double, double>>(policy, std::pair<double, double>(commit_tps, abort_tps)));
}

Policy *PolicySelector::match_policy(double commit_tps, double abort_tps) {
  Policy *best = nullptr;
  double min_distance = (std::numeric_limits<double>::max)();

  ALWAYS_ASSERT(policy_map.size() > 0);
  for (auto &kv : policy_map) {
    std::pair<double, double> normalized = kv.second;
    double distance = sqrt(pow(fabs(commit_tps - normalized.first), 2) + pow(fabs(abort_tps - normalized.second), 2));
    if (distance < min_distance) {
      best = kv.first;
      min_distance = distance;
    }
  }
  return best;
}

bool PolicySelector::detect_change(double commit_tps, double abort_tps) {
  ALWAYS_ASSERT(last_commit_tps != 0);
  double change_rate = fabs((commit_tps - last_commit_tps) / last_commit_tps);
  if (change_rate > threshold)
    return true;
  return false;
}

Policy *PolicySelector::policy_interrupt(double commit_tps, double abort_tps) {
  if (state == initial_state) {

    state = new_policy_state;
    // about to do the normalization
    last_commit_tps = 0;
    last_abort_tps = 0;
    return const_cast<Policy *>(default_policy);

  } else if (state == new_policy_state) {

    state = stable_state;
    // pick a ideal policy
    selected_policy = match_policy(commit_tps, abort_tps);
    return selected_policy;

  } else if (state == stable_state) {

    if (last_commit_tps == 0 && last_abort_tps == 0) {
      // indicating the current policy has just been applied
      last_commit_tps = commit_tps;
      last_abort_tps = abort_tps;
    } else {
      if (detect_change(commit_tps, abort_tps)) {
        state = new_policy_state;
        // detect a workload change
        last_commit_tps = 0;
        last_abort_tps = 0;
        return const_cast<Policy *>(default_policy);
      }
    }
    return selected_policy;

  } else 
    ALWAYS_ASSERT(false);
}

Policy *PolicySelector::pick_policy_by_name(std::string& name) {
  return name_map[name];
}