#ifndef _POLICY_H_
#define _POLICY_H_

#include <cmath>
#include <map>
#include <string>
#include <vector>

#define MAX_ACC_ID std::numeric_limits<uint32_t>::max()

// #define WORKLOAD_TYPE_TPCC
// #define WORKLOAD_TYPE_TPCE
#define WORKLOAD_TYPE_MICRO_BENCH

#ifdef WORKLOAD_TYPE_MICRO_BENCH
  #define TXN_TYPE 10
  // acc_id(access id) starts from 0, [0, ACCESSES - 1]
  #define ACCESSES 80
  #define TXN_ACCESS_NUM { 0 /*just a placeholder*/, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 }
#endif

#ifdef WORKLOAD_TYPE_TPCC
  // type1: neworder, type2: payment, type3: delivery
  #define TXN_TYPE 3
  // acc_id(access id) starts from 0, [0, ACCESSES - 1]
  #define ACCESSES 26
  #define TXN_ACCESS_NUM { 0 /*just a placeholder*/, 11, 7, 8}
#endif

#ifdef WORKLOAD_TYPE_TPCE
  // type1: trade_order
  #define TXN_TYPE 4
  // acc_id(access id) starts from 0, [0, ACCESSES - 1]
  #define ACCESSES 105
  #define TXN_ACCESS_NUM { 0 /*just a placeholder*/, 30, 48, 10, 17 }
#endif

#define WAIT_ACCESS_INFO_LENGTH (ACCESSES * TXN_TYPE)
#define COMMIT_PHASE_WAIT_ACCESS_INFO_LENGTH (TXN_TYPE * TXN_TYPE)

using txn_type_bool = bool[TXN_TYPE + 1];
using txn_type_int = int[TXN_TYPE + 1];
using txn_type_decision = int[TXN_TYPE + 1];
using commit_phase_wait_access_info_uint32_t = uint32_t[COMMIT_PHASE_WAIT_ACCESS_INFO_LENGTH];

enum AgentDecision : unsigned char {
  // two are for read/write operations
  agent_clean_private = 0,
  agent_dirty_public = 1,
  // two are for wait decisions
  agent_noWait = 2,
  agent_waitAccess = 3,
  // three are for backoff decisions
  agent_inc_backoff = 4,
  agent_dec_backoff = 5,
  agent_nop_backoff = 6,
};

typedef std::pair<AgentDecision, double> backoff_action;

// One SubPolicy is for one contention range
class SubPolicy {
private:
  bool* acc_decisions;
  AgentDecision* wait_decisions;
  AgentDecision* before_commit_piece_wait_decisions;
  bool* piece_end_decisions;
  uint32_t* wait_access_info;
  txn_type_bool const_backoff_shift_bound;
  txn_type_int backoff_shift_bound;
  txn_type_decision final_commit_wait_decisions;
  commit_phase_wait_access_info_uint32_t final_commit_wait_access_info;

  // total access number for each txn type
  unsigned txn_access_num[TXN_TYPE + 1] =  TXN_ACCESS_NUM;

  //2D dimensional array: 
  //1st dimension - txn type, 2nd dimension - access * all_txn_type, value - internal access id (index inside a txn) which we should wait for
  // txn type starts from 1
  // internal access id starts from 1
  uint32_t** conflicts;

  // initialize conflicts with wait_access_info
  void init_conflicts();

  // this acc_ids are global access ids, which start from 0, [0, ACCESSES)
  // this func is only used for manual policy, manually setting the wait_access_info
  void set_conflict(const uint32_t acc_id_1, const uint32_t acc_id_2);

public:
  SubPolicy();
  ~SubPolicy();

  void print_policy();

  void init_occ();

  // Load policy from target stream
  void init(std::ifstream *pol_file);

  // return true (meaning occ action) if decision is cleanRead or privateWrite
  // return false (meaning ic3 action) if decision is dirtyRead or publicWrite
  inline bool inference_access(uint32_t acc_id) const {
    INVARIANT(acc_id < ACCESSES);
    return acc_decisions[acc_id];
  }

  // this func is called at the beginning of a txn and return the waitAccess info for all possible dependent txn type
  // return value example: TXN_TYPE is 2
  // index 0  placeholder
  // index 1  placeholder
  // index 2  internal access 1 depends on txn type 1
  // index 3  internal access 1 depends on txn type 2
  // index 4  internal access 2 depends on txn type 1
  // ......
  inline uint32_t* inference_wait_access(uint32_t txn_type) const {
    INVARIANT(txn_type > 0 && txn_type <= TXN_TYPE);
    return conflicts[txn_type];
  }

  // three action types: noWait, waitCommit or waitAccess
  inline AgentDecision inference_wait(uint32_t acc_id) const {
    INVARIANT(acc_id < 2 * ACCESSES);
    if (acc_id < ACCESSES) {
      return wait_decisions[acc_id];
    } else {
      return before_commit_piece_wait_decisions[acc_id - ACCESSES];
    }
  }

  inline bool inference_piece_end(uint32_t acc_id) const {
    ALWAYS_ASSERT(acc_id >= ACCESSES && acc_id < ACCESSES * 2);
    return piece_end_decisions[acc_id - ACCESSES];
  }

  inline int inference_backoff_shift_bound(uint32_t txn_type) const {
    return backoff_shift_bound[txn_type];
  }

  inline bool inference_const_backoff(uint32_t txn_type) const {
    return const_backoff_shift_bound[txn_type];
  }

  inline AgentDecision inference_commit_phase_wait(uint32_t my_type) const {
    return final_commit_wait_decisions[my_type];
  }

  inline uint32_t inference_commit_phase_wait_access(uint32_t my_type, uint32_t dep_type) const {
    int idx = (my_type - 1) * TXN_TYPE + dep_type - 1;
    return final_commit_wait_access_info[idx];
  }
};

enum Contention : int {
  contention_low = 0,
  contention_high = 1,
  contention_end_type = 2
};

const size_t contention_ranges = (contention_end_type - contention_low);

class Policy {
private:
  int txn_buf_size;
  const std::string identifier;

  SubPolicy **sub_policies;

public:
  Policy(std::string policy_file);
  ~Policy();

  void print_policy();

  inline std::string get_identifier() {
    return identifier;
  }

  inline int get_txn_buf_size() {
    return txn_buf_size;
  }

  inline Contention contention_into_range(uint16_t contention) {
    if (contention < 1) return contention_low;
    else return contention_high;
  }

  inline bool inference_access(uint32_t acc_id, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_access(acc_id);
  }

  inline uint32_t* inference_wait_access(uint32_t txn_type, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_wait_access(txn_type);
  }

  inline AgentDecision inference_wait(uint32_t acc_id, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_wait(acc_id);
  }

  inline bool inference_piece_end(uint32_t acc_id, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_piece_end(acc_id);
  }

  inline bool inference_lock_mode(uint32_t acc_id, uint16_t contention) const {
    INVARIANT(acc_id < 2 * ACCESSES);
    // mode == true means spin mode
    // mode == false means mcs mode
    // hack - manually assign spin mode for all scenarios
    return true;
  }

  inline bool inference_lock_mode_txn_end(uint32_t txn_type, uint16_t contention) const {
    // mode == true means spin mode
    // mode == false means mcs mode
    // hack - manually assign spin mode for all scenarios
    return true;
  }

  inline int inference_backoff_shift_bound(uint32_t txn_type, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_backoff_shift_bound(txn_type);
  }

  inline bool inference_const_backoff(uint32_t txn_type, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_const_backoff(txn_type);
  }

  inline AgentDecision inference_commit_phase_wait(uint32_t my_type, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_commit_phase_wait(my_type);
  }

  inline uint32_t inference_commit_phase_wait_access(uint32_t my_type, uint32_t dep_type, uint16_t contention) const {
    return sub_policies[contention_into_range(contention)]->inference_commit_phase_wait_access(my_type, dep_type);
  }
};

enum SelectorState : unsigned char {
  initial_state = 0,
  new_policy_state = 1,
  stable_state = 2
};

class PolicySelector {
private:
  // mapping from policy name to policy
  std::map<std::string, Policy *> name_map;
  // mapping from policy to normalized info
  std::map<Policy *, std::pair<double, double>> policy_map;
  // serve as assigned policy at static workload and normalized policy at dynamic workload
  const Policy *default_policy;
  // policy being seleceted for current workload
  Policy *selected_policy;
  // whether is trying to normalize workload contention
  SelectorState state;
  // useful when normalizing == false
  double last_commit_tps;
  double last_abort_tps;
  // workload change threshold, (0, 1]
  double threshold = 0.2;

  Policy *match_policy(double commit_tps, double abort_tps);

  bool detect_change(double commit_tps, double abort_tps);

public:
  PolicySelector(Policy* policy);
  ~PolicySelector();

  inline Policy *get_default_policy() {
    return const_cast<Policy *>(default_policy);
  }

  void add_policy(Policy *policy, double commit_tps, double abort_tps);

  Policy *policy_interrupt(double commit_tps, double abort_tps);

  Policy *pick_policy_by_name(std::string& name);
};

#endif /* _POLICY_H_ */