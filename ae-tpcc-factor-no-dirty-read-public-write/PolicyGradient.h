#ifndef _POLICYGRADIENT_H_
#define _POLICYGRADIENT_H_
#define MAX_ACC_ID std::numeric_limits<uint32_t>::max()

#define OP_TYPES 1
#define CONTENTION_LEVEL 1
#define RETRY_TIMES 3

#define TXN_ACTION_DIMENSION 3

#include "policy.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <regex>

enum OP_TYPE : unsigned char {
    OP_RW = 0,
    OP_COMMIT = 1
};
// #include "tensorflow/core/public/session.h"
// #include "tensorflow/core/platform/env.h"

// using namespace tensorflow;
class PolicyAction {
    private:
    bool access_action = false;
    AgentDecision wait_action = AgentDecision::agent_noWait;
    uint32_t access_wait_info[TXN_TYPE] = {0};

    bool piece_end_decision = false;
    AgentDecision validation_wait = AgentDecision::agent_noWait;
    uint32_t validation_wait_info[TXN_TYPE] = {0};

    public:
    PolicyAction(bool access_a, AgentDecision wait_a, int wait_i[TXN_TYPE], bool piece_end_d)
    {
        access_action = access_a;

        piece_end_decision = piece_end_d;
        wait_action = wait_a;
        for(int i = 0; i < TXN_TYPE; ++i)
        {
            access_wait_info[i] = *(wait_i + i);
        }
    }
    PolicyAction(){}
    ~PolicyAction()
    {
        if (this)
        {
            delete this;
        }
    }
    bool GetAccessAction()
    {
        return access_action;
    }
    AgentDecision GetWaitAction()
    {
        return wait_action;
    }
    bool GetPieceEndDecision()
    {
        return piece_end_decision;
    }
    uint32_t *GetAccessWaitInfo()
    {
        return access_wait_info;
    }
    AgentDecision GetValidationWaitAction()
    {
        return validation_wait;
    }
    uint32_t *GetValidationWaitInfo()
    {
        return validation_wait_info;
    }
    void set_validation_wait_info(AgentDecision validation_wait_a, int validation_wait_i[TXN_TYPE])
    {
        validation_wait = validation_wait_a;
        for(int i = 0; i < TXN_TYPE; ++i)
        {
            validation_wait_info[i] = *(validation_wait_i + i);
        }
    }
};

using backoff_info = double[2][RETRY_TIMES][TXN_TYPE + 1];

class PolicyGradient {
    private:
    int txn_buf_size;
    backoff_info backoff;
    PolicyAction* policy[ACCESSES][OP_TYPES][CONTENTION_LEVEL];

    int txn_access_num[TXN_TYPE + 1] = TXN_ACCESS_NUM;
    public:
    PolicyGradient(std::string policy_f)
    {
        std::ifstream pol_file;
        pol_file.open(policy_f);
        if (!pol_file.is_open()) {
            std::cerr << "Could not open file: " << policy_f << ". "
                        << "Please specify correct policy file with the --policy option"
                        << std::endl;
            ALWAYS_ASSERT(false);
        }
        std::string not_using;
        // txn buffer size
        std::string txn_buf_size_str;
        std::getline(pol_file, not_using);
        std::getline(pol_file, txn_buf_size_str);
        txn_buf_size = std::stoi(txn_buf_size_str);
        // txn retry backoff - successful commit part
        std::getline(pol_file, not_using);
        for (int i = 0; i < RETRY_TIMES; ++i)
        {
            std::string backoff_str;
            std::getline(pol_file, backoff_str);
            std::regex re(" ");
            std::sregex_token_iterator p(backoff_str.begin(), backoff_str.end(), re, -1);
            std::sregex_token_iterator end;
            int type = 1;
            
            backoff[1][i][0] = 0.0;
            while(p != end)
            {
                std::string cur = *p++;
                backoff[1][i][type++] = std::stod(cur);
            }
        }
        // txn retry backoff - fail to commit part
        std::getline(pol_file, not_using);
        for (int i = 0; i < RETRY_TIMES; ++i)
        {
            std::string backoff_str;
            std::getline(pol_file, backoff_str);
            std::regex re(" ");
            std::sregex_token_iterator p(backoff_str.begin(), backoff_str.end(), re, -1);
            std::sregex_token_iterator end;
            int type = 1;
            
            backoff[0][i][0] = 0.0;
            while(p != end)
            {
                std::string cur = *p++;
                backoff[0][i][type++] = std::stod(cur);
            }
        }

        std::getline(pol_file, not_using);
        // normal access
        for (int i = 0; i < ACCESSES; ++i)
        {
            for (int j = 0; j < OP_TYPES; ++j)
            {
                for (int k = 0; k < CONTENTION_LEVEL; ++k)
                {
                    std::string tmp;
                    std::getline(pol_file, tmp);
                    policy[i][j][k] = transfer_action(tmp);
                }
            }
        }

        int base = 0;
        for (int i = 1; i < TXN_TYPE + 1; i++)
        {
            for (int j = 0; j < txn_access_num[i]; j++)
            {
                for (int m = 0; m < OP_TYPES; ++m)
                {
                    for (int n = 0; n < CONTENTION_LEVEL; ++n)
                    {
                        if (j != txn_access_num[i] - 1)
                        {
                            policy[j + base][m][n]->set_validation_wait_info(policy[j + base + 1][m][n]->GetWaitAction(), policy[j + base + 1][m][n]->GetAccessWaitInfo());
                        }
                        else
                        {
                            policy[j + base][m][n]->set_validation_wait_info(AgentDecision::agent_waitAccess, txn_access_num);
                        }
                    }
                }
            }
            base += txn_access_num[i];
        }
    }

    ~PolicyGradient()
    {
        for(int i = 0; i < ACCESSES; ++i)
        {
            for(int j = 0; j < OP_TYPES; ++j)
            {
                for(int k = 0; k < CONTENTION_LEVEL; ++k)
                {
                    delete policy[i][j][k];
                }

            }
        }
        if (this)
        {
            delete this;
        }
    }

    void strtoaction(float buffer[TXN_ACTION_DIMENSION], std::string content)
    {
        std::istringstream iss(content);
        for(int i = 0; i < TXN_ACTION_DIMENSION + TXN_TYPE; ++i)
        {
            std::string tmp;
            std::getline(iss, tmp, ' ');
            buffer[i] = atoi(tmp.c_str());
        }
    }

    PolicyAction* transfer_action(std::string action)
    {
        float buffer[TXN_ACTION_DIMENSION + TXN_TYPE];
        strtoaction(buffer, action);
        int wait_info[TXN_TYPE];
        bool access_action = (buffer[0] == 0);
        AgentDecision wait_action = (buffer[1] == 0)?(AgentDecision::agent_noWait):(AgentDecision::agent_waitAccess);
        bool piece_action = (buffer[2] == 1);
        for (int i = 0; i < TXN_TYPE; ++i)
        {
            wait_info[i] = buffer[3 + i];
        }
        return new PolicyAction(access_action, wait_action, wait_info, piece_action);
    }

    inline uint16_t cal_contention_level(uint16_t contention)
    {
        return 0;
    }

    PolicyAction* inference(uint32_t acc_id, uint16_t contention = 0, OP_TYPE op_type = 0)
    {
        if (acc_id >= ACCESSES)
        {
            acc_id -= ACCESSES;
        }
        if (acc_id >= ACCESSES)
        {
            return nullptr;
        }
        uint16_t contention_level = cal_contention_level(contention);
        return policy[acc_id][op_type][contention_level];
    }

    int get_txn_buf_size() const {
        return txn_buf_size;
    }

    backoff_action inference_backoff_action(bool commit_success, uint16_t which_retry, uint32_t txn_type) const {

        if (txn_type == 0) return  backoff_action(agent_nop_backoff, 0);

        uint16_t retry_times = which_retry > 2 ? 2 : which_retry;
        if (commit_success)
            return backoff_action(agent_dec_backoff, backoff[1][retry_times][txn_type]);
        else
            return backoff_action(agent_inc_backoff, backoff[0][retry_times][txn_type]);
    }
};

#endif