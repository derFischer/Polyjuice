#ifndef CONFLICT_GRAPH_H
#define CONFLICT_GRAPH_H
#include <stdint.h>
#include <limits>

class conflict_graph
{

public:

#define MAX_STEP std::numeric_limits<uint32_t>::max()

public:
  class chopped_tx
  {

    public:
    chopped_tx()
    {
      conflicts =  nullptr;
      type = 0;
      total_ops = 0;
      total_types = 0;
    }

    ~chopped_tx()
    {
      //TODO free memory
    }

    void init(uint8_t tp, uint32_t cps, uint32_t total_tps)
    {

      type = tp;
      total_ops = cps;
      total_types = total_tps;

      conflicts = new uint32_t*[total_ops + 1];

      for(uint32_t i = 1; i <= total_ops; i++){ 
        conflicts[i] = new uint32_t[total_types + 1]();
      }

      //Each transaction conflict with it self
      for(uint32_t i = 1; i <= total_ops; i++) {
        conflicts[i][type] = i;
        //fprintf(stderr, "op<%ld, %ld>: %d\n", i, type, i);
      }
    }

    // C(this.op1, type.op2) = 1
    void set_conflict(uint32_t op1, uint8_t type, uint32_t op2)
    {

      //ALWAYS_ASSERT(op1 <= total_ops && op1 > 0);
      //ALWAYS_ASSERT(op2 <= total_types && type > 0);
      conflicts[op1][type] = op2;
    }

    //Get the step of type which conflict my_step
    int32_t get_conflict(uint32_t my_step, uint8_t type)
    {
      return conflicts[my_step][type];
    }

    //Get the first step which is not less than min_step
    //and conflict with type
    int32_t get_first_conflict(uint32_t min_step, uint8_t type)
    {
      for(uint32_t i = min_step; i <= total_ops; i++)
      {
        if(conflicts[i][type] > 0)
            return i;
      }
      return 0;
    }

    //return t2.op2: t2.op2 conflict with t1.op2 which < op
    //return 0 if not exist
    int32_t get_prev_conflict(uint32_t op, uint8_t t2)
    {

      for(int i = op - 1 ; i > 0; i--)
      {
        if(conflicts[i][t2] > 0) {
          return conflicts[i][t2];
        }
        
      }
      return 0;
    }

    //return t2.op2: op2 conflict with t1.op2 which is >= op
    //return 0 if not exist
    int32_t get_next_conflict(uint32_t op, uint8_t t2)
    {
    
      for(uint32_t i = op; i <= total_ops; i++)
      {
        if(conflicts[i][t2] > 0) {
          return conflicts[i][t2];
        }
      }
      return 0;
    }

    uint32_t get_ops()
    {
        return total_ops;
    }

    private:
      //2D dimensional array: 
      //1st dimension - step number, 2nd dimension - type, value - conflict step
      uint32_t** conflicts;
      uint8_t type;
      uint32_t total_ops;
      uint32_t total_types;
  };


public:

  conflict_graph(uint8_t ts)
  {
    types = ts;
    txns = new chopped_tx[types + 1];
  }

  void init_txn(uint8_t type, uint32_t ops)
  {
    txns[type].init(type, ops, types);
  }

  void set_conflict(uint8_t t1, uint32_t op1, uint8_t t2, uint32_t op2)
  {
    ALWAYS_ASSERT(t1 != t2);
    txns[t1].set_conflict(op1, t2, op2);
    txns[t2].set_conflict(op2, t1, op1);
  }

  //get the t1's step which is conflict with t2
  uint32_t conflict_step(uint8_t t1, uint8_t t2, uint32_t step)
  {
    return txns[t2].get_conflict(step, t1);
  }


  //get the first piece of t1 which is conflict with with pieces with op or 
  uint32_t get_r_piece(uint8_t t2, uint8_t t1, uint32_t op) 
  {
    return txns[t1].get_next_conflict(op, t2);
  }
  
  //get the conflict region of t2 which include t1.op 
  //return val: t2. <op1, op2>: 
  //   t2.op1 conflict t1.op1, t2.op2 conflict with t1.op2
  //   and  t1.op1 < t1.op <= t1.op2

  std::pair<uint32_t, uint32_t> 
  get_conflict_region(uint8_t t1, uint32_t op, uint8_t t2)
  {
    uint32_t op1 = txns[t1].get_prev_conflict(op, t2);
    uint32_t op2 = txns[t1].get_next_conflict(op, t2);

    return std::pair<uint32_t, uint32_t>(op1, op2);
  }

  //get the first t2's step >= min_step and conflict with t1
  uint32_t first_conflict_step(uint8_t t1, uint8_t t2, uint32_t min_step)
  {
    return txns[t2].get_first_conflict(min_step, t1);
  }

 
  uint32_t get_ops(uint8_t t)
  {
    return txns[t].get_ops();
  }
private:
  chopped_tx* txns;
  uint32_t types;

};

#endif