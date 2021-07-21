#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>
#include <stdio.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <iostream>
#include <sstream>
#include <fstream>

#include <set>
#include <vector>
#include <math.h>

#include "smallbank.h"
#include "../txn.h"
#include "../macros.h"
#include "../scopedperf.hh"
#include "../spinlock.h"


#include "bench.h"
#include "../txn_btree.h"
#include "../conflict_graph.h"
//#include "../txn_proto2_impl.h"

#undef INVARIANT
#define INVARIANT(expr) ((void)0)

using namespace std;
using namespace util;

const int NUM_ACCOUNTS = 100;
//const int NUM_ACCOUNTS = 1000000;
//const int NUM_ACCOUNTS = 10;
const int MIN_BALANCE  = 10000;
const int MAX_BALANCE  = 50000;


enum bank_tx_types
{
  amalgate_type = 1,
  deposit_checking_type,
  send_payment_type,
  transact_savings_type,
  write_check_type,
  end_type
};

const size_t type_num = (end_type - amalgate_type);

static inline conflict_graph*
init_bank_cgraph()
{
  conflict_graph* g = new conflict_graph(type_num);

  //No need to specify the constraint,
  //if only one c-edge

  g->init_txn(amalgate_type, 1);
  g->init_txn(deposit_checking_type, 1);
  g->init_txn(send_payment_type,1);
  g->init_txn(transact_savings_type,1);
  g->init_txn(write_check_type,1);


  g->set_conflict(amalgate_type, 1, deposit_checking_type, 1);
  g->set_conflict(amalgate_type, 1, send_payment_type, 1);
  g->set_conflict(amalgate_type, 1, transact_savings_type, 1);
  g->set_conflict(amalgate_type, 1, write_check_type, 1);
  g->set_conflict(deposit_checking_type, 1,send_payment_type, 1);
  g->set_conflict(deposit_checking_type, 1,transact_savings_type, 1);
  g->set_conflict(deposit_checking_type, 1,write_check_type, 1);
  g->set_conflict(send_payment_type, 1,transact_savings_type, 1);
  g->set_conflict(send_payment_type, 1,write_check_type, 1);
  g->set_conflict(transact_savings_type, 1,write_check_type, 1);



  return g;
}

static conflict_graph* cgraph = NULL;

#define BANK_TABLE_LIST(x) \
	x(account) \
	x(account_index) \
	x(checking) \
	x(saving)

//int INFO::txn_read_abort = 0, INFO::txn_write_abort = 0, INFO::txn_absent_abort = 0;


//static unsigned g_txn_workload_mix[] = {10,5,5,65,10,5};//{10,10,35,20,10,15};//{20, 5, 5, 20, 45, 5}; //{15,5,5,15,55,5};//{10,4,4,10,68,4};//{20, 5, 5, 20, 45, 5};
		//del reserv/findflights/findopenseats/new reserv/update c/update reserv

//static unsigned g_txn_workload_mix[] = {20,20,20,10,10,20};
static unsigned g_txn_workload_mix[] = {20,20,20,10,10,20};
//skip load the file






static inline int NumPartitions(){
	return (int)scale_factor;
}






struct _dummy {};


class bank_worker_mixin : private _dummy {
#define DEFN_TBL_INIT_X(name) \
  , tbl_ ## name ## _vec(partitions.at(#name))

public:
  bank_worker_mixin(const map<string, vector<abstract_ordered_index *>> &partitions) :
    _dummy() // so hacky...
    BANK_TABLE_LIST(DEFN_TBL_INIT_X)
  {
    ALWAYS_ASSERT(NumPartitions() >= 1);
  }

#undef DEFN_TBL_INIT_X

protected:

#define DEFN_TBL_ACCESSOR_X(name) \
private:  \
  vector<abstract_ordered_index *> tbl_ ## name ## _vec; \
protected: \
  inline ALWAYS_INLINE abstract_ordered_index * \
  tbl_ ## name (unsigned int wid) \
  { \
    return tbl_ ## name ## _vec[wid - 1]; \
  };

  BANK_TABLE_LIST(DEFN_TBL_ACCESSOR_X);

#undef DEFN_TBL_ACCESSOR_X

	static uint64_t makeFlightId(uint64_t a, uint64_t da, uint64_t aa, uint64_t st, uint64_t dt){
		uint64_t flight_date = (dt - st)/3600000;
		uint64_t id = a | (da << 16) | (aa << 32) | (flight_date << 48);
		return id;
	}

	static inline string
	RandomStr(fast_random &r, uint len)
	{
	  // this is a property of the oltpbench implementation...
	  if (!len)
		return "";

	  uint i = 0;
	  string buf(len - 1, 0);
	  while (i < (len - 1)) {
		const char c = (char) r.next_char();
		// XXX(stephentu): oltpbench uses java's Character.isLetter(), which
		// is a less restrictive filter than isalnum()
		if (!isalnum(c))
		  continue;
		buf[i++] = c;
	  }
	  return buf;
	}

	static inline ALWAYS_INLINE unsigned int
	PartitionId(unsigned int wid)
	{
	
	  wid -= 1; // 0-idx

	  if (NumPartitions() <= (int)nthreads)
	    // more workers than partitions, so its easy
	    return wid;
	  const unsigned nwhse_per_partition = NumPartitions() / nthreads;
	  const unsigned partid = wid / nwhse_per_partition;
	  if (partid >= nthreads)
	    return nthreads - 1;
	  return partid;
	}

	static void
  	PinToPartitionId(unsigned int wid)
  	{
    	const unsigned int partid = PartitionId(wid);
    	ALWAYS_ASSERT(partid < nthreads);
    	const unsigned int pinid  = partid;
    	if (verbose)
    	  cerr << "PinToWarehouseId(): coreid=" << coreid::core_id()
    	       << " pinned to whse=" << wid << " (partid=" << partid << ")"
    	       << endl;
    	rcu::s_instance.pin_current_thread(pinid);
    	rcu::s_instance.fault_region();
  	}

	static inline ALWAYS_INLINE int
	RandomNumber(fast_random &r, int min, int max)
	{
	  return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
	}

	static inline ALWAYS_INLINE
	double gaussrand()
	{
    static double V1, V2, S;
    static int phase = 0;
    double X;
    if ( phase == 0 ) {
        do {
            double U1 = (double)rand() / RAND_MAX;
            double U2 = (double)rand() / RAND_MAX;
            V1 = 2 * U1 - 1;
            V2 = 2 * U2 - 1;
            S = V1 * V1 + V2 * V2;
        } while(S >= 1 || S == 0);
        X = V1 * sqrt(-2 * log(S) / S);
    } else
        X = V2 * sqrt(-2 * log(S) / S);
    	phase = 1 - phase;
    	return X;
	}

	static inline ALWAYS_INLINE int
	Gaussian(fast_random &r, int min, int max)
	{
			int range_size = (max - min) + 1;
            //double mean = range_size / 2.0;
		    int value = -1;
            while (value < 0 || value >= range_size) {

                double gaussian = (gaussrand() + 2.0) / 4.0;
                value = (int)round(gaussian * range_size);
            }
            return (value + min);
	}


	static inline ALWAYS_INLINE int
	CheckBetweenInclusive(int v, int lower, int upper)
	{
	  
	  return v;
	}


};

STATIC_COUNTER_DECL(scopedperf::tsc_ctr, seats_txn, seats_txn_cg);




class bank_worker : public bench_worker, public bank_worker_mixin {
public:
	bank_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              const map<string, vector<abstract_ordered_index *>> &partitions,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              uint partition_id)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      bank_worker_mixin(partitions),
      partition_id(partition_id)
	{
		partid = (partition_id-1)%(int)scale_factor;
		obj_key0.reserve(str_arena::MinStrReserveLength);
		obj_key1.reserve(str_arena::MinStrReserveLength);
		obj_v.reserve(str_arena::MinStrReserveLength);
	}

	bool complete(){
	//	printf("newseatdrop %d newcustdrop %d urcon %d\n", newseatdrop, newcustdrop, urcon);
	//	printf("read abort %d write abort %d absent abort %d\n", INFO::txn_read_abort, INFO::txn_write_abort, INFO::txn_absent_abort);
		return true;
	}

	txn_result txn_balance();
	static txn_result
	TxnBalance(bench_worker *w)
	{
		ANON_REGION("TxnBalance:", &seats_txn_cg);
		return static_cast<bank_worker *>(w)->txn_balance();
	}
	txn_result txn_amalgate();
	static txn_result
	TxnAmalgate(bench_worker *w)
	{
		ANON_REGION("TxnAmalgate:", &seats_txn_cg);
		return static_cast<bank_worker *>(w)->txn_amalgate();
	}

	txn_result txn_deposit_checking();

	static txn_result
	TxnDepositChecking(bench_worker *w)
	{
		ANON_REGION("TxnDepositChecking:", &seats_txn_cg);
		return static_cast<bank_worker *>(w)->txn_deposit_checking();
	}
	txn_result txn_send_payment();
	static txn_result
	TxnSendPayment(bench_worker *w)
	{
		ANON_REGION("TxnSendPayment:", &seats_txn_cg);
		return static_cast<bank_worker *>(w)->txn_send_payment();
	}
	txn_result txn_transact_savings();
	static txn_result
	TxnTransactSavings(bench_worker *w)
	{
		ANON_REGION("TxnTransactSavings:", &seats_txn_cg);
		return static_cast<bank_worker *>(w)->txn_transact_savings();
	}
	txn_result txn_write_check();
	static txn_result
	TxnWriteCheck(bench_worker *w)
	{
		ANON_REGION("TxnWriteCheck:", &seats_txn_cg);
		return static_cast<bank_worker *>(w)->txn_write_check();
	}
	void check_delay(){
	}

	virtual workload_desc_vec
    get_workload() const
    {
    	workload_desc_vec w;

		unsigned m = 0;
		for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
		  m += g_txn_workload_mix[i];
		ALWAYS_ASSERT(m == 100);
		if (g_txn_workload_mix[0])
			w.push_back(workload_desc("Balance", double(g_txn_workload_mix[0])/100.0, TxnBalance));
		if (g_txn_workload_mix[1])
			w.push_back(workload_desc("Amalgate:", double(g_txn_workload_mix[1])/100.0, TxnAmalgate));
		if (g_txn_workload_mix[2])
			w.push_back(workload_desc("DepositChecking:", double(g_txn_workload_mix[2])/100.0, TxnDepositChecking));
		if (g_txn_workload_mix[3])
			w.push_back(workload_desc("SendPayment:", double(g_txn_workload_mix[3])/100.0, TxnSendPayment));
		if (g_txn_workload_mix[4])
			w.push_back(workload_desc("TransactSavings", double(g_txn_workload_mix[4])/100.0, TxnTransactSavings));
		if (g_txn_workload_mix[5])
			w.push_back(workload_desc("WriteCheck", double(g_txn_workload_mix[5])/100.0, TxnWriteCheck));
		return w;

	}


	inline ALWAYS_INLINE string &
	str()
	{
	  return *arena.next();
	}

	virtual void
	on_run_setup() OVERRIDE
	{
	    if (!pin_cpus)
	      return;
		//bind 0,scale_factor, 2*scale_factor... to the first numa node
		//1, scale_factor+1,... to the second numa node(if scale_factor >= 1)
		//2, scale_factor+2,... to the second numa node(if scale_factor >= 2)
		//...
	    const size_t a = worker_id % coreid::num_cpus_online();
		size_t num = scale_factor;
	    size_t b = a % nthreads;
		size_t startid = 0;
		//nqueue_handle::nlist = NULL;
		//dqueue_handle::dlist = NULL;

		if(nthreads/num==8)
			b = ((worker_id - startid)%num)*8 + (worker_id - startid)/num;

	    rcu::s_instance.pin_current_thread(b);
	    rcu::s_instance.fault_region();
		int partnum = nthreads/num;
		if(partnum==0)
			partnum = 1;
	//num of threads in the parition


		threadpartid= ((worker_id-startid)/num)%partnum;


		//newseatdrop = newcustdrop = urcon = paymentdrop=transactdrop=0;
	}

public:

    std::vector<bank_worker*>* partition_list;
	uint partition_id;
	//uint64_t sdelt, sfft, sfot, snewt, supct, suprt;
	//int sdelc, sffc, sfoc, snewc, supcc, suprc;
	//int newseatdrop, newcustdrop, urcon,paymentdrop,transactdrop;
    int bankAccountIndex=0;
    int partid;
    int threadpartid;
private:
	string obj_key0;
	string obj_key1;
	string obj_v;
};











class smallbank_account_loader : public bench_loader, public bank_worker_mixin {
public:
	smallbank_account_loader(unsigned long seed, abstract_db *db,
        const map<string, abstract_ordered_index *> &open_tables,
        const map<string, vector<abstract_ordered_index *>> &partitions,
        ssize_t part_id)
    : bench_loader(seed, db, open_tables),
      bank_worker_mixin(partitions),
      part_id(part_id)
    {}

protected:
	virtual void
	load()
	{
		string obj_buf;
		int st_part = part_id, ed_part = part_id;
		if(part_id == -1){
			st_part = 1;
			ed_part = NumPartitions();
		}
		for(int p_id = st_part; p_id <= ed_part; p_id++){
			int bp_id = p_id;
			if(nthreads/(int)scale_factor==8)
				bp_id = p_id*8;
			PinToPartitionId(bp_id);
			for (uint i=0 ; i<NUM_ACCOUNTS; i++) {
				account::key k;
				k.cust_partition = p_id;
				k.cust_id = i;
				account::value v;
                char id_buf[64];
                sprintf(id_buf,"%ld",(long int)i);
                v.cust_name.assign(id_buf);
                account_index::key k_index;
                k_index.cust_partition = p_id;
                k_index.cust_name.assign(id_buf);
                account_index::value vi(i);
				//bool succ = false;
				//printf("account created %d\n",i);
				try {
					void *txn = db->new_txn(txn_flags, arena, txn_buf());
					tbl_account(p_id)->insert(txn, Encode(k), Encode(obj_buf, v));
					tbl_account_index(p_id)->insert(txn, Encode(k_index), Encode(obj_buf, vi));
					ALWAYS_ASSERT(db->commit_txn(txn));
				}
				catch(abstract_db::abstract_abort_exception &ex) {
					ALWAYS_ASSERT(false);
				}


			}
		}
	}

private:
	ssize_t part_id;

};


class smallbank_checking_loader : public bench_loader, public bank_worker_mixin {
public:
	smallbank_checking_loader(unsigned long seed, abstract_db *db,
        const map<string, abstract_ordered_index *> &open_tables,
        const map<string, vector<abstract_ordered_index *>> &partitions,
        ssize_t part_id)
    : bench_loader(seed, db, open_tables),
      bank_worker_mixin(partitions),
      part_id(part_id)
    {}

protected:
	virtual void
	load()
	{
		string obj_buf;
		int st_part = part_id, ed_part = part_id;
		if(part_id == -1){
			st_part = 1;
			ed_part = NumPartitions();
		}
		for(int p_id = st_part; p_id <= ed_part; p_id++){
			int bp_id = p_id;
			if(nthreads/(int)scale_factor==8)
				bp_id = p_id*8;
			PinToPartitionId(bp_id);
			for (uint i=0 ; i<NUM_ACCOUNTS; i++) {
				account::key k;
				k.cust_partition = p_id;
				k.cust_id = i;
				checking::value v;
                v.balance = RandomNumber(r, MIN_BALANCE, MAX_BALANCE);
				//printf("checking created %d\n",i);
				//bool succ = false;
				try {
					void *txn = db->new_txn(txn_flags, arena, txn_buf());
					tbl_checking(p_id)->insert(txn, Encode(k), Encode(obj_buf, v));
					ALWAYS_ASSERT(db->commit_txn(txn));
				}
				catch(abstract_db::abstract_abort_exception &ex) {
					ALWAYS_ASSERT(false);
				}


			}
		}
	}

private:
	ssize_t part_id;

};


class smallbank_saving_loader : public bench_loader, public bank_worker_mixin {
public:
	smallbank_saving_loader(unsigned long seed, abstract_db *db,
        const map<string, abstract_ordered_index *> &open_tables,
        const map<string, vector<abstract_ordered_index *>> &partitions,
        ssize_t part_id)
    : bench_loader(seed, db, open_tables),
      bank_worker_mixin(partitions),
      part_id(part_id)
    {}

protected:
	virtual void
	load()
	{
		string obj_buf;
		int st_part = part_id, ed_part = part_id;
		if(part_id == -1){
			st_part = 1;
			ed_part = NumPartitions();
		}
		for(int p_id = st_part; p_id <= ed_part; p_id++){
			int bp_id = p_id;
			if(nthreads/(int)scale_factor==8)
				bp_id = p_id*8;
			PinToPartitionId(bp_id);
			for (uint i=0 ; i<NUM_ACCOUNTS; i++) {
				account::key k;
				k.cust_partition = p_id;
				k.cust_id = i;
				saving::value v;
                v.balance = RandomNumber(r, MIN_BALANCE, MAX_BALANCE);
				//bool succ = false;
				//printf("saving created %d\n",i);
				try {
					void *txn = db->new_txn(txn_flags, arena, txn_buf());
					tbl_saving(p_id)->insert(txn, Encode(k), Encode(obj_buf, v));
					ALWAYS_ASSERT(db->commit_txn(txn));
				}
				catch(abstract_db::abstract_abort_exception &ex) {
					ALWAYS_ASSERT(false);
				}


			}
		}
	}

private:
	ssize_t part_id;

};



template <typename T>
static vector<T>
unique_filter(const vector<T> &v)
{
  set<T> seen;
  vector<T> ret;
  for (auto &e : v)
    if (!seen.count(e)) {
      ret.emplace_back(e);
      seen.insert(e);
    }
  return ret;
}





bank_worker::txn_result
bank_worker::txn_balance()
{
	const uint64_t read_only_mask = transaction_base::TXN_FLAG_READ_ONLY;
    scoped_str_arena s_arena(arena);
	void *txn = db->new_txn(txn_flags | read_only_mask , arena, txn_buf());
	bool txn_ret = false;
	//printf("balance...\n");
	try{
		//int pa_id = RandomNumber(r, 1, NumPartitions());
		int pa_id = partition_id;
		//int cust_id = RandomNumber(r,1,NUM_ACCOUNTS)-1;
		int cust_id = bankAccountIndex++;
		if (bankAccountIndex==NUM_ACCOUNTS)
		 bankAccountIndex=0;
		char id_buf[64];
		sprintf(id_buf, "%ld", (long int)cust_id);
		account_index::key k_i;
		k_i.cust_partition = pa_id;
		k_i.cust_name.assign(id_buf);
		ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i), obj_v));
		account_index::value v_temp;
		const account_index::value* v_c = Decode(obj_v, v_temp);
		if (v_c->cust_id!=cust_id){
			ALWAYS_ASSERT(false);
		}

        checking::key k_c;
        k_c.cust_partition = pa_id;
        k_c.cust_id = cust_id;
        ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
        checking::value v_c_temp;
        const checking::value* v_c_c = Decode(obj_v,v_c_temp);
        float c_balance = v_c_c->balance;
        c_balance +=1;

        saving::key k_s;
        k_s.cust_partition = pa_id;
        k_s.cust_id = cust_id;
        ALWAYS_ASSERT(tbl_saving(partition_id)->get(txn, Encode(obj_key0, k_s), obj_v));
        saving::value v_s_temp;
        const saving::value* v_c_s = Decode(obj_v,v_s_temp);
        float s_balance = v_c_s->balance;
        s_balance -=1;

        //ALWAYS_ASSERT(c_balance+s_balance>=0);
        txn_ret = db->commit_txn(txn);

        return txn_result(txn_ret, 0);
	}catch (abstract_db::abstract_abort_exception &ex) {
		db->abort_txn(txn);
		txn_ret = false;
	}
	return txn_result(txn_ret, 0);
}




bank_worker::txn_result
bank_worker::txn_amalgate(){
    //cout<<"amal"<<endl;
    scoped_str_arena s_arena(arena);
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
	db->init_txn(txn,cgraph,amalgate_type);
	bool txn_ret = false;
	try{
		int pa_id_0 = partition_id;
		int cust_id_0 = bankAccountIndex++;
		if (bankAccountIndex==NUM_ACCOUNTS)
                 bankAccountIndex=0;
		int pa_id_1 = partition_id;
        int cust_id_1 = bankAccountIndex++;
		if (bankAccountIndex==NUM_ACCOUNTS)
                 bankAccountIndex=0;
        if (cust_id_1<cust_id_0){
        	int tempp = cust_id_1;
        	cust_id_1 = cust_id_0;
        	cust_id_0 = tempp;
        }
		char id_buf_0[64];
		char id_buf_1[64];
		sprintf(id_buf_0, "%ld", (long int)cust_id_0);
		sprintf(id_buf_1, "%ld", (long int)cust_id_1);
		account_index::key k_i_0;
		account_index::key k_i_1;
		k_i_0.cust_partition=pa_id_0;
		k_i_1.cust_partition=pa_id_1;
		k_i_0.cust_name.assign(id_buf_0);
		k_i_1.cust_name.assign(id_buf_1);
		account_index::value v_temp;
		ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i_0), obj_v));
        const account_index::value* v_c_0 = Decode(obj_v, v_temp);
        ALWAYS_ASSERT(v_c_0->cust_id==cust_id_0);
		ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i_1), obj_v));
		const account_index::value* v_c_1 = Decode(obj_v, v_temp);
		ALWAYS_ASSERT(v_c_1->cust_id==cust_id_1);
        float c_balance;
        float s_balance;
      //  float total_balance;

amalgate_piece_1:
        try{
          db->mul_ops_begin(txn);
          checking::key k_c;
          k_c.cust_partition = pa_id_1;
          k_c.cust_id = cust_id_1;
          ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
          checking::value v_c_temp;
          const checking::value* v_c_c = Decode(obj_v,v_c_temp);
          c_balance = v_c_c->balance;
          k_c.cust_partition = pa_id_0;
          k_c.cust_id = cust_id_0;
          v_c_temp.balance = c_balance;
          tbl_checking(partition_id)->put(txn, Encode(str(), k_c), Encode(obj_v, v_c_temp));
            saving::key k_s;
            k_s.cust_partition = pa_id_0;
            k_s.cust_id = cust_id_0;
            ALWAYS_ASSERT(tbl_saving(partition_id)->get(txn, Encode(obj_key0, k_s), obj_v));
            saving::value v_s_temp;
            const saving::value* v_c_s = Decode(obj_v,v_s_temp);
            s_balance = v_c_s->balance;
           // total_balance = s_balance + c_balance;
            k_s.cust_partition = pa_id_1;
            k_s.cust_id = cust_id_1;
            ALWAYS_ASSERT(tbl_saving(partition_id)->get(txn,Encode(obj_key0,k_s),obj_v));
            saving::value v_s_dummy;
            s_balance = Decode(obj_v,v_s_dummy)->balance;
            v_s_temp.balance = s_balance-1;
            tbl_saving(partition_id)->put(txn,Encode(str(),k_s),Encode(obj_v,v_s_temp));
        	if(!db->mul_ops_end(txn))
            goto amalgate_piece_1;
        } catch(piece_abort_exception &ex){
        	db->atomic_ops_abort(txn);
            goto amalgate_piece_1;
        }


        float total_balance = c_balance + s_balance;
        total_balance --;
/*		saving::key k_s;
        k_s.cust_partition = pa_id_0;
        k_s.cust_id = cust_id_0;
        ALWAYS_ASSERT(tbl_saving(partition_id)->get(txn, Encode(obj_key0, k_s), obj_v));
        saving::value v_s_temp;
        const saving::value* v_c_s = Decode(obj_v,v_s_temp);
        float s_balance = v_c_s->balance;*/

   /*     checking::key k_c;
        k_c.cust_partition = pa_id_1;
        k_c.cust_id = cust_id_1;
        ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
        checking::value v_c_temp;
        const checking::value* v_c_c = Decode(obj_v,v_c_temp);
        float c_balance = v_c_c->balance;*/

    //    double total = s_balance + c_balance;

     /*   k_c.cust_partition = pa_id_0;
        k_c.cust_id = cust_id_0;
        //To avoid user abortion
        v_c_temp.balance = c_balance;
        tbl_checking(partition_id)->put(txn, Encode(str(), k_c), Encode(str(), v_c_temp));*/

       /* k_s.cust_partition = pa_id_1;
        k_s.cust_id = cust_id_1;
        tbl_saving(partition_id)->get(txn,Encode(obj_key0,k_s),obj_v);
        saving::value v_s_dummy;
        s_balance = Decode(obj_v,v_s_dummy)->balance;
        //To avoid user abortion
        v_s_temp.balance = s_balance-1;

        tbl_saving(partition_id)->put(txn,Encode(str(),k_s),Encode(str(),v_s_temp));*/
        txn_ret = db->commit_txn(txn);
        return txn_result(txn_ret, 0);

	}catch (abstract_db::abstract_abort_exception &ex) {
		db->abort_txn(txn);
		txn_ret = false;
	}
	return txn_result(txn_ret, 0);

}

bank_worker::txn_result
bank_worker::txn_deposit_checking(){
	//printf("deposit_checking\n");
	scoped_str_arena s_arena(arena);
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
	db->init_txn(txn,cgraph,deposit_checking_type);
	bool txn_ret = false;
    try{
    	int pa_id = partition_id;
		int cust_id = bankAccountIndex++;
		 if (bankAccountIndex==NUM_ACCOUNTS)
                 bankAccountIndex=0;
		char id_buf[64];
		sprintf(id_buf, "%ld", (long int)cust_id);
		account_index::key k_i;
		k_i.cust_partition = pa_id;
		k_i.cust_name.assign(id_buf);
		ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i), obj_v));
		account_index::value v_temp;
		const account_index::value* v_c = Decode(obj_v, v_temp);
		if (v_c->cust_id!=cust_id){
			ALWAYS_ASSERT(false);
		}
deposit_checking_piece_1:
        try{
            db->mul_ops_begin(txn);
            checking::key k_c;
            k_c.cust_partition = pa_id;
            k_c.cust_id = cust_id;
            ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
            checking::value v_c_temp;
            const checking::value* v_c_c = Decode(obj_v,v_c_temp);
            float c_balance = v_c_c->balance;
            v_c_temp.balance = c_balance+1;
            tbl_checking(partition_id)->put(txn, Encode(str(), k_c), Encode(obj_v, v_c_temp));
            if(!db->mul_ops_end(txn))
            goto deposit_checking_piece_1;
        }
        catch(piece_abort_exception &ex){
        	db->atomic_ops_abort(txn);
            goto deposit_checking_piece_1;
        }
        txn_ret = db->commit_txn(txn);
        return txn_result(txn_ret, 0);

    }catch (abstract_db::abstract_abort_exception &ex) {
		db->abort_txn(txn);
		txn_ret = false;
	}
	return txn_result(txn_ret, 0);

}

bank_worker::txn_result
bank_worker::txn_send_payment() {
	//printf("send_payment\n");
	scoped_str_arena s_arena(arena);
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
	db->init_txn(txn,cgraph,send_payment_type);
	bool txn_ret = false;
	try{

    int pa_id_0 = partition_id;

	int pa_id_1 = partition_id;

	int cust_id_0 = bankAccountIndex++;
        if (bankAccountIndex==NUM_ACCOUNTS)
                 bankAccountIndex=0;
	int cust_id_1= bankAccountIndex++;
        if (bankAccountIndex==NUM_ACCOUNTS)
            bankAccountIndex=0;
	int amount=1;
	char id_buf_0[64];
	char id_buf_1[64];
	sprintf(id_buf_0, "%ld", (long int)cust_id_0);
	sprintf(id_buf_1, "%ld", (long int)cust_id_1);
	account_index::key k_i_0;
	account_index::key k_i_1;
	k_i_0.cust_partition=pa_id_0;
	k_i_1.cust_partition=pa_id_1;
	k_i_0.cust_name.assign(id_buf_0);
	k_i_1.cust_name.assign(id_buf_1);
	account_index::value v_temp;
	ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i_0), obj_v));
    const account_index::value* v_cc_0 = Decode(obj_v, v_temp);
    ALWAYS_ASSERT(v_cc_0->cust_id==cust_id_0);
	ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i_1), obj_v));
	const account_index::value* v_cc_1 = Decode(obj_v, v_temp);
	ALWAYS_ASSERT(v_cc_1->cust_id==cust_id_1);


send_payment_piece_1:

    try{
          db->mul_ops_begin(txn);
          checking::key k_c;
          k_c.cust_partition = pa_id_0;
          k_c.cust_id = cust_id_0;
          ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
          checking::value v_c_temp;
          const checking::value* v_c_0 = Decode(obj_v,v_c_temp);
          float balance_0 = v_c_0->balance;

          k_c.cust_partition = pa_id_1;
          k_c.cust_id = cust_id_1;
          ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));

          const checking::value* v_check_1 = Decode(obj_v,v_c_temp);
          float balance_1 = v_check_1->balance;
          v_c_temp.balance = balance_1+amount;
          tbl_checking(partition_id)->put(txn, Encode(str(), k_c), Encode(obj_v, v_c_temp));
          k_c.cust_partition = pa_id_0;
          k_c.cust_id = cust_id_0;
          v_c_temp.balance = balance_0-amount;
          tbl_checking(partition_id)->put(txn, Encode(str(), k_c), Encode(obj_v, v_c_temp));
          if(!db->mul_ops_end(txn))
            goto send_payment_piece_1;
      } catch(piece_abort_exception &ex){
        	db->atomic_ops_abort(txn);
            goto send_payment_piece_1;
      }

      txn_ret = db->commit_txn(txn);
      return txn_result(txn_ret, 0);

  }catch (abstract_db::abstract_abort_exception &ex) {
		db->abort_txn(txn);
		txn_ret = false;
  }
  return txn_result(txn_ret, 0);



}


bank_worker::txn_result
bank_worker::txn_transact_savings() {
	scoped_str_arena s_arena(arena);
 	//printf("transact_saving\n");
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
	db->init_txn(txn,cgraph,transact_savings_type);
	bool txn_ret = false;
	try{
		int pa_id = partition_id;
		int cust_id = bankAccountIndex++;
                if (bankAccountIndex==NUM_ACCOUNTS)
                 bankAccountIndex=0;
		char id_buf[64];
		sprintf(id_buf, "%ld", (long int)cust_id);
		account_index::key k_i;
		k_i.cust_partition = pa_id;
		k_i.cust_name.assign(id_buf);
		ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i), obj_v));
		account_index::value v_temp;
		const account_index::value* v_c = Decode(obj_v, v_temp);
		if (v_c->cust_id!=cust_id){
			ALWAYS_ASSERT(false);
		}
        int amount = 1;

transact_savings_piece_1:
        try{
             db->mul_ops_begin(txn);
             saving::key k_s;
             k_s.cust_partition = pa_id;
             k_s.cust_id = cust_id;
             ALWAYS_ASSERT(tbl_saving(partition_id)->get(txn, Encode(obj_key0, k_s), obj_v));
             saving::value v_s_temp;
             const saving::value* v_c_s = Decode(obj_v,v_s_temp);
             float c_balance = v_c_s->balance;
             v_s_temp.balance = c_balance-amount;
             tbl_saving(partition_id)->put(txn, Encode(str(), k_s), Encode(obj_v, v_s_temp));
             if(!db->mul_ops_end(txn))
               goto transact_savings_piece_1;
       } catch(piece_abort_exception &ex){
        	db->atomic_ops_abort(txn);
            goto transact_savings_piece_1;
        }

        txn_ret = db->commit_txn(txn);
        return txn_result(txn_ret, 0);
	}catch (abstract_db::abstract_abort_exception &ex) {
		db->abort_txn(txn);
		txn_ret = false;
  }
	return txn_result(txn_ret, 0);

}

bank_worker::txn_result
bank_worker::txn_write_check(){
	//printf("write_check\n");
	scoped_str_arena s_arena(arena);
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
    db->init_txn(txn,cgraph,write_check_type);
	bool txn_ret = false;
	try{
		int pa_id = partition_id;
		//int pa_id = RandomNumber(r, 1, NumPartitions());
		//int cust_id = RandomNumber(r,1,NUM_ACCOUNTS)-1;
		int cust_id = bankAccountIndex++;
                if (bankAccountIndex==NUM_ACCOUNTS)
                 bankAccountIndex=0;
		char id_buf[64];
		sprintf(id_buf, "%ld", (long int)cust_id);
		int amount = 1;
		//int amount = RandomNumber(r,1,MIN_BALANCE);
		account_index::key k_i;
		k_i.cust_partition = pa_id;
		k_i.cust_name.assign(id_buf);
		ALWAYS_ASSERT(tbl_account_index(partition_id)->get(txn, Encode(obj_key0, k_i), obj_v));
		account_index::value v_temp;
		const account_index::value* v_c = Decode(obj_v, v_temp);
		if (v_c->cust_id!=cust_id){
			ALWAYS_ASSERT(false);
		}


 write_check_piece_1:
        try{
            db->mul_ops_begin(txn);
            checking::key k_c;
            k_c.cust_partition = pa_id;
            k_c.cust_id = cust_id;
            ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
            checking::value v_c_temp;
            const checking::value* v_c_c = Decode(obj_v,v_c_temp);
            float c_balance = v_c_c->balance;
            saving::key k_s;
            k_s.cust_partition = pa_id;
            k_s.cust_id = cust_id;
            ALWAYS_ASSERT(tbl_saving(partition_id)->get(txn, Encode(obj_key0, k_s), obj_v));
            saving::value v_s_temp;
            const saving::value* v_c_s = Decode(obj_v,v_s_temp);
            float s_balance = v_c_s->balance;

            if (c_balance+s_balance<amount)
              v_c_temp.balance = c_balance-amount+1;
            else
              v_c_temp.balance = c_balance-amount;
            tbl_checking(partition_id)->put(txn, Encode(str(), k_c), Encode(obj_v, v_c_temp));
            if(!db->mul_ops_end(txn))
              goto write_check_piece_1;
        }catch(piece_abort_exception &ex){
        	db->atomic_ops_abort(txn);
            goto write_check_piece_1;
        }

    /*   checking::key k_c;
        k_c.cust_partition = pa_id;
        k_c.cust_id = cust_id;
        ALWAYS_ASSERT(tbl_checking(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
        checking::value v_c_temp;
        const checking::value* v_c_c = Decode(obj_v,v_c_temp);
        float c_balance = v_c_c->balance;
        //v_c_temp.balance = c_balance-1;

        saving::key k_s;
        k_s.cust_partition = pa_id;
        k_s.cust_id = cust_id;
        ALWAYS_ASSERT(tbl_saving(partition_id)->get(txn, Encode(obj_key0, k_s), obj_v));
        saving::value v_s_temp;
        const saving::value* v_c_s = Decode(obj_v,v_s_temp);
        float s_balance = v_c_s->balance;

        if (c_balance+s_balance<amount)
          v_c_temp.balance = c_balance-amount+1;
        else
          v_c_temp.balance = c_balance-amount;

        tbl_checking(partition_id)->put(txn, Encode(str(), k_c), Encode(str(), v_c_temp));*/
		txn_ret = db->commit_txn(txn);
        return txn_result(txn_ret, 0);
	}catch (abstract_db::abstract_abort_exception &ex) {
		db->abort_txn(txn);
		txn_ret = false;
  }
	return txn_result(txn_ret, 0);



}











class small_bench_runner : public bench_runner {

private:
	static vector<abstract_ordered_index *>
	OpenTablesForTablespace(abstract_db *db, const char *name, size_t expected_size)
	{
		const string s_name(name);
		vector<abstract_ordered_index *> ret((int)scale_factor);
		abstract_ordered_index *idx = db->open_index(s_name, expected_size, false);
		for (size_t i = 0; i < scale_factor; i++)
			ret[i] = idx;
		return ret;
	  /*if (NumPartitions() <= nthreads) {
        for (size_t i = 0; i < NumPartitions(); i++)
          ret[i] = db->open_index(s_name + "_" + to_string(i), expected_size, false);
      } else {
        const unsigned nwhse_per_partition = NumPartitions() / nthreads;
        for (size_t partid = 0; partid < nthreads; partid++) {
          const unsigned wstart = partid * nwhse_per_partition;
          const unsigned wend   = (partid + 1 == nthreads) ?
            NumPartitions() : (partid + 1) * nwhse_per_partition;
          abstract_ordered_index *idx =
            db->open_index(s_name + "_" + to_string(partid), expected_size, false);
          for (size_t i = wstart; i < wend; i++)
            ret[i] = idx;
        }
      }
	  return ret;*/
	}

public:
	small_bench_runner(abstract_db *db)
		:bench_runner(db)
	{
#define OPEN_TABLESPACE_X(x) \
		partitions[#x] = OpenTablesForTablespace(db, #x, sizeof(x));

		BANK_TABLE_LIST(OPEN_TABLESPACE_X);
#undef OPEN_TABLESPACE_X
		for (auto &t : partitions) {
			auto v = unique_filter(t.second);
      		for (size_t i = 0; i < v.size(); i++)
        		open_tables[t.first + "_" + to_string(i)] = v[i];
		}
	}

protected:
	virtual vector<bench_loader *>
	make_loaders()
	{
		vector<bench_loader *>ret;

		if (enable_parallel_loading) {
			fast_random r(923587856425);
			for (uint i = 1; i <= scale_factor; i++)
				ret.push_back(new smallbank_account_loader(r.next(),db,open_tables,partitions,i));
		}else{
			    ret.push_back(new smallbank_account_loader(923587856425,db,open_tables,partitions,-1));
		}

		if (enable_parallel_loading) {
			fast_random r(923587856425);
			for (uint i = 1; i <= scale_factor; i++)
				ret.push_back(new smallbank_checking_loader(r.next(),db,open_tables,partitions,i));
		}else{
			    ret.push_back(new smallbank_checking_loader(12345,db,open_tables,partitions,-1));
		}


		if (enable_parallel_loading) {
			fast_random r(923587856425);
			for (uint i = 1; i <= scale_factor; i++)
				ret.push_back(new smallbank_saving_loader(r.next(),db,open_tables,partitions,i));
		}else{
			    ret.push_back(new smallbank_saving_loader(2343352,db,open_tables,partitions,-1));
		}


/*#if 1
		if (enable_parallel_loading) {
			fast_random r(12345);
			for (uint i = 1; i <= scale_factor; i++)
				ret.push_back(new seats_reservation_loader(r.next(), db, open_tables, partitions, i));
		}else{
			ret.push_back(new seats_reservation_loader(12345, db, open_tables, partitions, -1));
		}

		if (enable_parallel_loading) {
			fast_random r(2343352);
			for (uint i = 1; i <= scale_factor; i++)
				ret.push_back(new seats_freqflyer_loader(r.next(), db, open_tables, partitions, i));
		}else{
			ret.push_back(new seats_freqflyer_loader(2343352, db, open_tables, partitions, -1));

		}
#endif*/

		return ret;
	}


	/*virtual vector<bench_worker *>
	make_workers()
	{

		const unsigned alignment = coreid::num_cpus_online();
		const int blockstart =
		  coreid::allocate_contiguous_aligned_block(nthreads, alignment);
		ALWAYS_ASSERT(blockstart >= 0);
		ALWAYS_ASSERT((blockstart % alignment) == 0);
		start_thread_id = blockstart;
		fast_random r(23984543);
		vector<bench_worker *> ret;
		for (size_t i=0; i<nthreads; i++)
			ret.push_back(
				new seats_worker(
					blockstart + i,
		            r.next(), db, open_tables, partitions,
        		    &barrier_a, &barrier_b,
				(i % (int)scale_factor) + 1));
		std::vector<std::vector<seats_worker*>*> all_list;
		for(int i = 0; i < (int)scale_factor; i++){
			all_list.push_back(new std::vector<seats_worker*>());
		}
		for(bench_worker* temp: ret){
			seats_worker* tseat = (seats_worker*)temp;
			all_list[tseat->partition_id-1]->push_back(tseat);
		}
		for(bench_worker* temp: ret){
			seats_worker* tseat = (seats_worker*)temp;
			tseat->partition_list = all_list[tseat->partition_id-1];
		}

		return ret;
	}*/
        virtual vector<bench_worker *>
	make_workers()
	{

		const unsigned alignment = coreid::num_cpus_online();
		const int blockstart =
		  coreid::allocate_contiguous_aligned_block(nthreads, alignment);
		ALWAYS_ASSERT(blockstart >= 0);
		ALWAYS_ASSERT((blockstart % alignment) == 0);
		//int start_thread_id = blockstart;
		fast_random r(23984543);
		vector<bench_worker *> ret;
		for (size_t i=0; i<nthreads; i++)
			ret.push_back(
				new bank_worker(
					blockstart + i,
		            r.next(), db, open_tables, partitions,
        		    &barrier_a, &barrier_b,
				(i % (int)scale_factor) + 1));
		std::vector<std::vector<bank_worker*>*> all_list;
		for(int i = 0; i < (int)scale_factor; i++){
			all_list.push_back(new std::vector<bank_worker*>());
		}
		for(bench_worker* temp: ret){
			bank_worker* tseat = (bank_worker*)temp;
			all_list[tseat->partition_id-1]->push_back(tseat);
		}
		for(bench_worker* temp: ret){
			bank_worker* tseat = (bank_worker*)temp;
			tseat->partition_list = all_list[tseat->partition_id-1];
		}

		return ret;
	}

private:
  map<string, vector<abstract_ordered_index *>> partitions;

};

void
small_do_test(abstract_db *db, int argc, char **argv)
{
	optind = 1;

	cgraph = init_bank_cgraph();
	small_bench_runner r(db);

	r.run();
}
#undef INVARIANT
#define INVARIANT(expr) ALWAYS_ASSERT(expr)
