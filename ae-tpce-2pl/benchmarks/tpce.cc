#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <set>
#include <vector>

#include "../txn.h"
#include "../macros.h"
#include "../scopedperf.hh"
#include "../spinlock.h"

#undef EARLY_ABORT // must not be defined

#include "bench.h"
#include "tpce.h"
//#include "ndb_wrapper.h"

using namespace std;
using namespace util;
using namespace TPCE;

#define PINCPU

// helper global variables
const int str_len = 1024;
std::string min_str(str_len, 0);
std::string max_str(str_len, 127);
inline_str_fixed_tpce<TPCE::sm_cTT_ID_len> type_stop_loss;
inline_str_fixed_tpce<TPCE::sm_cTT_ID_len> type_limit_sell;
inline_str_fixed_tpce<TPCE::sm_cTT_ID_len> type_limit_buy;
inline_str_fixed_tpce<TPCE::sm_cST_ID_len> st_completed_id;
inline_str_fixed_tpce<TPCE::sm_cST_ID_len> st_submitted_id;
INT64 min_int64 = 0x0L;
INT64 max_int64 = 0x7FFFFFFFFFFFFFFFL;
//#define NO_CONFLICT_ON_SYMBOL
//#define MANUAL_MARKET_FEED

TTrade max_load_trade_id = 0;

#define CHECK 1

// Driver Defaults
TIdent iStartFromCustomer = iDefaultStartFromCustomer;
TIdent iCustomerCount = iDefaultCustomerCount;         // # of customers for this instance
TIdent iTotalCustomerCount = iDefaultCustomerCount;    // total number of customers in the database
TIdent customer_selection_count = iTotalCustomerCount;
#ifdef NO_CONFLICT_ON_SYMBOL
TIdent max_security_range = 1;
#else
TIdent max_security_range = 0;
#endif
uint32_t market_order_rate = 60;
uint32_t market_ignore_rate = 0;
UINT iLoadUnitSize = iDefaultLoadUnitSize;           // # of customers in one load unit
UINT iScaleFactor = 500;                             // # of customers for 1 tpsE
UINT iDaysOfInitialTrades = 3;
UINT iHoursOfInitialTrades = iDaysOfInitialTrades * HoursPerWorkDay;
bool bGenerateUsingCache = true;
char szInDir[iMaxPath] = "egen/flat_in";
uint64_t imarket_feed_timeout = 1 * 1 * 1; // in microseconds

double g_zipf_theta = 1;

static inline void 
bind_thread(uint worker_id)
{

#ifdef PINCPU
  const size_t cpu = worker_id % coreid::num_cpus_online();
//   printf("worker_id %ld w/ cpu %ld \n", worker_id,cpu );
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  int s = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (s != 0)
      fprintf(stderr, "pthread_setaffinity_np");
#endif

}

uint32_t
get_num_numa_nodes()
{
    //FIXME
    uint32_t cores_per_numa = 12;
    return nthreads % cores_per_numa ? nthreads / cores_per_numa + 1 : nthreads / cores_per_numa;
}

DataFileManager *dfm = NULL;
// static conflict_graph* cgraph = NULL;
enum tpce_tx_types {
    market_feed_type = 1,
    trade_order_type,
    trade_result_type,
    trade_update_type,
    end_type
};

const size_t type_num = (end_type - market_feed_type);

//TODO
// static inline conflict_graph*
// init_tpce_cgraph()
// {
  // conflict_graph* g = new conflict_graph(type_num);

  // //No need to specify the constraint,
  // //if only one c-edge

  // g->init_txn(market_feed_type, 3);
  // g->init_txn(trade_order_type, 7);
  // g->init_txn(trade_result_type, 8);
  // g->init_txn(trade_update_type, 4);

  // g->set_conflict(market_feed_type, 1, trade_order_type, 3);
  // g->set_conflict(market_feed_type, 2, trade_order_type, 5);
  // g->set_conflict(market_feed_type, 3, trade_order_type, 6);
  // g->set_conflict(market_feed_type, 3, trade_update_type, 2);
  // g->set_conflict(market_feed_type, 3, trade_result_type, 4);

  // g->set_conflict(trade_order_type, 1, trade_result_type, 1);
  // g->set_conflict(trade_order_type, 2, trade_result_type, 2);
  // g->set_conflict(trade_order_type, 4, trade_update_type, 1);
  // g->set_conflict(trade_order_type, 4, trade_result_type, 3);
  // g->set_conflict(trade_order_type, 6, trade_update_type, 2);
  // g->set_conflict(trade_order_type, 6, trade_result_type, 4);
  // g->set_conflict(trade_order_type, 7, trade_result_type, 7);

  // g->set_conflict(trade_update_type, 1, trade_result_type, 3);
  // g->set_conflict(trade_update_type, 2, trade_result_type, 4);
  // g->set_conflict(trade_update_type, 3, trade_result_type, 6);
  // g->set_conflict(trade_update_type, 4, trade_result_type, 8);

  // return g;
// }

#define TPCE_TABLE_LIST(x) \
    x(ACCOUNT_PERMISSION) \
    x(ADDRESS) \
    x(BROKER) \
    x(CASH_TRANSACTION) \
    x(CHARGE) \
    x(COMMISSION_RATE) \
    x(COMPANY) \
    x(COMPANY_COMPETITOR) \
    x(CUSTOMER) \
    x(CUSTOMER_ACCOUNT) \
    x(CUSTOMER_TAXRATE) \
    x(DAILY_MARKET) \
    x(EXCHANGE) \
    x(FINANCIAL) \
    x(HOLDING) \
    x(HOLDING_HISTORY) \
    x(HOLDING_SUMMARY) \
    x(INDUSTRY) \
    x(LAST_TRADE) \
    x(NEWS_ITEM) \
    x(NEWS_XREF) \
    x(SECTOR) \
    x(SECURITY) \
    x(SETTLEMENT) \
    x(STATUS_TYPE) \
    x(TAX_RATE) \
    x(TRADE) \
    x(TRADE_HISTORY) \
    x(TRADE_REQUEST) \
    x(TRADE_TYPE) \
    x(WATCH_ITEM) \
    x(WATCH_LIST) \
    x(ZIP_CODE)

#define TPCE_SECONDARY_INDEX_TABLE_LIST(x) \
    x(BROKER_B_NAME) \
    x(COMPANY_CO_IN_ID) \
    x(COMPANY_CO_NAME) \
    x(CUSTOMER_C_TAX_ID) \
    x(CUSTOMER_ACCOUNT_CA_C_ID) \
    x(HOLDING_H_CA_ID) \
    x(INDUSTRY_SC_ID) \
    x(INDUSTRY_IN_NAME) \
    x(SECTOR_SC_NAME) \
    x(SECURITY_S_CO_ID) \
    x(TRADE_T_CA_ID) \
    x(TRADE_T_S_SYMB) \
    x(TRADE_REQUEST_TR_B_ID) \
    x(TRADE_REQUEST_TR_S_SYMB) \
    x(WATCH_LIST_WL_C_ID)

#define TPCE_TXN_LIST(x) \
    x(broker_volume) \
    x(customer_position) \
    x(market_feed) \
    x(market_watch) \
    x(security_detail) \
    x(trade_lookup) \
    x(trade_order) \
    x(trade_result) \
    x(trade_status) \
    x(trade_update)

uint16_t ACCOUNT_PERMISSION_ = 0;
uint16_t ADDRESS_ = 1;
uint16_t BROKER_ = 2;
uint16_t CASH_TRANSACTION_ = 3;
uint16_t CHARGE_ = 4;
uint16_t COMMISSION_RATE_ = 5;
uint16_t COMPANY_ = 6;
uint16_t COMPANY_COMPETITOR_ = 7;
uint16_t CUSTOMER_ = 8;
uint16_t CUSTOMER_ACCOUNT_ = 9;
uint16_t CUSTOMER_TAXRATE_ = 10;
uint16_t DAILY_MARKET_ = 11;
uint16_t EXCHANGE_ = 12;
uint16_t FINANCIAL_ = 13;
uint16_t HOLDING_ = 14;
uint16_t HOLDING_HISTORY_ = 15;
uint16_t HOLDING_SUMMARY_ = 16;
uint16_t INDUSTRY_ = 17;
uint16_t LAST_TRADE_ = 18;
uint16_t NEWS_ITEM_ = 19;
uint16_t NEWS_XREF_ = 20;
uint16_t SECTOR_ = 21;
uint16_t SECURITY_ = 22;
uint16_t SETTLEMENT_ = 23;
uint16_t STATUS_TYPE_ = 24;
uint16_t TAX_RATE_ = 25;
uint16_t TRADE_ = 26;
uint16_t TRADE_HISTORY_ = 27;
uint16_t TRADE_REQUEST_ = 28;
uint16_t TRADE_TYPE_ = 29;
uint16_t WATCH_ITEM_ = 30;
uint16_t WATCH_LIST_ = 31;
uint16_t ZIP_CODE_ = 32;

uint16_t BROKER_B_NAME_ = 33;
uint16_t COMPANY_CO_IN_ID_ = 34;
uint16_t COMPANY_CO_NAME_ = 35;
uint16_t CUSTOMER_C_TAX_ID_ = 36;
uint16_t CUSTOMER_ACCOUNT_CA_C_ID_ = 37;
uint16_t HOLDING_H_CA_ID_ = 38;
uint16_t INDUSTRY_SC_ID_ = 39;
uint16_t INDUSTRY_IN_NAME_ = 40;
uint16_t SECTOR_SC_NAME_ = 41;
uint16_t SECURITY_S_CO_ID_ = 42;
uint16_t TRADE_T_CA_ID_ = 43;
uint16_t TRADE_T_S_SYMB_ = 44;
uint16_t TRADE_REQUEST_TR_B_ID_ = 45;
uint16_t TRADE_REQUEST_TR_S_SYMB_ = 46;
uint16_t WATCH_LIST_WL_C_ID_ = 47;

uint16_t OFFSET = 5;

uint16_t table_order(uint16_t table_type, uint16_t partition)
{
    return (table_type << OFFSET) + partition;
}


static int g_disable_xpartition_txn = 0;
static int g_disable_read_only_scans = 0;
static int g_enable_partition_locks = 0;
static int g_enable_separate_tree_per_partition = 1; //XXX

static unsigned g_txn_workload_mix[] = {
	6,	// broker_volume,		ce, 	RO	49
	15, // customer_position,	ce, 	RO	130
	0,	// market_feed, 		mee,	RW
	20, // market_watch,		ce, 	RO	180
	16, // security_detail, 	ce, 	RO	140
	9,	// trade_lookup,		ce, 	RO	80
	11, // trade_order, 		ce, 	RW	101
	0,	// trade_result,		mee,	RW
	21, // trade_status,		ce, 	RO	190
	2	// trade_update,		ce, 	RW	20
};

static aligned_padded_elem<spinlock> *g_partition_locks = nullptr;

struct _dummy {}; // exists so we can inherit from it, so we can use a macro in
                  // an init list...
class tpce_worker_mixin : private _dummy {
#define DEFN_TBL_INIT_X(name) \
  , tbl_ ## name ## _vec(partitions.at(#name))

public:
  tpce_worker_mixin(const map<string, vector<abstract_ordered_index *>> &partitions) :
    _dummy() // so hacky...
    TPCE_TABLE_LIST(DEFN_TBL_INIT_X)
    TPCE_SECONDARY_INDEX_TABLE_LIST(DEFN_TBL_INIT_X)
  {
    // initialization
    c = 0;
    alpha = g_zipf_theta;
    // TIdent m_iSecCount = dfm->SecurityFile().GetConfiguredSecurityCount();
    TIdent m_iSecCount = 3425;
    n = m_iSecCount - 1;

    // Compute normalization constant on first call only
    for (int i=1; i<=n; i++)
    c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;

    sum_probs = (double *) malloc((n+1)*sizeof(*sum_probs));
    sum_probs[0] = 0;
    for (int i=1; i<=n; i++) {
        sum_probs[i] = sum_probs[i-1] + c / pow((double) i, alpha);
    }
  }

#undef DEFN_TBL_INIT_X

protected:

#define DEFN_TBL_ACCESSOR_X(name) \
private:  \
  vector<abstract_ordered_index *> tbl_ ## name ## _vec; \
protected: \
  inline ALWAYS_INLINE abstract_ordered_index * \
  tbl_ ## name (unsigned int numa_id) \
  { \
    return tbl_ ## name ## _vec[numa_id - 1]; \
  }

  TPCE_TABLE_LIST(DEFN_TBL_ACCESSOR_X)
  TPCE_SECONDARY_INDEX_TABLE_LIST(DEFN_TBL_ACCESSOR_X)

#undef DEFN_TBL_ACCESSOR_X

public:
  static inline uint32_t
  GetCurrentTimeMillis()
  {
    //struct timeval tv;
    //ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    //return tv.tv_sec * 1000;

    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number
    static __thread uint32_t tl_hack = 1;
    return tl_hack++;
  }

  // utils for generating random #s and strings
  static inline ALWAYS_INLINE int
  CheckBetweenInclusive(int v, int lower, int upper)
  {
    INVARIANT(v >= lower);
    INVARIANT(v <= upper);
    return v;
  }

  inline bool RandomPercent(fast_random &r, int percent)
  {
  	return (RandomNumber(r, 1, 100) <= percent);
  }

  RNGSEED RndNthElement( RNGSEED nSeed, RNGSEED nCount) {
	  UINT64    a = UInt64Rand_A_MULTIPLIER;
	  UINT64    c = UInt64Rand_C_INCREMENT;
	  int       nBit;
	  UINT64    Apow = a;
	  UINT64    Dsum = UInt64Rand_ONE;

	  // if nothing to do, do nothing !
	  if( nCount == 0 ) {
	      return nSeed;
	  }

	  // Recursively compute X(n) = A * X(n-1) + C
	  //
	  // explicitly:
	  // X(n) = A^n * X(0) + { A^(n-1) + A^(n-2) + ... A + 1 } * C
	  //
	  // we write this as:
	  // X(n) = Apow(n) * X(0) + Dsum(n) * C
	  //
	  // we use the following relations:
	  // Apow(n) = A^(n%2)*Apow(n/2)*Apow(n/2)
	  // Dsum(n) =   (n%2)*Apow(n/2)*Apow(n/2) + (Apow(n/2) + 1) * Dsum(n/2)
	  //

	  // first get the highest non-zero bit
	  for( nBit = 0; (nCount >> nBit) != UInt64Rand_ONE ; nBit ++){}

	  // go 1 bit at the time
	  while( --nBit >= 0 ) {
	    Dsum *= (Apow + 1);
	    Apow = Apow * Apow;
	    if( ((nCount >> nBit) % 2) == 1 ) { // odd value
	      Dsum += Apow;
	      Apow *= a;
	    }
	  }
	  nSeed = nSeed * Apow + Dsum * c;
	  return nSeed;
  }

  inline int RndGenerateIntegerPercentage(fast_random &r)
  {
    return( RandomNumber(r, 1, 100));
  }

  static inline ALWAYS_INLINE int
  RandomNumber(fast_random &r, int min, int max)
  {
    return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
  }

  double alpha;
  int n;
  double c;             // Normalization constant
  double *sum_probs;    // Pre-calculated sum of probabilities

  INT64 ZipfInt64(fast_random &r)
  {
    double z;                     // Uniform random number (0 < z < 1)
    int zipf_value;               // Computed exponential value to be returned
    int i;                        // Loop counter
    int low, high, mid;           // Binary-search bounds

    // Pull a uniform random number (0 < z < 1)
    do
    {
        z = r.next_uniform();
    }
    while ((z == 0) || (z == 1));

    // Map z to the value
    low = 1, high = n, mid;
    do {
        mid = floor((low+high)/2);
        if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
            zipf_value = mid;
            break;
        } else if (sum_probs[mid] >= z) {
            high = mid-1;
        } else {
            low = mid+1;
        }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >=1) && (zipf_value <= n));

    return(zipf_value);
  }

  INT64 RndInt64Range(fast_random &r, INT64 min, INT64 max)
  {
    if ( min == max )
        return min;
    // Check on system symbol for 64-bit MAXINT
    //assert( max < MAXINT );
    // This assert would detect when the next line would
    // cause an overflow.
    max++;
    //if ( max <= min )
    //    return max;

    return min + (INT64)(r.next_uniform() * (double)(max - min));
  }

  INT64 RndInt64RangeExclude(fast_random &r, INT64 low, INT64 high, INT64 exclude)
  {
    INT64       temp;

    temp = RndInt64Range(r, low, high-1 );
    if (temp >= exclude)
        temp += 1;

    return temp;
  }

  static inline ALWAYS_INLINE int
  NonUniformRandom(fast_random &r, int A, int C, int min, int max)
  {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

  INT64 NURnd(fast_random &r, INT64 P, INT64 Q, INT32 A, INT32 s )
  {
    return ((( RndInt64Range(r, P, Q) | (RndInt64Range(r, 0, A ) << s )) % (Q - P + 1) ) + P );
  }

  double RndDoubleIncrRange(fast_random &r, double min, double max, double incr)
  {
    INT64 width = (INT64)((max - min) / incr);  // need [0..width], so no +1
    return min + ((double)RndInt64Range(r, 0, width) * incr);
  }

  static inline ALWAYS_INLINE bool
  included(uint id, std::vector<uint> &ids)
  {
      for (uint id_ : ids) {
          if (id == id_)
              return true;
      }
      return false;
  }

  // following oltpbench, we really generate strings of len - 1...
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

  // RandomNStr() actually produces a string of length len
  static inline string
  RandomNStr(fast_random &r, uint len)
  {
    const char base = '0';
    string buf(len, 0);
    for (uint i = 0; i < len; i++)
      buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }
};

#define TBL_LOADER_CLASS_NAME(t) Loader ## t
#define TBL2ROW(t) t ## _ROW

#define TBL_LOADER_DEF(t) \
class TBL_LOADER_CLASS_NAME(t) : public CBaseLoader<TBL2ROW(t)>, public tpce_worker_mixin { \
public: \
    virtual \
    ~TBL_LOADER_CLASS_NAME(t)() {} \
\
    TBL_LOADER_CLASS_NAME(t)( \
            const map<string, vector<abstract_ordered_index *>> &partitions, \
            tpce_loader *tl) \
    : tpce_worker_mixin(partitions), tpce_loader_(tl) \
    {} \
\
    virtual void \
    Init() \
    { \
    } \
\
    virtual void \
    SetOrder(); \
\
    virtual void \
    WriteNextRecord(const TBL2ROW(t) &rec); \
\
    virtual void \
    Commit() \
    { \
    } \
\
    virtual void FinishLoad(); \
private: \
    tpce_loader *tpce_loader_; \
    std::string obj_buf; \
};

class tpce_loader;

TPCE_TABLE_LIST(TBL_LOADER_DEF);

#undef TBL_LOADER_DEF

STATIC_COUNTER_DECL(scopedperf::tsc_ctr, tpce_txn, tpce_txn_cg);

#define TPCE_TXN_ID_ENUM(t) TPCE_ENUM_ ## t

enum {
#define TPCE_TXN_ID_ENUM_ENTRY(t) TPCE_TXN_ID_ENUM(t),
    TPCE_TXN_LIST(TPCE_TXN_ID_ENUM_ENTRY)
#undef TPCE_TXN_ID_ENUM_ENTRY
};



class tpce_worker : public bench_worker, public tpce_worker_mixin {
public:
  tpce_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              const map<string, vector<abstract_ordered_index *>> &partitions,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tpce_worker_mixin(partitions),
      trade_id_(max_load_trade_id + worker_id)
  {
      //TODO
    if (verbose) {
    }

  }

  struct hold_t {
  	TTrade trade_id;
    int_date_time hold_dts;
    float hold_price;
    int32_t hold_qty;

    hold_t() = delete;
    hold_t(TTrade tid, int_date_time dt, float price, int32_t qty)
    	: trade_id(tid), hold_dts(dt), hold_price(price), hold_qty(qty) {}

    bool operator<(const hold_t &rhs) const {
    	if (this == &rhs) return false;
        else if (hold_dts < rhs.hold_dts) return true;
        else if (hold_dts == rhs.hold_dts) {
        	return trade_id < rhs.trade_id;
        }
		else return false;
    }
  };

  struct hold_kv_t {
	HOLDING::key key;
	HOLDING::value *value;

	hold_kv_t() = delete;
	hold_kv_t(const HOLDING::key key_, HOLDING::value *value_)
		: key(key_), value(value_) {}

	bool operator<(const hold_kv_t &rhs) const {
		ALWAYS_ASSERT(value);
		if (this == &rhs) return false;
		else if (value->H_DTS < rhs.value->H_DTS) return true;
		else if (value->H_DTS == rhs.value->H_DTS) {
			return key.H_T_ID < rhs.key.H_T_ID;
		} else return false;
	}
  };

  typedef enum {
	mftt_stop_loss,
	mftt_limit_sell,
	mftt_limit_buy,
  } market_feed_trade_type_t;

  struct trade_request_t {
	TTrade trade_id;
	TIdent broker_id;
	float bid_price;
	market_feed_trade_type_t trade_type;
	int32_t trade_qty;
    bool mark_deleted;
	trade_request_t() = delete;
	trade_request_t(TTrade tid, float bp, market_feed_trade_type_t tt) :
	  trade_id(tid), bid_price(bp), trade_type(tt), mark_deleted(false) {
		broker_id = 0;
		trade_qty = 0;
	}
	bool operator<(const trade_request_t &rhs) const {
		if (this == &rhs) return false;
		if (trade_id < rhs.trade_id) return true;
		else if (trade_id == rhs.trade_id) {
			ALWAYS_ASSERT(broker_id == rhs.broker_id);
			ALWAYS_ASSERT(trade_type == rhs.trade_type);
			ALWAYS_ASSERT(bid_price == rhs.bid_price);
			return false;
		} else return false;
    }
  };

  struct trade_history_t {
      TTrade t_id;
      inline_str_fixed_tpce<TPCE::sm_cST_ID_len> st_id;
      int_date_time dts;
      trade_history_t() = delete;
      trade_history_t(TTrade _t_id,
              const inline_str_fixed_tpce<TPCE::sm_cST_ID_len> &_st_id,
              int_date_time _dts) :
          t_id(_t_id), st_id(_st_id), dts(_dts) {}
      bool operator<(const trade_history_t &rhs) const {
          if (this == &rhs) return false;
          if (dts < rhs.dts) return true;
          else if (dts == rhs.dts)
              if (t_id < rhs.t_id) return true;
              else return false;
          else return false;
      }
  };

  struct mkt_cap_t {
      float new_price;
      int64_t num_out;
      float old_price;
  };

  virtual void insert_holding(
            HOLDING::key &key,
            HOLDING::value &value,
            void *txn);

  virtual void remove_holding(
            const hold_kv_t &kv,
            void *txn);

  virtual void insert_trade_request(
            TRADE_REQUEST::key &key,
            TRADE_REQUEST::value &value,
            void *txn);

  virtual void remove_trade_request(
            const trade_request_t &tr,
            const char *symbol,
            void *txn);

  virtual void insert_trade(
            TRADE::key &key,
            TRADE::value &value,
            void *txn);

  int64_t
  get_unique_trade_id()
  {
      //FIXME
      trade_id_ += nthreads;
      return trade_id_;
  }

  util::fast_random &getR(){
	return r;
  }

  virtual ~tpce_worker()
  {
  }

  txn_result check_txn_queue(bool *run);

#define txn_name(t) txn_ ## t
#define TXN_name(t) TXN ## t

#define TPCE_TXN_FN_DEF(t) \
  txn_result txn_name(t)(); \
  static txn_result \
  TXN_name(t)(bench_worker *worker) \
  { \
  	ANON_REGION("Txn" #t, &tpce_txn_cg); \
  	tpce_worker *w = static_cast<tpce_worker *>(worker); \
        bool is_run; \
        txn_result ret = w->check_txn_queue(&is_run); \
        if (is_run) return ret; \
  	ret = w->txn_name(t)(); \
    if (ret.first) \
      worker->increase_commit(TPCE_TXN_ID_ENUM(t)); \
    else \
      worker->increase_abort(TPCE_TXN_ID_ENUM(t)); \
  	return ret; \
  }

  TPCE_TXN_LIST(TPCE_TXN_FN_DEF)
#undef TPCE_TXN_FN_DEF

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;

    unsigned m = 0;
    for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
      m += g_txn_workload_mix[i];
    ALWAYS_ASSERT(m == 100);

// #define PUSH_WORKLOAD(t) 
	// if(g_txn_workload_mix[TPCE_TXN_ID_ENUM(t)] > 0) 
        // w.push_back(workload_desc( #t, double(g_txn_workload_mix[TPCE_TXN_ID_ENUM(t)])/100.0, TXN_name(t)));
#define PUSH_WORKLOAD(t) \
    w.push_back(workload_desc( #t, double(g_txn_workload_mix[TPCE_TXN_ID_ENUM(t)])/100.0, TXN_name(t)));
    TPCE_TXN_LIST(PUSH_WORKLOAD)
#undef PUSH_WORKLOAD

    return w;
  }

  inline ALWAYS_INLINE string &
  str()
  {
    return *arena.next();
  }


protected:
  TTrade trade_id_;

  virtual void
  on_run_setup() OVERRIDE
  {
    bind_thread(worker_id);
    
    if (pin_cpus) {
      const size_t a = worker_id % coreid::num_cpus_online();
      const size_t b = a % nthreads;
      rcu::s_instance.pin_current_thread(b);
    }

    rcu::s_instance.fault_region();

    bench_worker::on_run_setup();
	m_rnd.SetSeed(RNGSeedBaseTxnInputGenerator);
	m_person = new CPerson(*dfm, iBrokerNameIDShift, true);
	m_pDriverCETxnSettings = new TDriverCETxnSettings;
    m_pDriverCETxnSettings->TO_settings.cur.market = market_order_rate;
    m_pDriverCETxnSettings->TO_settings.cur.limit = 100 - market_order_rate;
	m_CustomerSelection = new CCustomerSelection(&m_rnd, iStartFromCustomer, customer_selection_count/*iTotalCustomerCount*/);
	m_StartTime.Set(
        InitialTradePopulationBaseYear,
        InitialTradePopulationBaseMonth,
        InitialTradePopulationBaseDay,
        InitialTradePopulationBaseHour,
        InitialTradePopulationBaseMinute,
        InitialTradePopulationBaseSecond,
        InitialTradePopulationBaseFraction );

	m_EndTime = m_StartTime;
    m_EndTime.AddWorkMs( (INT64)(iHoursOfInitialTrades * SecondsPerHour + 15 * SecondsPerMinute) * MsPerSecond );

    market_feed_timer_.lap();
    market_feed_time_ = 0;
	marketfeedIndex = 0;
  }

public:
    util::timer market_feed_timer_;
    uint64_t market_feed_time_;
    inline bool
    market_feed_timeout()
    {
        market_feed_time_ += market_feed_timer_.lap();
        if (market_feed_time_ >= imarket_feed_timeout) {
            return true;
        } else
            return false;
    }

    inline bool
    market_feed_ready()
    {
        return this->marketfeedIndex >= TPCE::max_feed_len
            || (this->marketfeedIndex > 0 && this->market_feed_timeout());
    }

    inline void
    market_feed_success()
    {
        market_feed_time_ = 0;
        marketfeedIndex = 0;
    }

	void final_check(){}

	CRandom m_rnd;
	CPerson *m_person;
	CDateTime                                   m_StartTime;
    CDateTime                                   m_EndTime;
	CCustomerSelection *m_CustomerSelection;
	PDriverCETxnSettings	m_pDriverCETxnSettings;

	//first this queue check(prior due to relation of performance)
    std::queue<TTradeResultTxnInput> traderesultqueue;

	//Trade-Order (limitorder[price not fit] first add here)
	//before txn start check the queue to see any price fit TTickerEntry
	//(if any add to marketfeedInput then do marketfeedInput check)
	vector<TTickerEntry> limitorderqueue;

	TMarketFeedTxnInput marketfeedInput; //add content to marketfeedInput.Entries
	INT32 marketfeedIndex; //if marketfeedIndex == max_feed_len then invoke MarketFeed

private:
	// some scratch buffer space
	string obj_key0;
	string obj_key1;
	string obj_v;
};

void copyTTickerEntry(TTickerEntry &to, const TTickerEntry &from)
{
    to.price_quote = from.price_quote;
    to.trade_qty = from.trade_qty;
    memcpy(to.symbol, from.symbol, cSYMBOL_len);
}

tpce_worker::txn_result
tpce_worker::check_txn_queue(bool *run) {
	//util::timer t;
    *run = false;
	txn_result ret;
#ifndef MANUAL_MARKET_FEED
    if (!this->traderesultqueue.empty()) { // trade_result txn
        *run = true;
        ret = this->txn_trade_result();
		if(ret.first) {
            traderesultqueue.pop();
            increase_commit(TPCE_TXN_ID_ENUM(trade_result));
			// increase_commit();
        } else {
            increase_abort(TPCE_TXN_ID_ENUM(trade_result));
        }
    } else if (market_feed_ready()) {
        *run = true;
        ret = this->txn_market_feed();
        if (ret.first) {
            market_feed_success();
            increase_commit(TPCE_TXN_ID_ENUM(market_feed));
            // increase_commit();
        } else {
            increase_abort(TPCE_TXN_ID_ENUM(market_feed));
        }
    } else {
        std::vector<TTickerEntry>::iterator erase = this->limitorderqueue.begin();
		//printf("limitorderqueue size %d %d\n", limitorderqueue.size(), marketfeedIndex);
        for (auto &it : this->limitorderqueue) {
            copyTTickerEntry(this->marketfeedInput.Entries[this->marketfeedIndex++], it);
            erase++;
            if (this->marketfeedIndex >= TPCE::max_feed_len) {
                break;
            }
        }
        this->limitorderqueue.erase(this->limitorderqueue.begin(), erase);
        if (market_feed_ready()) { // market_feed
            *run = true;
            ret = this->txn_market_feed();
			if (ret.first) {
                market_feed_success();
                increase_commit(TPCE_TXN_ID_ENUM(market_feed));
				// increase_commit();
            } else {
                increase_abort(TPCE_TXN_ID_ENUM(market_feed));
            }
        }
    }
    // uint64_t init_time_sum_cyc += t.lap();
#endif
    return ret;
}

class tpce_loader_factory : public CBaseLoaderFactory {
public:
    tpce_loader_factory(
            const map<string, vector<abstract_ordered_index *>> &partitions,
            tpce_loader *tl)
    {
        ap_ = new TBL_LOADER_CLASS_NAME(ACCOUNT_PERMISSION)(partitions, tl);
        ad_ = new TBL_LOADER_CLASS_NAME(ADDRESS)(partitions, tl);
        b_ = new TBL_LOADER_CLASS_NAME(BROKER)(partitions, tl);
        ct_ = new TBL_LOADER_CLASS_NAME(CASH_TRANSACTION)(partitions, tl);
        ch_ = new TBL_LOADER_CLASS_NAME(CHARGE)(partitions, tl);
        cr_ = new TBL_LOADER_CLASS_NAME(COMMISSION_RATE)(partitions, tl);
        co_ = new TBL_LOADER_CLASS_NAME(COMPANY)(partitions, tl);
        cp_ = new TBL_LOADER_CLASS_NAME(COMPANY_COMPETITOR)(partitions, tl);
        c_ = new TBL_LOADER_CLASS_NAME(CUSTOMER)(partitions, tl);
        ca_ = new TBL_LOADER_CLASS_NAME(CUSTOMER_ACCOUNT)(partitions, tl);
        cx_ = new TBL_LOADER_CLASS_NAME(CUSTOMER_TAXRATE)(partitions, tl);
        dm_ = new TBL_LOADER_CLASS_NAME(DAILY_MARKET)(partitions, tl);
        ex_ = new TBL_LOADER_CLASS_NAME(EXCHANGE)(partitions, tl);
        fi_ = new TBL_LOADER_CLASS_NAME(FINANCIAL)(partitions, tl);
        h_ = new TBL_LOADER_CLASS_NAME(HOLDING)(partitions, tl);
        hh_ = new TBL_LOADER_CLASS_NAME(HOLDING_HISTORY)(partitions, tl);
        hs_ = new TBL_LOADER_CLASS_NAME(HOLDING_SUMMARY)(partitions, tl);
        in_ = new TBL_LOADER_CLASS_NAME(INDUSTRY)(partitions, tl);
        lt_ = new TBL_LOADER_CLASS_NAME(LAST_TRADE)(partitions, tl);
        ni_ = new TBL_LOADER_CLASS_NAME(NEWS_ITEM)(partitions, tl);
        nx_ = new TBL_LOADER_CLASS_NAME(NEWS_XREF)(partitions, tl);
        sc_ = new TBL_LOADER_CLASS_NAME(SECTOR)(partitions, tl);
        s_ = new TBL_LOADER_CLASS_NAME(SECURITY)(partitions, tl);
        se_ = new TBL_LOADER_CLASS_NAME(SETTLEMENT)(partitions, tl);
        st_ = new TBL_LOADER_CLASS_NAME(STATUS_TYPE)(partitions, tl);
        tx_ = new TBL_LOADER_CLASS_NAME(TAX_RATE)(partitions, tl);
        t_ = new TBL_LOADER_CLASS_NAME(TRADE)(partitions, tl);
        th_ = new TBL_LOADER_CLASS_NAME(TRADE_HISTORY)(partitions, tl);
        tr_ = new TBL_LOADER_CLASS_NAME(TRADE_REQUEST)(partitions, tl);
        tt_ = new TBL_LOADER_CLASS_NAME(TRADE_TYPE)(partitions, tl);
        wi_ = new TBL_LOADER_CLASS_NAME(WATCH_ITEM)(partitions, tl);
        wl_ = new TBL_LOADER_CLASS_NAME(WATCH_LIST)(partitions, tl);
        zc_ = new TBL_LOADER_CLASS_NAME(ZIP_CODE)(partitions, tl);
    }

    virtual
    ~tpce_loader_factory()
    {
        //XXX EGenGenerateAndLoad.cpp deleted these already...wtf
        //delete ap_;
        //delete ad_;
        //delete b_;
        //delete ct_;
        //delete ch_;
        //delete cr_;
        //delete co_;
        //delete cp_;
        //delete c_;
        //delete ca_;
        //delete cx_;
        //delete dm_;
        //delete ex_;
        //delete fi_;
        //delete h_;
        //delete hh_;
        //delete hs_;
        //delete in_;
        //delete lt_;
        //delete ni_;
        //delete nx_;
        //delete sc_;
        //delete s_;
        //delete se_;
        //delete st_;
        //delete tx_;
        //delete t_;
        //delete th_;
        //delete tr_;
        //delete tt_;
        //delete wi_;
        //delete wl_;
        //delete zc_;
    }

    virtual CBaseLoader<ACCOUNT_PERMISSION_ROW>*
    CreateAccountPermissionLoader()
    {
        return ap_;
    }
    virtual CBaseLoader<ADDRESS_ROW>*
    CreateAddressLoader()
    {
        return ad_;
    }
    virtual CBaseLoader<BROKER_ROW>*
    CreateBrokerLoader()
    {
        return b_;
    }
    virtual CBaseLoader<CASH_TRANSACTION_ROW>*
    CreateCashTransactionLoader()
    {
        return ct_;
    }
    virtual CBaseLoader<CHARGE_ROW>*
    CreateChargeLoader()
    {
        return ch_;
    }
    virtual CBaseLoader<COMMISSION_RATE_ROW>*
    CreateCommissionRateLoader()
    {
        return cr_;
    }
    virtual CBaseLoader<COMPANY_ROW>*
    CreateCompanyLoader()
    {
        return co_;
    }
    virtual CBaseLoader<COMPANY_COMPETITOR_ROW>*
    CreateCompanyCompetitorLoader()
    {
        return cp_;
    }
    virtual CBaseLoader<CUSTOMER_ROW>*
    CreateCustomerLoader()
    {
        return c_;
    }
    virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW>*
    CreateCustomerAccountLoader()
    {
        return ca_;
    }
    virtual CBaseLoader<CUSTOMER_TAXRATE_ROW>*
    CreateCustomerTaxrateLoader()
    {
        return cx_;
    }
    virtual CBaseLoader<DAILY_MARKET_ROW>*
    CreateDailyMarketLoader()
    {
        return dm_;
    }
    virtual CBaseLoader<EXCHANGE_ROW>*
    CreateExchangeLoader()
    {
        return ex_;
    }
    virtual CBaseLoader<FINANCIAL_ROW>*
    CreateFinancialLoader()
    {
        return fi_;
    }
    virtual CBaseLoader<HOLDING_ROW>*
    CreateHoldingLoader()
    {
        return h_;
    }
    virtual CBaseLoader<HOLDING_HISTORY_ROW>*
    CreateHoldingHistoryLoader()
    {
        return hh_;
    }
    virtual CBaseLoader<HOLDING_SUMMARY_ROW>*
    CreateHoldingSummaryLoader()
    {
        return hs_;
    }
    virtual CBaseLoader<INDUSTRY_ROW>*
    CreateIndustryLoader()
    {
        return in_;
    }
    virtual CBaseLoader<LAST_TRADE_ROW>*
    CreateLastTradeLoader()
    {
        return lt_;
    }
    virtual CBaseLoader<NEWS_ITEM_ROW>*
    CreateNewsItemLoader()
    {
        return ni_;
    }
    virtual CBaseLoader<NEWS_XREF_ROW>*
    CreateNewsXRefLoader()
    {
        return nx_;
    }
    virtual CBaseLoader<SECTOR_ROW>*
    CreateSectorLoader()
    {
        return sc_;
    }
    virtual CBaseLoader<SECURITY_ROW>*
    CreateSecurityLoader()
    {
        return s_;
    }
    virtual CBaseLoader<SETTLEMENT_ROW>*
    CreateSettlementLoader()
    {
        return se_;
    }
    virtual CBaseLoader<STATUS_TYPE_ROW>*
    CreateStatusTypeLoader()
    {
        return st_;
    }
    virtual CBaseLoader<TAX_RATE_ROW>*
    CreateTaxRateLoader()
    {
        return tx_;
    }
    virtual CBaseLoader<TRADE_ROW>*
    CreateTradeLoader()
    {
        return t_;
    }
    virtual CBaseLoader<TRADE_HISTORY_ROW>*
    CreateTradeHistoryLoader()
    {
        return th_;
    }
    virtual CBaseLoader<TRADE_REQUEST_ROW>*
    CreateTradeRequestLoader()
    {
        return tr_;
    }
    virtual CBaseLoader<TRADE_TYPE_ROW>*
    CreateTradeTypeLoader()
    {
        return tt_;
    }
    virtual CBaseLoader<WATCH_ITEM_ROW>*
    CreateWatchItemLoader()
    {
        return wi_;
    }
    virtual CBaseLoader<WATCH_LIST_ROW>*
    CreateWatchListLoader()
    {
        return wl_;
    }
    virtual CBaseLoader<ZIP_CODE_ROW>*
    CreateZipCodeLoader()
    {
        return zc_;
    }
private:
    CBaseLoader<ACCOUNT_PERMISSION_ROW> *ap_;
    CBaseLoader<ADDRESS_ROW> *ad_;
    CBaseLoader<BROKER_ROW> *b_;
    CBaseLoader<CASH_TRANSACTION_ROW> *ct_;
    CBaseLoader<CHARGE_ROW> *ch_;
    CBaseLoader<COMMISSION_RATE_ROW> *cr_;
    CBaseLoader<COMPANY_ROW> *co_;
    CBaseLoader<COMPANY_COMPETITOR_ROW> *cp_;
    CBaseLoader<CUSTOMER_ROW> *c_;
    CBaseLoader<CUSTOMER_ACCOUNT_ROW> *ca_;
    CBaseLoader<CUSTOMER_TAXRATE_ROW> *cx_;
    CBaseLoader<DAILY_MARKET_ROW> *dm_;
    CBaseLoader<EXCHANGE_ROW> *ex_;
    CBaseLoader<FINANCIAL_ROW> *fi_;
    CBaseLoader<HOLDING_ROW> *h_;
    CBaseLoader<HOLDING_HISTORY_ROW> *hh_;
    CBaseLoader<HOLDING_SUMMARY_ROW> *hs_;
    CBaseLoader<INDUSTRY_ROW> *in_;
    CBaseLoader<LAST_TRADE_ROW> *lt_;
    CBaseLoader<NEWS_ITEM_ROW> *ni_;
    CBaseLoader<NEWS_XREF_ROW> *nx_;
    CBaseLoader<SECTOR_ROW> *sc_;
    CBaseLoader<SECURITY_ROW> *s_;
    CBaseLoader<SETTLEMENT_ROW> *se_;
    CBaseLoader<STATUS_TYPE_ROW> *st_;
    CBaseLoader<TAX_RATE_ROW> *tx_;
    CBaseLoader<TRADE_ROW> *t_;
    CBaseLoader<TRADE_HISTORY_ROW> *th_;
    CBaseLoader<TRADE_REQUEST_ROW> *tr_;
    CBaseLoader<TRADE_TYPE_ROW> *tt_;
    CBaseLoader<WATCH_ITEM_ROW> *wi_;
    CBaseLoader<WATCH_LIST_ROW> *wl_;
    CBaseLoader<ZIP_CODE_ROW> *zc_;
};

class tpce_loader : public bench_loader, public tpce_worker_mixin {
public:
    tpce_loader(unsigned long seed, abstract_db *db,
            const map<string, abstract_ordered_index *> &open_tables,
            const map<string, vector<abstract_ordered_index *>> &partitions)
        : bench_loader(seed, db, open_tables),
        tpce_worker_mixin(partitions),
        partitions_(partitions)
    {
        txn_max_batch_size_ = db->txn_max_batch_size();
        num_batch_rows_ = 0;
        txn_ = NULL;
    }

    void
    inserted_row()
    {
        ALWAYS_ASSERT(txn_);
        if (txn_max_batch_size_ != -1) {
            num_batch_rows_++;
            if (!(num_batch_rows_ % txn_max_batch_size_)) {
                ALWAYS_ASSERT(db->commit_txn(txn_));
                txn_ = db->new_txn(txn_flags, arena, txn_buf());
                arena.reset();
            }
        }
    }

    void *
    get_txn()
    {
        if (!txn_) {
            txn_ = db->new_txn(txn_flags, arena, txn_buf());
            arena.reset();
        }
        return txn_;
    }

    void
    commit_txn()
    {
        if(txn_) {
            ALWAYS_ASSERT(db->commit_txn(txn_));
            txn_ = NULL;
        }
    }

    virtual void
    load()
    {
        char szLogFileName[64];
        snprintf(&szLogFileName[0], sizeof(szLogFileName),
                 "EGenLoaderFrom%" PRId64 "To%" PRId64 ".log",
                 iStartFromCustomer, (iStartFromCustomer + iCustomerCount)-1);
        // Create log formatter and logger instance
        CLogFormatTab fmt;
        CEGenLogger logger(eDriverEGenLoader, 0, szLogFileName, &fmt);
        // Set up data file manager for lazy load.
        dfm = new DataFileManager(std::string(szInDir), iTotalCustomerCount, iTotalCustomerCount);

        tpce_loader_factory *load_factory = new tpce_loader_factory(partitions_, this);
        CGenerateAndLoadStandardOutput Output;
        // Create the main class instance
        CGenerateAndLoad *cgal = new CGenerateAndLoad(
                *dfm,
                iCustomerCount,
                iStartFromCustomer,
                iTotalCustomerCount,
                iLoadUnitSize,
                iScaleFactor,
                iDaysOfInitialTrades,
                load_factory,
                &logger,
                &Output,
                szInDir,
                bGenerateUsingCache);
        //printf("start loading fixed tables\n");
        cgal->GenerateAndLoadFixedTables();
        //printf("start loading scaling tables\n");
        cgal->GenerateAndLoadScalingTables();
        //printf("start loading growing tables\n");
        cgal->GenerateAndLoadGrowingTables();
        //printf("finish loading\n");

        delete cgal;
        delete load_factory;
        if (verbose) {
        }
    }
private:
    ssize_t txn_max_batch_size_, num_batch_rows_;
    void *txn_;
    const map<string, vector<abstract_ordered_index *>> &partitions_;
};

// FinishLoad function
#define TBL_LOADER_FINISH_FUNC_DEF(tblname) \
void \
TBL_LOADER_CLASS_NAME(tblname)::FinishLoad() \
{ \
    tpce_loader_->commit_txn(); \
}
TPCE_TABLE_LIST(TBL_LOADER_FINISH_FUNC_DEF);
#undef TBL_LOADER_FINISH_FUNC_DEF

// load AP
void
TBL_LOADER_CLASS_NAME(ACCOUNT_PERMISSION)::WriteNextRecord(const TBL2ROW(ACCOUNT_PERMISSION) &rec)
{
    ACCOUNT_PERMISSION::key k;
    k.AP_CA_ID = rec.AP_CA_ID;
    k.AP_TAX_ID.assign(rec.AP_TAX_ID);
    ACCOUNT_PERMISSION::value v;
    v.AP_ACL.assign(rec.AP_ACL);
    v.AP_L_NAME.assign(rec.AP_L_NAME);
    v.AP_F_NAME.assign(rec.AP_F_NAME);
    void *txn = tpce_loader_->get_txn();
    tbl_ACCOUNT_PERMISSION(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

// load address
void
TBL_LOADER_CLASS_NAME(ADDRESS)::WriteNextRecord(const TBL2ROW(ADDRESS) &rec)
{
    ADDRESS::key k;
    k.AD_ID = rec.AD_ID;
    ADDRESS::value v;
#ifndef SMALLER_DATA_SET
    v.AD_LINE1.assign(rec.AD_LINE1);
    v.AD_LINE2.assign(rec.AD_LINE2);
#endif
    v.AD_ZC_CODE.assign(rec.AD_ZC_CODE);
#ifndef SMALLER_DATA_SET
    v.AD_CTRY.assign(rec.AD_CTRY);
#endif
    void *txn = tpce_loader_->get_txn();
    tbl_ADDRESS(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

// load broker
void
TBL_LOADER_CLASS_NAME(BROKER)::WriteNextRecord(const TBL2ROW(BROKER) &rec)
{
    {
        BROKER::key k;
        k.B_ID = rec.B_ID;
        BROKER::value v;
        v.B_ST_ID.assign(rec.B_ST_ID);
        v.B_NAME.assign(rec.B_NAME);
        v.B_NUM_TRADES = rec.B_NUM_TRADES;
        v.B_COMM_TOTAL = rec.B_COMM_TOTAL;
        void *txn = tpce_loader_->get_txn();
        tbl_BROKER(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    //secondary
    {
        BROKER_B_NAME::key k;
        k.B1_B_NAME.assign(rec.B_NAME);
        k.B1_B_ID_K = rec.B_ID;
        BROKER_B_NAME::value v;
        v.B1_B_ID_V = rec.B_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_BROKER_B_NAME(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

// load cash txn
void
TBL_LOADER_CLASS_NAME(CASH_TRANSACTION)::WriteNextRecord(const TBL2ROW(CASH_TRANSACTION) &rec)
{
    CASH_TRANSACTION::key k;
    k.CT_T_ID = rec.CT_T_ID;
    CASH_TRANSACTION::value v;
#ifndef SMALLER_DATA_SET
    v.CT_DTS = rec.CT_DTS.GetTime();
    v.CT_AMT = rec.CT_AMT;
#endif
    v.CT_NAME.assign(rec.CT_NAME);
    void *txn = tpce_loader_->get_txn();
    tbl_CASH_TRANSACTION(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

// load charge
void
TBL_LOADER_CLASS_NAME(CHARGE)::WriteNextRecord(const TBL2ROW(CHARGE) &rec)
{
    CHARGE::key k;
    k.CH_TT_ID.assign(rec.CH_TT_ID);
    k.CH_C_TIER = rec.CH_C_TIER;
    CHARGE::value v;
    v.CH_CHRG = rec.CH_CHRG;
    void *txn = tpce_loader_->get_txn();
    tbl_CHARGE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(COMMISSION_RATE)::WriteNextRecord(const TBL2ROW(COMMISSION_RATE) &rec)
{
    COMMISSION_RATE::key k;
    k.CR_C_TIER = rec.CR_C_TIER;
    k.CR_TT_ID.assign(rec.CR_TT_ID);
    k.CR_EX_ID.assign(rec.CR_EX_ID);
    k.CR_FROM_QTY = rec.CR_FROM_QTY;
    COMMISSION_RATE::value v;
    v.CR_TO_QTY = rec.CR_TO_QTY;
    v.CR_RATE = rec.CR_RATE;
    void *txn = tpce_loader_->get_txn();
    tbl_COMMISSION_RATE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(COMPANY)::WriteNextRecord(const TBL2ROW(COMPANY) &rec)
{
    {
        COMPANY::key k;
        k.CO_ID = rec.CO_ID;
        COMPANY::value v;
        v.CO_ST_ID.assign(rec.CO_ST_ID);
        v.CO_NAME.assign(rec.CO_NAME);
        v.CO_IN_ID.assign(rec.CO_IN_ID);
#ifndef SMALLER_DATA_SET
        v.CO_SP_RATE.assign(rec.CO_SP_RATE);
        v.CO_CEO.assign(rec.CO_CEO);
#endif
        v.CO_AD_ID = rec.CO_AD_ID;
#ifndef SMALLER_DATA_SET
        v.CO_DESC.assign(rec.CO_DESC);
#endif
        v.CO_OPEN_DATE = rec.CO_OPEN_DATE.GetTime();
        void *txn = tpce_loader_->get_txn();
        tbl_COMPANY(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    //secondary
    {
        COMPANY_CO_IN_ID::key k;
        k.CO1_CO_IN_ID.assign(rec.CO_IN_ID);
        k.CO1_CO_ID_K = rec.CO_ID;
        COMPANY_CO_IN_ID::value v;
        v.CO1_CO_ID_V = rec.CO_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_COMPANY_CO_IN_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        COMPANY_CO_NAME::key k;
        k.CO2_CO_NAME.assign(rec.CO_NAME);
        k.CO2_CO_ID_K = rec.CO_ID;
        COMPANY_CO_NAME::value v;
        v.CO2_CO_ID_V = rec.CO_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_COMPANY_CO_NAME(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(COMPANY_COMPETITOR)::WriteNextRecord(const TBL2ROW(COMPANY_COMPETITOR) &rec)
{
    COMPANY_COMPETITOR::key k;
    k.CP_CO_ID = rec.CP_CO_ID;
    k.CP_COMP_CO_ID = rec.CP_COMP_CO_ID;
    k.CP_IN_ID.assign(rec.CP_IN_ID);
    COMPANY_COMPETITOR::value v;
    v.dummy_value = 0;
    void *txn = tpce_loader_->get_txn();
    tbl_COMPANY_COMPETITOR(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(CUSTOMER)::WriteNextRecord(const TBL2ROW(CUSTOMER) &rec)
{
    {
        CUSTOMER::key k;
        k.C_ID = rec.C_ID;
        CUSTOMER::value v;
        v.C_TAX_ID.assign(rec.C_TAX_ID);
        v.C_ST_ID.assign(rec.C_ST_ID);
        v.C_L_NAME.assign(rec.C_L_NAME);
        v.C_F_NAME.assign(rec.C_F_NAME);
#ifndef SMALLER_DATA_SET
        v.C_M_NAME.assign(rec.C_M_NAME);
        v.C_GNDR = rec.C_GNDR;
#endif
        v.C_TIER = rec.C_TIER;
#ifndef SMALLER_DATA_SET
        v.C_DOB = rec.C_DOB.GetTime();
#endif
        v.C_AD_ID = rec.C_AD_ID;
#ifndef SMALLER_DATA_SET
        v.C_CTRY_1.assign(rec.C_CTRY_1);
        v.C_AREA_1.assign(rec.C_AREA_1);
        v.C_LOCAL_1.assign(rec.C_LOCAL_1);
        v.C_EXT_1.assign(rec.C_EXT_1);
        v.C_CTRY_2.assign(rec.C_CTRY_2);
        v.C_AREA_2.assign(rec.C_AREA_2);
        v.C_LOCAL_2.assign(rec.C_LOCAL_2);
        v.C_EXT_2.assign(rec.C_EXT_2);
        v.C_CTRY_3.assign(rec.C_CTRY_3);
        v.C_AREA_3.assign(rec.C_AREA_3);
        v.C_LOCAL_3.assign(rec.C_LOCAL_3);
        v.C_EXT_3.assign(rec.C_EXT_3);
        v.C_EMAIL_1.assign(rec.C_EMAIL_1);
        v.C_EMAIL_2.assign(rec.C_EMAIL_2);
#endif
        void *txn = tpce_loader_->get_txn();
        tbl_CUSTOMER(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        CUSTOMER_C_TAX_ID::key k;
        k.C1_C_TAX_ID.assign(rec.C_TAX_ID);
        k.C1_C_ID_K = rec.C_ID;
        CUSTOMER_C_TAX_ID::value v;
        v.C1_C_ID_V = rec.C_ID;

        void *txn = tpce_loader_->get_txn();
        tbl_CUSTOMER_C_TAX_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(CUSTOMER_ACCOUNT)::WriteNextRecord(const TBL2ROW(CUSTOMER_ACCOUNT) &rec)
{
    {
        CUSTOMER_ACCOUNT::key k;
        k.CA_ID = rec.CA_ID;
        CUSTOMER_ACCOUNT::value v;
        v.CA_B_ID = rec.CA_B_ID;
        v.CA_C_ID = rec.CA_C_ID;
#ifndef SMALLER_DATA_SET
        v.CA_NAME.assign(rec.CA_NAME);
#endif
        v.CA_TAX_ST = rec.CA_TAX_ST;
        v.CA_BAL = rec.CA_BAL;
        void *txn = tpce_loader_->get_txn();
        tbl_CUSTOMER_ACCOUNT(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        CUSTOMER_ACCOUNT_CA_C_ID::key k;
        k.CA1_CA_C_ID = rec.CA_C_ID;
        k.CA1_CA_ID_K = rec.CA_ID;
        CUSTOMER_ACCOUNT_CA_C_ID::value v;
        v.CA1_CA_ID_V = rec.CA_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_CUSTOMER_ACCOUNT_CA_C_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(CUSTOMER_TAXRATE)::WriteNextRecord(const TBL2ROW(CUSTOMER_TAXRATE) &rec)
{
    CUSTOMER_TAXRATE::key k;
    k.CX_TX_ID.assign(rec.CX_TX_ID);
    k.CX_C_ID = rec.CX_C_ID;
    CUSTOMER_TAXRATE::value v(0);
    void *txn = tpce_loader_->get_txn();
    tbl_CUSTOMER_TAXRATE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(DAILY_MARKET)::WriteNextRecord(const TBL2ROW(DAILY_MARKET) &rec)
{
    DAILY_MARKET::key k;
    k.DM_DATE = rec.DM_DATE.GetTime();
    k.DM_S_SYMB.assign(rec.DM_S_SYMB);
    DAILY_MARKET::value v;
    v.DM_CLOSE = rec.DM_CLOSE;
    v.DM_HIGH = rec.DM_HIGH;
    v.DM_LOW = rec.DM_LOW;
    v.DM_VOL = rec.DM_VOL;
    void *txn = tpce_loader_->get_txn();
    tbl_DAILY_MARKET(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(EXCHANGE)::WriteNextRecord(const TBL2ROW(EXCHANGE) &rec)
{
    EXCHANGE::key k;
    k.EX_ID.assign(rec.EX_ID);
    EXCHANGE::value v;
#ifndef SMALLER_DATA_SET
    v.EX_NAME.assign(rec.EX_NAME);
#endif
    v.EX_NUM_SYMB = v.EX_NUM_SYMB;
    v.EX_OPEN = rec.EX_OPEN;
    v.EX_CLOSE = rec.EX_CLOSE;
#ifndef SMALLER_DATA_SET
    v.EX_DESC.assign(rec.EX_DESC);
#endif
    v.EX_AD_ID = rec.EX_AD_ID;
    void *txn = tpce_loader_->get_txn();
    tbl_EXCHANGE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(FINANCIAL)::WriteNextRecord(const TBL2ROW(FINANCIAL) &rec)
{
    FINANCIAL::key k;
    k.FI_CO_ID = rec.FI_CO_ID;
    k.FI_YEAR = rec.FI_YEAR;
    k.FI_QTR = rec.FI_QTR;
    FINANCIAL::value v;
    v.FI_QTR_START_DATE = rec.FI_QTR_START_DATE.GetTime();
    v.FI_REVENUE = rec.FI_REVENUE;
    v.FI_NET_EARN = rec.FI_NET_EARN;
    v.FI_BASIC_EPS = rec.FI_BASIC_EPS;
    v.FI_DILUT_EPS = rec.FI_DILUT_EPS;
    v.FI_MARGIN = rec.FI_MARGIN;
    v.FI_INVENTORY = rec.FI_INVENTORY;
    v.FI_ASSETS = rec.FI_ASSETS;
    v.FI_LIABILITY = rec.FI_LIABILITY;
    v.FI_OUT_BASIC = rec.FI_OUT_BASIC;
    v.FI_OUT_DILUT = rec.FI_OUT_DILUT;
    void *txn = tpce_loader_->get_txn();
    tbl_FINANCIAL(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(HOLDING)::WriteNextRecord(const TBL2ROW(HOLDING) &rec)
{
    {
        HOLDING::key k;
        k.H_T_ID = rec.H_T_ID;
        HOLDING::value v;
        v.H_CA_ID = rec.H_CA_ID;
        v.H_S_SYMB.assign(rec.H_S_SYMB);
        v.H_DTS = rec.H_DTS.GetTime();
        v.H_PRICE = rec.H_PRICE;
        v.H_QTY = rec.H_QTY;
        void *txn = tpce_loader_->get_txn();
        tbl_HOLDING(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        HOLDING_H_CA_ID::key k;
        k.H1_H_CA_ID = rec.H_CA_ID;
        k.H1_H_S_SYMB.assign(rec.H_S_SYMB);
        k.H1_H_T_ID_K = rec.H_T_ID;
        HOLDING_H_CA_ID::value v;
        v.H1_H_T_ID_V = rec.H_T_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_HOLDING_H_CA_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(HOLDING_HISTORY)::WriteNextRecord(const TBL2ROW(HOLDING_HISTORY) &rec)
{
#ifndef SMALLER_DATA_SET
    HOLDING_HISTORY::key k;
    k.HH_T_ID = rec.HH_T_ID;
    k.HH_H_T_ID = rec.HH_H_T_ID;
    HOLDING_HISTORY::value v;
    v.HH_BEFORE_QTY = rec.HH_BEFORE_QTY;
    v.HH_AFTER_QTY = rec.HH_AFTER_QTY;
    void *txn = tpce_loader_->get_txn();
    tbl_HOLDING_HISTORY(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
#endif
}

void
TBL_LOADER_CLASS_NAME(HOLDING_SUMMARY)::WriteNextRecord(const TBL2ROW(HOLDING_SUMMARY) &rec)
{
    HOLDING_SUMMARY::key k;
    k.HS_CA_ID = rec.HS_CA_ID;
    k.HS_S_SYMB.assign(rec.HS_S_SYMB);
    HOLDING_SUMMARY::value v;
    ALWAYS_ASSERT(rec.HS_QTY != 0);
    v.HS_QTY = rec.HS_QTY;
    void *txn = tpce_loader_->get_txn();
    tbl_HOLDING_SUMMARY(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(INDUSTRY)::WriteNextRecord(const TBL2ROW(INDUSTRY) &rec)
{
    {
        INDUSTRY::key k;
        k.IN_ID.assign(rec.IN_ID);
        INDUSTRY::value v;
        v.IN_NAME.assign(rec.IN_NAME);
        v.IN_SC_ID.assign(rec.IN_SC_ID);
        void *txn = tpce_loader_->get_txn();
        tbl_INDUSTRY(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        INDUSTRY_SC_ID::key k;
        k.IN1_IN_SC_ID.assign(rec.IN_SC_ID);
        k.IN1_IN_ID_K.assign(rec.IN_ID);
        INDUSTRY_SC_ID::value v;
        v.IN1_IN_ID_V.assign(rec.IN_ID);
        void *txn = tpce_loader_->get_txn();
        tbl_INDUSTRY_SC_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        INDUSTRY_IN_NAME::key k;
        k.IN2_IN_NAME.assign(rec.IN_NAME);
        k.IN2_IN_ID_K.assign(rec.IN_ID);
        INDUSTRY_IN_NAME::value v;
        v.IN2_IN_ID_V.assign(rec.IN_ID);
        void *txn = tpce_loader_->get_txn();
        tbl_INDUSTRY_IN_NAME(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(LAST_TRADE)::WriteNextRecord(const TBL2ROW(LAST_TRADE) &rec)
{
    LAST_TRADE::key k;
    k.LT_S_SYMB.assign(rec.LT_S_SYMB);
    LAST_TRADE::value v;
    v.LT_DTS = rec.LT_DTS.GetTime();
    v.LT_PRICE = rec.LT_PRICE;
    v.LT_OPEN_PRICE = rec.LT_OPEN_PRICE;
    v.LT_VOL = rec.LT_VOL;
    void *txn = tpce_loader_->get_txn();
    tbl_LAST_TRADE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(NEWS_ITEM)::WriteNextRecord(const TBL2ROW(NEWS_ITEM) &rec)
{
    NEWS_ITEM::key k;
    k.NI_ID = rec.NI_ID;
    NEWS_ITEM::value v;
    //v.NI_HEADLINE.assign(rec.NI_HEADLINE);
    //v.NI_SUMMARY.assign(rec.NI_SUMMARY);
    //v.NI_ITEM.assign(rec.NI_ITEM); unused field
    //v.NI_DTS = rec.NI_DTS.GetTime();
    //v.NI_SOURCE.assign(rec.NI_SOURCE);
    //v.NI_AUTHOR.assign(rec.NI_AUTHOR);
    v.dummy_value = 0;
    void *txn = tpce_loader_->get_txn();
    tbl_NEWS_ITEM(1)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(NEWS_XREF)::WriteNextRecord(const TBL2ROW(NEWS_XREF) &rec)
{
    NEWS_XREF::key k;
    k.NX_CO_ID = rec.NX_CO_ID;
    k.NX_NI_ID = rec.NX_NI_ID;
    NEWS_XREF::value v(0);
    void *txn = tpce_loader_->get_txn();
    tbl_NEWS_XREF(1)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(SECTOR)::WriteNextRecord(const TBL2ROW(SECTOR) &rec)
{
    {
        SECTOR::key k;
        k.SC_ID.assign(rec.SC_ID);
        SECTOR::value v;
        v.SC_NAME.assign(rec.SC_NAME);
        void *txn = tpce_loader_->get_txn();
        tbl_SECTOR(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        SECTOR_SC_NAME::key k;
        k.SC1_SC_NAME.assign(rec.SC_NAME);
        k.SC1_SC_ID_K.assign(rec.SC_ID);
        SECTOR_SC_NAME::value v;
        v.SC1_SC_ID_V.assign(rec.SC_ID);
        void *txn = tpce_loader_->get_txn();
        tbl_SECTOR_SC_NAME(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(SECURITY)::WriteNextRecord(const TBL2ROW(SECURITY) &rec)
{
    {
        SECURITY::key k;
        k.S_SYMB.assign(rec.S_SYMB);
        SECURITY::value v;
        v.S_ISSUE.assign(rec.S_ISSUE);
        v.S_ST_ID.assign(rec.S_ST_ID);
        v.S_NAME.assign(rec.S_NAME);
        v.S_EX_ID.assign(rec.S_EX_ID);
        v.S_CO_ID = rec.S_CO_ID;
        v.S_NUM_OUT = rec.S_NUM_OUT;
        v.S_START_DATE = rec.S_START_DATE.GetTime();
        v.S_EXCH_DATE = rec.S_EXCH_DATE.GetTime();
        v.S_PE = rec.S_PE;
        v.S_52WK_HIGH = rec.S_52WK_HIGH;
        v.S_52WK_HIGH_DATE = rec.S_52WK_HIGH_DATE.GetTime();
        v.S_52WK_LOW = rec.S_52WK_LOW;
        v.S_52WK_LOW_DATE = rec.S_52WK_LOW_DATE.GetTime();
        v.S_DIVIDEND = rec.S_DIVIDEND;
        v.S_YIELD = rec.S_YIELD;
        void *txn = tpce_loader_->get_txn();
        tbl_SECURITY(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        SECURITY_S_CO_ID::key k;
        k.S1_S_CO_ID = rec.S_CO_ID;
        k.S1_S_ISSUE.assign(rec.S_ISSUE);
        k.S1_S_SYMB_K.assign(rec.S_SYMB);
        SECURITY_S_CO_ID::value v;
        v.S1_S_SYMB_V.assign(rec.S_SYMB);
        void *txn = tpce_loader_->get_txn();
        tbl_SECURITY_S_CO_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(SETTLEMENT)::WriteNextRecord(const TBL2ROW(SETTLEMENT) &rec)
{
    SETTLEMENT::key k;
    k.SE_T_ID = rec.SE_T_ID;
    SETTLEMENT::value v;
    v.SE_CASH_TYPE.assign(rec.SE_CASH_TYPE);
#ifndef SMALLER_DATA_SET
    v.SE_CASH_DUE_DATE = rec.SE_CASH_DUE_DATE.GetTime();
    v.SE_AMT = rec.SE_AMT;
#endif
    void *txn = tpce_loader_->get_txn();
    tbl_SETTLEMENT(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(STATUS_TYPE)::WriteNextRecord(const TBL2ROW(STATUS_TYPE) &rec)
{
    if (0 == strcmp(rec.ST_NAME, "Completed"))
        st_completed_id.assign(rec.ST_ID);
    else if (0 == strcmp(rec.ST_NAME, "Submitted"))
        st_submitted_id.assign(rec.ST_ID);
    STATUS_TYPE::key k;
    k.ST_ID.assign(rec.ST_ID);
    STATUS_TYPE::value v;
    v.ST_NAME.assign(rec.ST_NAME);
    void *txn = tpce_loader_->get_txn();
    tbl_STATUS_TYPE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(TAX_RATE)::WriteNextRecord(const TBL2ROW(TAX_RATE) &rec)
{
    TAX_RATE::key k;
    k.TX_ID.assign(rec.TX_ID);
    TAX_RATE::value v;
#ifndef SMALLER_DATA_SET
    v.TX_NAME.assign(rec.TX_NAME);
#endif
    v.TX_RATE = rec.TX_RATE;
    void *txn = tpce_loader_->get_txn();
    tbl_TAX_RATE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(TRADE)::WriteNextRecord(const TBL2ROW(TRADE) &rec)
{
    if (rec.T_ID > max_load_trade_id)
        max_load_trade_id = rec.T_ID;
    {
        TRADE::key k;
        k.T_ID = rec.T_ID;
        TRADE::value v;
        v.T_DTS = rec.T_DTS.GetTime();
        v.T_ST_ID.assign(rec.T_ST_ID);
        v.T_TT_ID.assign(rec.T_TT_ID);
        v.T_IS_CASH = bool2char(rec.T_IS_CASH);
        v.T_S_SYMB.assign(rec.T_S_SYMB);
        v.T_QTY = rec.T_QTY;
        v.T_BID_PRICE = rec.T_BID_PRICE;
        v.T_CA_ID = rec.T_CA_ID;
        v.T_EXEC_NAME.assign(rec.T_EXEC_NAME);
        v.T_TRADE_PRICE = rec.T_TRADE_PRICE;
        v.T_CHRG = rec.T_CHRG;
        v.T_COMM = rec.T_COMM;
        v.T_TAX = rec.T_TAX;
        v.T_LIFO = bool2char(rec.T_LIFO);
        void *txn = tpce_loader_->get_txn();
        tbl_TRADE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        TRADE_T_CA_ID::key k;
        k.T1_T_CA_ID = rec.T_CA_ID;
        k.T1_T_DTS = rec.T_DTS.GetTime();
        k.T1_T_ID_K = rec.T_ID;
        TRADE_T_CA_ID::value v;
        v.T1_T_ID_V = rec.T_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_TRADE_T_CA_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        TRADE_T_S_SYMB::key k;
        k.T2_T_S_SYMB.assign(rec.T_S_SYMB);
        k.T2_T_DTS = rec.T_DTS.GetTime();
        k.T2_T_ID_K = rec.T_ID;
        TRADE_T_S_SYMB::value v;
        v.T2_T_ID_V = rec.T_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_TRADE_T_S_SYMB(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(TRADE_HISTORY)::WriteNextRecord(const TBL2ROW(TRADE_HISTORY) &rec)
{
    TRADE_HISTORY::key k;
    k.TH_T_ID = rec.TH_T_ID;
    k.TH_ST_ID.assign(rec.TH_ST_ID);
    TRADE_HISTORY::value v;
    v.TH_DTS = rec.TH_DTS.GetTime();
    void *txn = tpce_loader_->get_txn();
    tbl_TRADE_HISTORY(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(TRADE_REQUEST)::WriteNextRecord(const TBL2ROW(TRADE_REQUEST) &rec)
{
    {
        TRADE_REQUEST::key k;
        k.TR_T_ID = rec.TR_T_ID;
        TRADE_REQUEST::value v;
        v.TR_TT_ID.assign(rec.TR_TT_ID);
        v.TR_S_SYMB.assign(rec.TR_S_SYMB);
        v.TR_QTY = rec.TR_QTY;
        v.TR_BID_PRICE = rec.TR_BID_PRICE;
        v.TR_B_ID = rec.TR_B_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_TRADE_REQUEST(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        TRADE_REQUEST_TR_B_ID::key k;
        k.TR1_TR_B_ID = rec.TR_B_ID;
        k.TR1_TR_S_SYMB.assign(rec.TR_S_SYMB);
        k.TR1_TR_T_ID_K = rec.TR_T_ID;
        TRADE_REQUEST_TR_B_ID::value v;
        v.TR1_TR_T_ID_V = rec.TR_T_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_TRADE_REQUEST_TR_B_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        TRADE_REQUEST_TR_S_SYMB::key k;
        k.TR2_TR_S_SYMB.assign(rec.TR_S_SYMB);
        k.TR2_TR_TT_ID.assign(rec.TR_TT_ID);
        k.TR2_TR_BID_PRICE = (int64_t)(rec.TR_BID_PRICE*100);
        k.TR2_TR_T_ID_K = rec.TR_T_ID;
        TRADE_REQUEST_TR_S_SYMB::value v;
        v.TR2_TR_T_ID_V = rec.TR_T_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_TRADE_REQUEST_TR_S_SYMB(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(TRADE_TYPE)::WriteNextRecord(const TBL2ROW(TRADE_TYPE) &rec)
{
    TRADE_TYPE::key k;
    k.TT_ID.assign(rec.TT_ID);
    TRADE_TYPE::value v;
    v.TT_NAME.assign(rec.TT_NAME);
    v.TT_IS_SELL = bool2char(rec.TT_IS_SELL);
    v.TT_IS_MRKT = bool2char(rec.TT_IS_MRKT);
    void *txn = tpce_loader_->get_txn();
    tbl_TRADE_TYPE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
    type_stop_loss.assign("TMB");
    type_limit_sell.assign("TLS");
    type_limit_buy.assign("TLB");
}

void
TBL_LOADER_CLASS_NAME(WATCH_ITEM)::WriteNextRecord(const TBL2ROW(WATCH_ITEM) &rec)
{
    WATCH_ITEM::key k;
    k.WI_WL_ID = rec.WI_WL_ID;
    k.WI_S_SYMB.assign(rec.WI_S_SYMB);
    WATCH_ITEM::value v(0);
    void *txn = tpce_loader_->get_txn();
    tbl_WATCH_ITEM(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(WATCH_LIST)::WriteNextRecord(const TBL2ROW(WATCH_LIST) &rec)
{
    {
        WATCH_LIST::key k;
        k.WL_ID = rec.WL_ID;
        WATCH_LIST::value v;
        v.WL_C_ID = rec.WL_C_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_WATCH_LIST(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }

    {
        WATCH_LIST_WL_C_ID::key k;
        k.WL1_WL_C_ID = rec.WL_C_ID;
        k.WL1_WL_ID_K = rec.WL_ID;
        WATCH_LIST_WL_C_ID::value v;
        v.WL1_WL_ID_V = rec.WL_ID;
        void *txn = tpce_loader_->get_txn();
        tbl_WATCH_LIST_WL_C_ID(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
        tpce_loader_->inserted_row();
    }
}

void
TBL_LOADER_CLASS_NAME(ZIP_CODE)::WriteNextRecord(const TBL2ROW(ZIP_CODE) &rec)
{
    ZIP_CODE::key k;
    k.ZC_CODE.assign(rec.ZC_CODE);
    ZIP_CODE::value v;
    v.ZC_TOWN.assign(rec.ZC_TOWN);
    v.ZC_DIV.assign(rec.ZC_DIV);
    void *txn = tpce_loader_->get_txn();
    tbl_ZIP_CODE(1/*TODO partition*/)->insert(txn, Encode(k), Encode(obj_buf, v));
    tpce_loader_->inserted_row();
}

void
TBL_LOADER_CLASS_NAME(ACCOUNT_PERMISSION)::SetOrder()
{
    tbl_ACCOUNT_PERMISSION(1)->set_table_order(table_order(ACCOUNT_PERMISSION_, 1));
}

void
TBL_LOADER_CLASS_NAME(ADDRESS)::SetOrder()
{
    tbl_ADDRESS(1)->set_table_order(table_order(ADDRESS_, 1));
}

void
TBL_LOADER_CLASS_NAME(BROKER)::SetOrder()
{
    tbl_BROKER(1)->set_table_order(table_order(BROKER_, 1));
    tbl_BROKER_B_NAME(1)->set_table_order(table_order(BROKER_B_NAME_, 1));
}

void
TBL_LOADER_CLASS_NAME(CASH_TRANSACTION)::SetOrder()
{
    tbl_CASH_TRANSACTION(1/*TODO partition*/)->set_table_order(table_order(CASH_TRANSACTION_, 1));
}

void
TBL_LOADER_CLASS_NAME(CHARGE)::SetOrder()
{
    tbl_CHARGE(1/*TODO partition*/)->set_table_order(table_order(CHARGE_, 1));
}

void
TBL_LOADER_CLASS_NAME(COMMISSION_RATE)::SetOrder()
{
    tbl_COMMISSION_RATE(1/*TODO partition*/)->set_table_order(table_order(COMMISSION_RATE_, 1));
}

void
TBL_LOADER_CLASS_NAME(COMPANY)::SetOrder()
{
    tbl_COMPANY(1/*TODO partition*/)->set_table_order(table_order(COMPANY_, 1));
    tbl_COMPANY_CO_IN_ID(1/*TODO partition*/)->set_table_order(table_order(COMPANY_CO_IN_ID_, 1));
    tbl_COMPANY_CO_NAME(1/*TODO partition*/)->set_table_order(table_order(COMPANY_CO_NAME_, 1));
}

void
TBL_LOADER_CLASS_NAME(COMPANY_COMPETITOR)::SetOrder()
{
    tbl_COMPANY_COMPETITOR(1/*TODO partition*/)->set_table_order(table_order(COMPANY_COMPETITOR_, 1));
}

void
TBL_LOADER_CLASS_NAME(CUSTOMER)::SetOrder()
{
    tbl_CUSTOMER(1/*TODO partition*/)->set_table_order(table_order(CUSTOMER_, 1));
    tbl_CUSTOMER_C_TAX_ID(1/*TODO partition*/)->set_table_order(table_order(CUSTOMER_ACCOUNT_CA_C_ID_, 1));
}

void
TBL_LOADER_CLASS_NAME(CUSTOMER_ACCOUNT)::SetOrder()
{
    tbl_CUSTOMER_ACCOUNT(1/*TODO partition*/)->set_table_order(table_order(CUSTOMER_ACCOUNT_, 1));
    tbl_CUSTOMER_ACCOUNT_CA_C_ID(1/*TODO partition*/)->set_table_order(table_order(CUSTOMER_ACCOUNT_CA_C_ID_, 1));
}

void
TBL_LOADER_CLASS_NAME(CUSTOMER_TAXRATE)::SetOrder()
{
    tbl_CUSTOMER_TAXRATE(1/*TODO partition*/)->set_table_order(table_order(CUSTOMER_TAXRATE_, 1));
}

void
TBL_LOADER_CLASS_NAME(DAILY_MARKET)::SetOrder()
{
    tbl_DAILY_MARKET(1/*TODO partition*/)->set_table_order(table_order(DAILY_MARKET_, 1));
}

void
TBL_LOADER_CLASS_NAME(EXCHANGE)::SetOrder()
{
    tbl_EXCHANGE(1/*TODO partition*/)->set_table_order(table_order(EXCHANGE_, 1));
}

void
TBL_LOADER_CLASS_NAME(FINANCIAL)::SetOrder()
{
    tbl_FINANCIAL(1/*TODO partition*/)->set_table_order(table_order(FINANCIAL_, 1));
}

void
TBL_LOADER_CLASS_NAME(HOLDING)::SetOrder()
{
    tbl_HOLDING(1/*TODO partition*/)->set_table_order(table_order(HOLDING_, 1));
    tbl_HOLDING_H_CA_ID(1/*TODO partition*/)->set_table_order(table_order(HOLDING_H_CA_ID_, 1));
}

void
TBL_LOADER_CLASS_NAME(HOLDING_HISTORY)::SetOrder()
{
    tbl_HOLDING_HISTORY(1/*TODO partition*/)->set_table_order(table_order(HOLDING_HISTORY_, 1));
}

void
TBL_LOADER_CLASS_NAME(HOLDING_SUMMARY)::SetOrder()
{
    tbl_HOLDING_SUMMARY(1/*TODO partition*/)->set_table_order(table_order(HOLDING_SUMMARY_, 1));
}

void
TBL_LOADER_CLASS_NAME(INDUSTRY)::SetOrder()
{
    tbl_INDUSTRY(1/*TODO partition*/)->set_table_order(table_order(INDUSTRY_, 1));
    tbl_INDUSTRY_SC_ID(1/*TODO partition*/)->set_table_order(table_order(INDUSTRY_SC_ID_, 1));
    tbl_INDUSTRY_IN_NAME(1/*TODO partition*/)->set_table_order(table_order(INDUSTRY_IN_NAME_, 1));
}

void
TBL_LOADER_CLASS_NAME(LAST_TRADE)::SetOrder()
{
    tbl_LAST_TRADE(1/*TODO partition*/)->set_table_order(table_order(LAST_TRADE_, 1));
}

void
TBL_LOADER_CLASS_NAME(NEWS_ITEM)::SetOrder()
{
    tbl_NEWS_ITEM(1)->set_table_order(table_order(NEWS_ITEM_, 1));
}

void
TBL_LOADER_CLASS_NAME(NEWS_XREF)::SetOrder()
{
    tbl_NEWS_XREF(1)->set_table_order(table_order(NEWS_XREF_, 1));
}

void
TBL_LOADER_CLASS_NAME(SECTOR)::SetOrder()
{
    tbl_SECTOR(1/*TODO partition*/)->set_table_order(table_order(SECTOR_, 1));
    tbl_SECTOR_SC_NAME(1/*TODO partition*/)->set_table_order(table_order(SECTOR_SC_NAME_, 1));
}

void
TBL_LOADER_CLASS_NAME(SECURITY)::SetOrder()
{
    tbl_SECURITY(1/*TODO partition*/)->set_table_order(table_order(SECURITY_, 1));
    tbl_SECURITY_S_CO_ID(1/*TODO partition*/)->set_table_order(table_order(SECURITY_S_CO_ID_, 1));
}

void
TBL_LOADER_CLASS_NAME(SETTLEMENT)::SetOrder()
{
    tbl_SETTLEMENT(1/*TODO partition*/)->set_table_order(table_order(SETTLEMENT_, 1));
}

void
TBL_LOADER_CLASS_NAME(STATUS_TYPE)::SetOrder()
{
    tbl_STATUS_TYPE(1/*TODO partition*/)->set_table_order(table_order(STATUS_TYPE_, 1));
}

void
TBL_LOADER_CLASS_NAME(TAX_RATE)::SetOrder()
{
    tbl_TAX_RATE(1/*TODO partition*/)->set_table_order(table_order(TAX_RATE_, 1));
}

void
TBL_LOADER_CLASS_NAME(TRADE)::SetOrder()
{
    tbl_TRADE(1/*TODO partition*/)->set_table_order(table_order(TRADE_, 1));
    tbl_TRADE_T_CA_ID(1/*TODO partition*/)->set_table_order(table_order(TRADE_T_CA_ID_, 1));
    tbl_TRADE_T_S_SYMB(1/*TODO partition*/)->set_table_order(table_order(TRADE_T_S_SYMB_, 1));
}

void
TBL_LOADER_CLASS_NAME(TRADE_HISTORY)::SetOrder()
{
    tbl_TRADE_HISTORY(1/*TODO partition*/)->set_table_order(table_order(TRADE_HISTORY_, 1));
}

void
TBL_LOADER_CLASS_NAME(TRADE_REQUEST)::SetOrder()
{
    tbl_TRADE_REQUEST(1/*TODO partition*/)->set_table_order(table_order(TRADE_REQUEST_, 1));
    tbl_TRADE_REQUEST_TR_B_ID(1/*TODO partition*/)->set_table_order(table_order(TRADE_REQUEST_TR_B_ID_, 1));
    tbl_TRADE_REQUEST_TR_S_SYMB(1/*TODO partition*/)->set_table_order(table_order(TRADE_REQUEST_TR_S_SYMB_, 1));
}

void
TBL_LOADER_CLASS_NAME(TRADE_TYPE)::SetOrder()
{
    tbl_TRADE_TYPE(1/*TODO partition*/)->set_table_order(table_order(TRADE_TYPE_, 1));
}

void
TBL_LOADER_CLASS_NAME(WATCH_ITEM)::SetOrder()
{
    tbl_WATCH_ITEM(1/*TODO partition*/)->set_table_order(table_order(WATCH_ITEM_, 1));
}

void
TBL_LOADER_CLASS_NAME(WATCH_LIST)::SetOrder()
{
    tbl_WATCH_LIST(1/*TODO partition*/)->set_table_order(table_order(WATCH_LIST_, 1));
    tbl_WATCH_LIST_WL_C_ID(1/*TODO partition*/)->set_table_order(table_order(WATCH_LIST_WL_C_ID_, 1));
}

void
TBL_LOADER_CLASS_NAME(ZIP_CODE)::SetOrder()
{
    tbl_ZIP_CODE(1/*TODO partition*/)->set_table_order(table_order(ZIP_CODE_, 1));
}

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

//TODO: callback not added yet

class scan_callback_normal : public abstract_ordered_index::scan_callback {
public:
    scan_callback_normal(bool ignore_key)
        : n(0), ignore_key(ignore_key)
    {
    }

    ~scan_callback_normal(){
        values.clear();
    }

    bool invoke(const char *keyp, size_t keylen, const string &value)
    {
		std::string *mvalue = new std::string(value);
        if (ignore_key) {
            values.emplace_back(nullptr, mvalue);
        } else {
        	std::string *mkey = new std::string(keyp, keylen);
            values.emplace_back(mkey, mvalue);
        }
        return true;
    }

    inline size_t
    size() const
    {
    	return values.size();
    }

    typedef std::pair<std::string *, std::string *> kv_pair;
    typename std::vector<kv_pair> values;

private:
    size_t n;
    bool ignore_key;

};


template <size_t N>
class scan_limit_callback : public abstract_ordered_index::scan_callback {
public:
    scan_limit_callback(bool ignore_key)
        : n(0), ignore_key(ignore_key)
    {
        static_assert(N > 0, "xx");
    }

    ~scan_limit_callback(){
        values.clear();
    }

    bool invoke(const char *keyp, size_t keylen, const string &value)
    {
        INVARIANT(n < N);
		std::string *mvalue = new std::string(value);
        if (ignore_key) {
            values.emplace_back(nullptr, mvalue);
        } else {
        	std::string *mkey = new std::string(keyp, keylen);
            values.emplace_back(mkey, mvalue);
        }
        return ++n < N;
    }

    inline size_t
    size() const
    {
    	return values.size();
    }

    typedef std::pair<std::string *, std::string *> kv_pair;
    typename util::vec<kv_pair, N>::type values;

private:
    size_t n;
    bool ignore_key;

};


class trade_request_scan_callback : public abstract_ordered_index::scan_callback {
public:
    trade_request_scan_callback(bool ignore_key) : ignore_key(ignore_key) {}

    ~trade_request_scan_callback(){
        values.clear();
    }

    bool
    invoke(const char *keyp, size_t keylen, const string &value)
    {
    	TRADE_REQUEST_TR_S_SYMB::value *mvalue = new TRADE_REQUEST_TR_S_SYMB::value;
		const TRADE_REQUEST_TR_S_SYMB::value *v_tr = Decode(value, *mvalue);
        if (ignore_key) {
            values.emplace_back(nullptr, (TRADE_REQUEST_TR_S_SYMB::value*)v_tr);
        } else {
        	TRADE_REQUEST_TR_S_SYMB::key *mkey = new TRADE_REQUEST_TR_S_SYMB::key;
			const TRADE_REQUEST_TR_S_SYMB::key *k_tr = Decode(keyp, *mkey);
			values.emplace_back((TRADE_REQUEST_TR_S_SYMB::key *)k_tr, (TRADE_REQUEST_TR_S_SYMB::value *)v_tr);
        }
        return true;
    }

    inline size_t
    size() const
    {
        return values.size();
    }

    typedef std::pair<TRADE_REQUEST_TR_S_SYMB::key *, TRADE_REQUEST_TR_S_SYMB::value *> kv_pair;
    typename std::vector<kv_pair> values;

private:
    bool ignore_key;
};


//TODO end for callback

//start of helper functions and structs

TIdent getBrokerCount(){
	return iCustomerCount / iBrokersDiv;
}

TIdent getStartFromBroker(){
	return ((iStartFromCustomer / iBrokersDiv) + iStartingBrokerID + iTIdentShift);
}

TIdent GenerateRandomBrokerId(tpce_worker *worker_){
	return worker_->RndInt64Range(worker_->getR(), getStartFromBroker(), getStartFromBroker()+getBrokerCount()-1);
}

void GenerateBrokerName(TIdent B_ID, char *B_NAME, size_t B_NAME_len, CPerson *m_person){
   snprintf(B_NAME, B_NAME_len, "%s %c. %s",
       m_person->GetFirstName(B_ID + iBrokerNameIDShift).c_str(),
       m_person->GetMiddleName(B_ID + iBrokerNameIDShift),
       m_person->GetLastName(B_ID + iBrokerNameIDShift).c_str());
}

void GenerateNonUniformRandomCustomerId(TIdent &iCustomerId, eCustomerTier &iCustomerTier, CCustomerSelection *m_CustomerSelection)
{
    m_CustomerSelection->GenerateRandomCustomer(iCustomerId, iCustomerTier);
}

int GetNumberOfAccounts(TIdent CID, eCustomerTier iCustomerTier, CCustomerSelection *m_CustomerSelection){
	int iMinAccountCount;
	int iMod;
	int iInverseCID;

	iMinAccountCount = iMinAccountsPerCustRange[iCustomerTier - eCustomerTierOne];
	iMod = iMaxAccountsPerCustRange[iCustomerTier - eCustomerTierOne] - iMinAccountCount + 1;
	iInverseCID = m_CustomerSelection->GetInverseCID(CID);

	// Note: the calculations below assume load unit contains 1000 customers.
	//
	if (iInverseCID < 200){      // Tier 1
		return ((iInverseCID % iMod) + iMinAccountCount);
	}
	else{
		if (iInverseCID < 800){  // Tier 2
			return (((iInverseCID - 200 + 1) % iMod) + iMinAccountCount);
		}
		else{                     // Tier 3
			return (((iInverseCID - 800 + 2) % iMod) + iMinAccountCount);
		}
	}
}

TIdent GetStartingCA_ID(TIdent CID)
{
    //start account ids on the next boundary for the new customer
    return ((CID-1) * iMaxAccountsPerCust + 1);
}

TIdent GetEndingCA_ID(TIdent CID)
{
    return (CID + iTIdentShift) * iMaxAccountsPerCust;
}

int GetNumPermsForCA(TIdent CA_ID, tpce_worker *worker_)
{
        RNGSEED OldSeed;
        int     iThreshold;
        int     iNumberOfPermissions;

        OldSeed = worker_->getR().get_seed();

        worker_->getR().set_seed( worker_->RndNthElement( RNGSeedBaseNumberOfAccountPermissions, CA_ID ));

        iThreshold = worker_->RndGenerateIntegerPercentage(worker_->getR());

        if (iThreshold <= (int)iPercentAccountAdditionalPermissions_0)
        {
            iNumberOfPermissions = 0;   //60% of accounts have just the owner row permissions
        }
        else
        {
            if (iThreshold <= (int)(iPercentAccountAdditionalPermissions_0 +
                iPercentAccountAdditionalPermissions_1))
            {
                iNumberOfPermissions = 1;   //38% of accounts have one additional permisison row
            }
            else
            {
                iNumberOfPermissions = 2;   //2% of accounts have two additional permission rows
            }
        }

        worker_->getR().set_seed( OldSeed );
        return( iNumberOfPermissions );
}

void GetCIDsForPermissions(TIdent CA_ID, TIdent Owner_CID, tpce_worker *worker_, TIdent *CID_1, TIdent *CID_2)
{
        RNGSEED OldSeed;

        if (CID_1 == NULL)
            return;

        OldSeed = worker_->getR().get_seed();
        worker_->getR().set_seed( worker_->RndNthElement( RNGSeedBaseCIDForPermission1, CA_ID ));

        // Select from a fixed range that doesn't depend on the number of customers in the database.
        // This allows not to specify the total number of customers to EGenLoader, only how many
        // a particular instance needs to generate (may be a fraction of total).
        // Note: this is not implemented right now.
        *CID_1 = worker_->RndInt64RangeExclude(worker_->getR(), iStartFromCustomer,
                                        iStartFromCustomer + iAccountPermissionIDRange,
                                        Owner_CID);

        if (CID_2 != NULL)
        {
            // NOTE: Reseeding the RNG here for the second CID value. The use of this sequence
            // is fuzzy because the number of RNG values consumed is dependant on not only the
            // CA_ID, but also the CID value chosen above for the first permission. Using a
            // different sequence here may help prevent potential overlaps that might occur if
            // the same sequence from above were used.
            worker_->getR().set_seed( worker_->RndNthElement( RNGSeedBaseCIDForPermission2, CA_ID ));
            do  //make sure the second id is different from the first
            {
                *CID_2 = worker_->RndInt64RangeExclude(worker_->getR(), iStartFromCustomer,
                                                iStartFromCustomer + iAccountPermissionIDRange,
                                                Owner_CID);
            }
            while (*CID_2 == *CID_1);
        }

        worker_->getR().set_seed( OldSeed );
}

void GenerateRandomAccountId(TIdent  iCustomerId, eCustomerTier   iCustomerTier, tpce_worker *worker_,
                             TIdent* piCustomerAccount, int*  piAccountCount){
	TIdent  iCustomerAccount;
	int     iAccountCount;
	TIdent  iStartingAccount;

    iAccountCount = GetNumberOfAccounts(iCustomerId, iCustomerTier, worker_->m_CustomerSelection);

    iStartingAccount = GetStartingCA_ID(iCustomerId);

    // Select random account for the customer
    //
   	iCustomerAccount = worker_->RndInt64Range(worker_->getR(), iStartingAccount,
                                            iStartingAccount + iAccountCount - 1);

    if (piCustomerAccount != NULL)
       *piCustomerAccount = iCustomerAccount;

    if (piAccountCount != NULL)
       *piAccountCount = iAccountCount;
}

TIdent GenerateRandomCustomerAccountId(tpce_worker *worker_){
	TIdent iCustomerId;
	eCustomerTier iCustomerTier;
	GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier, worker_->m_CustomerSelection);
	TIdent ret;
	GenerateRandomAccountId(iCustomerId, iCustomerTier, worker_, &ret, NULL);
	return ret;
}


bool IsAbortedTrade(TIdent TradeId){
   bool bResult = false;
   if( iAbortedTradeModFactor == TradeId % iAbortTrade )
      {
         bResult = true;
      }
   return bResult;
}


TTrade GenerateNonUniformTradeID( INT32 AValue, INT32 SValue, tpce_worker *worker_)
{
    TTrade TradeId;
	INT64 m_iMaxActivePrePopulatedTradeID = (INT64)((iHoursOfInitialTrades*SecondsPerHour*(iTotalCustomerCount/iScaleFactor )) * iAbortTrade / 100 );

    TradeId = worker_->NURnd(worker_->getR(), 1, m_iMaxActivePrePopulatedTradeID, AValue, SValue );

    // Skip over trade id's that were skipped over during load time.
    if (IsAbortedTrade(TradeId) )
    {
        TradeId++;
    }

    TradeId += iTTradeShift;    // shift trade id to 64-bit value

    return( TradeId );
}

void GenerateNonUniformTradeDTS(TPCE::TIMESTAMP_STRUCT &dts, INT64 MaxTimeInMilliSeconds, INT32 AValue, INT32 SValue, tpce_worker *worker_)
{
    CDateTime   TradeTime(  InitialTradePopulationBaseYear,
                            InitialTradePopulationBaseMonth,
                            InitialTradePopulationBaseDay,
                            InitialTradePopulationBaseHour,
                            InitialTradePopulationBaseMinute,
                            InitialTradePopulationBaseSecond,
                            InitialTradePopulationBaseFraction );   //NOTE: Interpretting Fraction as milliseconds,
                                                                    // probably 0 anyway.
    INT64       TradeTimeOffset;

    // Generate random number of seconds from the base time.
    //
    TradeTimeOffset = worker_->NURnd(worker_->getR(), 1, MaxTimeInMilliSeconds, AValue, SValue );

    // The time we have is an offset into the initial pre-populated trading time.
    // This needs to be converted into a "real" time taking into account 8 hour
    // business days, etc.

    TradeTime.AddWorkMs( TradeTimeOffset );
    TradeTime.GetTimeStamp( &dts );
}

int GetNumberOfSecurities(TIdent iCA_ID, eCustomerTier iTier, int iAccountCount, tpce_worker *worker_)
{
        RNGSEED OldSeed;
        int     iNumberOfSecurities;
        int     iMinRange, iMaxRange;   // for convenience

        iMinRange = iMinSecuritiesPerAccountRange[iTier - eCustomerTierOne][iAccountCount - 1];
        iMaxRange = iMaxSecuritiesPerAccountRange[iTier - eCustomerTierOne][iAccountCount - 1];
        INVARIANT(iMinRange != 0);
        INVARIANT(iMaxRange != 0);

        OldSeed = worker_->getR().get_seed();
        worker_->getR().set_seed( worker_->RndNthElement( RNGSeedBaseNumberOfSecurities, iCA_ID ));
        iNumberOfSecurities = worker_->RandomNumber(worker_->getR(), iMinRange, iMaxRange);
        worker_->getR().set_seed( OldSeed );
        return( iNumberOfSecurities );
}

RNGSEED GetStartingSecIDSeed(TIdent iCA_ID, tpce_worker *worker_)
{
  return( worker_->RndNthElement( RNGSeedBaseStartingSecurityID, iCA_ID * iMaxSecuritiesPerAccount ));
}


TIdent GetSecurityFlatFileIndex(TIdent  iCustomerAccount, UINT    iSecurityAccountIndex, tpce_worker *worker_)
{
        RNGSEED OldSeed;
        TIdent  iSecurityFlatFileIndex = 0; // index of the selected security in the input flat file
        UINT    iGeneratedIndexCount = 0;   // number of currently generated unique flat file indexes
        UINT    i;

        OldSeed = worker_->getR().get_seed();
        worker_->getR().set_seed( GetStartingSecIDSeed( iCustomerAccount, worker_));

        iGeneratedIndexCount = 0;
		TIdent m_iSecCount = dfm->SecurityFile().GetConfiguredSecurityCount();
		TIdent                      m_SecurityIds[iMaxSecuritiesPerAccount];

        while (iGeneratedIndexCount < iSecurityAccountIndex)
        {
            // iSecurityFlatFileIndex = worker_->RndInt64Range(worker_->getR(), 0, m_iSecCount-1);
            iSecurityFlatFileIndex = worker_->ZipfInt64(worker_->getR());
            // m_SecurityIds is not used in benchmark anyway
            break;

            for (i = 0; i < iGeneratedIndexCount; ++i)
            {
                if (m_SecurityIds[i] == iSecurityFlatFileIndex)
                    break;
            }

            // If a duplicate is found, overwrite it in the same location
            // so basically no changes are made.
            //
            m_SecurityIds[i] = iSecurityFlatFileIndex;

            // If no duplicate is found, increment the count of unique ids
            //
            if (i == iGeneratedIndexCount)
            {
                ++iGeneratedIndexCount;
            }
        }

        worker_->getR().set_seed( OldSeed );

        // iSecurityFlatFileIndex = max_security_range > 0 ?
        //     iSecurityFlatFileIndex % max_security_range :
        //     iSecurityFlatFileIndex;

        return iSecurityFlatFileIndex;
}

string tradetype[5] = {"TMB", "TMS", "TSL", "TLS", "TLB"};

const char* GetTradeType(eTradeTypeID tt){
	return tradetype[tt].data();
}

string statustype[5] = {"CMPT", "ACTV", "SBMT", "PNDG", "CNCL"};

const char* GetStatusType(eStatusTypeID st){
	return statustype[st].data();
}

void GenerateRandomAccountSecurity(TIdent iCustomer,eCustomerTier iTier, tpce_worker *worker_,
            TIdent* piCustomerAccount, TIdent* piSecurityFlatFileIndex, int* piSecurityAccountIndex){
        TIdent  iCustomerAccount;
        int     iAccountCount;
        int     iTotalAccountSecurities;
        int     iSecurityAccountIndex;  // index of the selected security in the account's basket
        TIdent  iSecurityFlatFileIndex; // index of the selected security in the input flat file

        // Select random account for the customer
        //
        GenerateRandomAccountId(iCustomer, iTier, worker_, &iCustomerAccount, &iAccountCount);

        iTotalAccountSecurities = GetNumberOfSecurities(iCustomerAccount, iTier, iAccountCount, worker_);

        // Select random security in the account
        //
        iSecurityAccountIndex = worker_->RandomNumber(worker_->getR(), 1, iTotalAccountSecurities);

        iSecurityFlatFileIndex = GetSecurityFlatFileIndex(iCustomerAccount, iSecurityAccountIndex, worker_);

        // Return data
        //
        *piCustomerAccount          = iCustomerAccount;
        *piSecurityFlatFileIndex    = iSecurityFlatFileIndex;
        if (piSecurityAccountIndex != NULL)
        {
            *piSecurityAccountIndex = iSecurityAccountIndex;
        }
 }

INT64 convertTime(TIMESTAMP_STRUCT ts){
	CDateTime dt(&ts);
	return dt.GetTime();
}

//end of helper functions and structs

//tpce utility functions
void tpce_worker::insert_holding(
        HOLDING::key &key,
        HOLDING::value &value,
        void *txn) {
    HOLDING_H_CA_ID::key kk;
    HOLDING_H_CA_ID::value vv;
    kk.H1_H_CA_ID = value.H_CA_ID;
    kk.H1_H_S_SYMB = value.H_S_SYMB;
    kk.H1_H_T_ID_K = key.H_T_ID;
    vv.H1_H_T_ID_V = key.H_T_ID;
    // insert secondary index
    tbl_HOLDING_H_CA_ID(1/*TODO*/)->insert(txn, Encode(str(), kk), Encode(str(), vv));
    // insert holding
    tbl_HOLDING(1/*TODO*/)->insert(txn, Encode(str(), key), Encode(str(), value));
}

void tpce_worker::remove_holding(
        const hold_kv_t &kv,
        void *txn) {
    // delete secondary index
    HOLDING_H_CA_ID::key k;
    k.H1_H_CA_ID = kv.value->H_CA_ID;
    k.H1_H_S_SYMB = kv.value->H_S_SYMB;
    k.H1_H_T_ID_K = kv.key.H_T_ID;

	tbl_HOLDING_H_CA_ID(1/*TODO*/)->remove(txn, Encode(str(), k));
    // delete
    tbl_HOLDING(1/*TODO*/)->remove(txn, Encode(str(), kv.key));
}

void tpce_worker::insert_trade_request(
        TRADE_REQUEST::key &key,
        TRADE_REQUEST::value &value,
        void *txn) {
    TRADE_REQUEST_TR_B_ID::key k1;
    TRADE_REQUEST_TR_B_ID::value v1;
    k1.TR1_TR_B_ID = value.TR_B_ID;
    k1.TR1_TR_S_SYMB = value.TR_S_SYMB;
    k1.TR1_TR_T_ID_K = key.TR_T_ID;
    v1.TR1_TR_T_ID_V = key.TR_T_ID;

    TRADE_REQUEST_TR_S_SYMB::key k2;
    TRADE_REQUEST_TR_S_SYMB::value v2;
    k2.TR2_TR_S_SYMB = value.TR_S_SYMB;
    k2.TR2_TR_TT_ID = value.TR_TT_ID;
    k2.TR2_TR_BID_PRICE = (int64_t)(value.TR_BID_PRICE*100);
    k2.TR2_TR_T_ID_K = key.TR_T_ID;
    v2.TR2_TR_T_ID_V = key.TR_T_ID;

    // insert TRADE_REQUEST
    tbl_TRADE_REQUEST(1/*TODO*/)->insert(txn, Encode(str(), key), Encode(str(), value));
    // insert secondary index
    tbl_TRADE_REQUEST_TR_B_ID(1/*TODO*/)->insert(txn, Encode(str(), k1), Encode(str(), v1));
    // insert secondary index
    tbl_TRADE_REQUEST_TR_S_SYMB(1/*TODO*/)->insert(txn, Encode(str(), k2), Encode(str(), v2));
}

void tpce_worker::remove_trade_request(
        const trade_request_t &tr,
        const char *symbol,
        void *txn) {
    TRADE_REQUEST::key k;
    k.TR_T_ID = tr.trade_id;
    TRADE_REQUEST_TR_B_ID::key k1;
    k1.TR1_TR_B_ID = tr.broker_id;
    k1.TR1_TR_S_SYMB.assign(symbol);
    k1.TR1_TR_T_ID_K = tr.trade_id;
    TRADE_REQUEST_TR_S_SYMB::key k2;
    k2.TR2_TR_S_SYMB.assign(symbol);
    switch (tr.trade_type) {
        case mftt_stop_loss:
            k2.TR2_TR_TT_ID = type_stop_loss;
            break;
        case mftt_limit_sell:
            k2.TR2_TR_TT_ID = type_limit_sell;
            break;
        case mftt_limit_buy:
            k2.TR2_TR_TT_ID = type_limit_buy;
            break;
    }
    k2.TR2_TR_BID_PRICE = (int64_t)(tr.bid_price*100);
    k2.TR2_TR_T_ID_K = tr.trade_id;
    // delete TRADE_REQUEST
    tbl_TRADE_REQUEST(1/*TODO*/)->remove(txn, Encode(str(), k));
    // delete secondary index TRADE_REQUEST_TR_B_ID
    tbl_TRADE_REQUEST_TR_B_ID(1/*TODO*/)->remove(txn, Encode(str(), k1));
    // delete secondary index TRADE_REQUEST_TR_S_SYMB
    tbl_TRADE_REQUEST_TR_S_SYMB(1/*TODO*/)->remove(txn, Encode(str(), k2));
}

void tpce_worker::insert_trade(
        TRADE::key &key,
        TRADE::value &value,
        void *txn) {
    TRADE_T_CA_ID::key k1;
    TRADE_T_CA_ID::value v1;
    k1.T1_T_CA_ID = value.T_CA_ID;
    k1.T1_T_DTS = value.T_DTS;
    k1.T1_T_ID_K = key.T_ID;
    v1.T1_T_ID_V = key.T_ID;
    TRADE_T_S_SYMB::key k2;
    TRADE_T_S_SYMB::value v2;
    k2.T2_T_S_SYMB = value.T_S_SYMB;
    k2.T2_T_DTS = value.T_DTS;
    k2.T2_T_ID_K = key.T_ID;
    v2.T2_T_ID_V = key.T_ID;
    // insert TRADE
    tbl_TRADE(1/*TODO*/)->insert(txn, Encode(str(), key), Encode(str(), value));
    // insert secondary index
    tbl_TRADE_T_CA_ID(1/*TODO*/)->insert(txn, Encode(str(), k1), Encode(str(), v1));
    // insert secondary index
    tbl_TRADE_T_S_SYMB(1/*TODO*/)->insert(txn, Encode(str(), k2), Encode(str(), v2));
}

tpce_worker::txn_result
tpce_worker::txn_broker_volume()
{
	INT32           iNumBrokers;
    INT32           iCount, i;
    TIdent          B_ID[max_broker_list_len];
    INT32           iSectorIndex;

	iNumBrokers = RandomNumber(r, min_broker_list_len, max_broker_list_len);

	if(iNumBrokers > getBrokerCount()){
		iNumBrokers = getBrokerCount();
	}
	iCount = 0;

	TBrokerVolumeTxnInput TxnReq;

	for (i = 0; i < max_broker_list_len; ++i)
    {
        TxnReq.broker_list[i][0] = '\0';
    }

    do
    {
        //select random broker ID (from active customer range)
        B_ID[iCount] = GenerateRandomBrokerId(this);

        for (i = 0; (i < iCount) && (B_ID[i] != B_ID[iCount]); ++i) { };

        if (i == iCount)    //make sure brokers are distinct
        {
            //put the broker name into the input parameter
            GenerateBrokerName(B_ID[iCount], TxnReq.broker_list[iCount], static_cast<int>(sizeof(TxnReq.broker_list[iCount])), m_person);
            ++iCount;
        }

    } while (iCount < iNumBrokers);

	iSectorIndex = RandomNumber(r, 0, dfm->SectorDataFile().size()-1);

    strncpy(TxnReq.sector_name, dfm->SectorDataFile()[iSectorIndex].SC_NAME_CSTR(), sizeof(TxnReq.sector_name));
	//  broker_list  &  sector_name  as input

    const uint64_t read_only_mask =
        g_disable_read_only_scans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
	void *txn = db->new_txn(
            txn_flags | read_only_mask, arena, txn_buf(), abstract_db::HINT_TPCE_BROKER_VOLUME);

	scoped_str_arena s_arena(arena);
    try {
        ssize_t ret = 0;

        std::vector<TIdent> b_id_list;
        b_id_list.reserve(iNumBrokers);
        // P1: [BROKER]
        {
            BROKER_B_NAME::value v_temp;
            BROKER_B_NAME::key k_lower, k_upper;
            for (int i = 0; i < iNumBrokers; i++) {
                static_limit_callback<1> c(s_arena.get(), true);
                k_lower.B1_B_NAME.assign(TxnReq.broker_list[i]);
                k_lower.B1_B_ID_K = 0;//= std::numeric_limits<TIdent>::min();
                k_upper.B1_B_NAME.assign(TxnReq.broker_list[i]);
                k_upper.B1_B_ID_K = std::numeric_limits<TIdent>::max();
                tbl_BROKER_B_NAME(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                ALWAYS_ASSERT(c.size() == 1);
                const BROKER_B_NAME::value *v = Decode(*c.values[0].second,
                        v_temp);
                b_id_list.push_back(v->B1_B_ID_V);
            }
        }

        // P2: [SECTOR]
        inline_str_fixed_tpce<TPCE::sm_cSC_ID_len> sc_id;
        {
            SECTOR_SC_NAME::value v_temp;
            SECTOR_SC_NAME::key k_lower, k_upper;
            k_lower.SC1_SC_NAME.assign(TxnReq.sector_name);
            k_lower.SC1_SC_ID_K.assign(min_str.c_str(), TPCE::sm_cSC_ID_len);
            k_upper.SC1_SC_NAME.assign(TxnReq.sector_name);
            k_upper.SC1_SC_ID_K.assign(max_str.c_str(), TPCE::sm_cSC_ID_len);
            static_limit_callback<1> c(s_arena.get(), true);
            tbl_SECTOR_SC_NAME(1)->scan(txn, Encode(obj_key0, k_lower),
                    &Encode(obj_key1, k_upper), c, s_arena.get());
            ALWAYS_ASSERT(c.size() == 1);
            const SECTOR_SC_NAME::value *v = Decode(*c.values[0].second,
                    v_temp);
            sc_id = v->SC1_SC_ID_V;
        }

        // P3: [INDUSTRY]
        std::vector<inline_str_fixed_tpce<TPCE::sm_cIN_ID_len> > in_id_list;
        {
            INDUSTRY_SC_ID::value v_temp;
            INDUSTRY_SC_ID::key k_lower, k_upper;
            limit_callback c;
            k_lower.IN1_IN_SC_ID = sc_id;
            k_lower.IN1_IN_ID_K.assign(min_str.c_str(), TPCE::sm_cIN_ID_len);
            k_upper.IN1_IN_SC_ID = sc_id;
            k_upper.IN1_IN_ID_K.assign(max_str.c_str(), TPCE::sm_cIN_ID_len);
            tbl_INDUSTRY_SC_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                    &Encode(obj_key1, k_upper), c, s_arena.get());
            ALWAYS_ASSERT(c.size() > 0);
            for (auto &it : c.values) {
                const INDUSTRY_SC_ID::value *v = Decode(it.second, v_temp);
                in_id_list.emplace_back(v->IN1_IN_ID_V);
            }
        }

        // P4: [COMPANY]
        std::vector<TIdent> co_id_list;
        {
            COMPANY_CO_IN_ID::value v_temp;
            COMPANY_CO_IN_ID::key k_lower, k_upper;
            k_lower.CO1_CO_ID_K = 0;//= std::numeric_limits<TIdent>::min();
            k_upper.CO1_CO_ID_K = std::numeric_limits<TIdent>::max();

            for (auto &it : in_id_list) {
                k_lower.CO1_CO_IN_ID = it;
                k_upper.CO1_CO_IN_ID = it;
                limit_callback c;
                tbl_COMPANY_CO_IN_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                for (auto &it2 : c.values) {
                    const COMPANY_CO_IN_ID::value *v = Decode(it2.second, v_temp);
                    co_id_list.push_back(v->CO1_CO_ID_V);
                }
            }

            ALWAYS_ASSERT(co_id_list.size() > 0);
        }

        // P5: [SECURITY]
        std::vector<inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len> > s_symb_list;
        {
            SECURITY_S_CO_ID::value v_temp;
            SECURITY_S_CO_ID::key k_lower, k_upper;
            k_lower.S1_S_ISSUE.assign(min_str.c_str(), TPCE::sm_cS_ISSUE_len);
            k_lower.S1_S_SYMB_K.assign(min_str.c_str(), TPCE::sm_cSYMBOL_len);
            k_upper.S1_S_ISSUE.assign(max_str.c_str(), TPCE::sm_cS_ISSUE_len);
            k_upper.S1_S_SYMB_K.assign(max_str.c_str(), TPCE::sm_cSYMBOL_len);

            for (auto &it : co_id_list) {
                k_lower.S1_S_CO_ID = it;
                k_upper.S1_S_CO_ID = it;
                limit_callback c;
                tbl_SECURITY_S_CO_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                for (auto &it2 : c.values) {
                    const SECURITY_S_CO_ID::value *v = Decode(it2.second,
                            v_temp);
                    s_symb_list.emplace_back(v->S1_S_SYMB_V);
                }
            }
            ALWAYS_ASSERT(s_symb_list.size() > 0);
        }

        // P6: [TRADE_REQUEST]
        {
            std::vector<TTrade> tid_list;
            TRADE_REQUEST_TR_B_ID::value v_temp;
            TRADE_REQUEST_TR_B_ID::key k_lower, k_upper;
            k_lower.TR1_TR_T_ID_K = 0;//= std::numeric_limits<TTrade>::min();
            k_upper.TR1_TR_T_ID_K = std::numeric_limits<TTrade>::max();
            //printf("symbol list len: %lu, broker id list: %lu\n", s_symb_list.size(), b_id_list.size());
            for (auto &symb : s_symb_list) {
                for (auto b_id : b_id_list) {
                    k_lower.TR1_TR_B_ID = b_id;
                    k_lower.TR1_TR_S_SYMB = symb;
                    k_upper.TR1_TR_B_ID = b_id;
                    k_upper.TR1_TR_S_SYMB = symb;
                    limit_callback c;
                    tbl_TRADE_REQUEST_TR_B_ID(1)->scan(txn,
                            Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &res : c.values) {
                        const TRADE_REQUEST_TR_B_ID::value *v = Decode(
                                res.second, v_temp);
                        tid_list.push_back(v->TR1_TR_T_ID_V);
                    }
                }
            }

            TRADE::value t_v_temp;
            for (auto it : tid_list) {
                const TRADE::key k(it);
                ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->get(txn, Encode(obj_key0, k),
                            obj_v));
                const TRADE::value *v = Decode(obj_v, t_v_temp);
                ALWAYS_ASSERT(v);
                //v->TR_QTY;
                //v->TR_BID_PRICE;
            }
        }
        if (likely(db->commit_txn(txn))){
            return txn_result(true, ret);
        }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}
    //fprintf(stderr, "broker volume: %d\n", broker_volume);
    return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_customer_position()
{
	TIdent          iCustomerId;
    eCustomerTier   iCustomerTier;

    GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier, m_CustomerSelection);

	TCustomerPositionTxnInput TxnReq;

    if (RandomPercent(r, m_pDriverCETxnSettings->CP_settings.cur.by_tax_id))
    {
        //send tax id instead of customer id
        m_person->GetTaxID(iCustomerId, TxnReq.tax_id);
		TxnReq.cust_id = 0; //don't need customer id since filled in the tax id
    }
    else
    {
        // send customer id and not the tax id
        TxnReq.cust_id = iCustomerId;
		TxnReq.tax_id[0] = '\0';
    }

    TxnReq.get_history = RandomPercent(r, m_pDriverCETxnSettings->CP_settings.cur.get_history);
    if(TxnReq.get_history )
    {
        TxnReq.acct_id_idx = RandomNumber(r, 0,
			GetNumberOfAccounts(iCustomerId, iCustomerTier, m_CustomerSelection) - 1);
    }
    else
    {
        TxnReq.acct_id_idx = -1;
    }

    const uint64_t read_only_mask =
        g_disable_read_only_scans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
	void *txn = db->new_txn(
            txn_flags | read_only_mask, arena, txn_buf(), abstract_db::HINT_TPCE_CUSTOMER_POSITION);
	scoped_str_arena s_arena(arena);
    try {
        ssize_t ret = 0;

        // P1: [CUSTOMER_C_TAX_ID]
        {
            if (0 == TxnReq.cust_id) {
                CUSTOMER_C_TAX_ID::value v_temp;
                CUSTOMER_C_TAX_ID::key k_lower, k_upper;
                k_lower.C1_C_TAX_ID.assign(TxnReq.tax_id);
                k_lower.C1_C_ID_K = 0;//= std::numeric_limits<TIdent>::min();
                k_upper.C1_C_TAX_ID.assign(TxnReq.tax_id);
                k_upper.C1_C_ID_K = std::numeric_limits<TIdent>::max();
                static_limit_callback<1> c(s_arena.get(), true);
                tbl_CUSTOMER_C_TAX_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                ALWAYS_ASSERT(c.size() == 1);
                const CUSTOMER_C_TAX_ID::value *v = Decode(*c.values[0].second,
                        v_temp);
                TxnReq.cust_id = v->C1_C_ID_V;
            }
        }

        // P2: [CUSTOMER]
        {
            const CUSTOMER::key k(TxnReq.cust_id);
      		ALWAYS_ASSERT(tbl_CUSTOMER(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
		    CUSTOMER::value v_temp;
		    const CUSTOMER::value *v = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v);
        }

        // P3: [CUSTOMER_ACCOUNT_CA_C_ID]
        std::vector<TIdent> ca_id_list;
        {
            CUSTOMER_ACCOUNT_CA_C_ID::value v_temp;
            CUSTOMER_ACCOUNT_CA_C_ID::key k_lower, k_upper;
            k_lower.CA1_CA_C_ID = TxnReq.cust_id;
            k_lower.CA1_CA_ID_K = 0;//std::numeric_limits<TIdent>::min();
            k_upper.CA1_CA_C_ID = TxnReq.cust_id;
            k_upper.CA1_CA_ID_K = std::numeric_limits<TIdent>::max();
            limit_callback c;
            tbl_CUSTOMER_ACCOUNT_CA_C_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                    &Encode(obj_key1, k_upper), c, s_arena.get());
            ALWAYS_ASSERT(c.size() > 0);
            for (auto &it : c.values) {
                const CUSTOMER_ACCOUNT_CA_C_ID::value *v = Decode(it.second,
                        v_temp);
                ca_id_list.push_back(v->CA1_CA_ID_V);
            }
        }

        // P4: [CUSTOMER_ACCOUNT]
        {
            CUSTOMER_ACCOUNT::value v_temp;
            for (auto ca_id : ca_id_list) {
                const CUSTOMER_ACCOUNT::key k(ca_id);
                ALWAYS_ASSERT(tbl_CUSTOMER_ACCOUNT(1/*TODO*/)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const CUSTOMER_ACCOUNT::value *v = Decode(obj_v, v_temp);
                ALWAYS_ASSERT(v);
            }
        }

        // P5: [HOLDING_SUMMARY]
        std::vector<inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len> > hs_s_symb_list;
        std::vector<int32_t> hs_qty_list;
        {
            HOLDING_SUMMARY::key k_lower, k_upper;
            k_lower.HS_S_SYMB.assign(min_str.c_str(), TPCE::sm_cSYMBOL_len);
            k_upper.HS_S_SYMB.assign(max_str.c_str(), TPCE::sm_cSYMBOL_len);
            HOLDING_SUMMARY::value v_temp;
            HOLDING_SUMMARY::key k_temp;
            for (auto ca_id : ca_id_list) {
                k_lower.HS_CA_ID = ca_id;
                k_upper.HS_CA_ID = ca_id;

                limit_callback c;
                tbl_HOLDING_SUMMARY(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                for (auto &it : c.values) {
                    const HOLDING_SUMMARY::key *k = Decode(it.first, k_temp);
                    hs_s_symb_list.emplace_back(k->HS_S_SYMB);
                    const HOLDING_SUMMARY::value *v = Decode(it.second, v_temp);
                    hs_qty_list.push_back(v->HS_QTY);
                }
            }
        }

        // P6: [LAST_TRADE]
        std::vector<float> lt_price_list;
        {
            for (auto &hs_s_symb : hs_s_symb_list) {
                const LAST_TRADE::key k(hs_s_symb);
                LAST_TRADE::value v_temp;
                ALWAYS_ASSERT(tbl_LAST_TRADE(1)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const LAST_TRADE::value *v = Decode(obj_v, v_temp);
                lt_price_list.push_back(v->LT_PRICE);
            }
        }

        if (TxnReq.get_history) {
            TIdent acct_id = ca_id_list[TxnReq.acct_id_idx];
            TTrade tid_list[10];
            //bool tid_used[10];
            int tid_index = 0;
            inline_str_fixed_tpce<TPCE::sm_cST_ID_len> status_types[5];
            int status_types_index = 0;

            // P7: [TRADE_T_CA_ID]
            {
                TRADE_T_CA_ID::value v_temp;
                TRADE_T_CA_ID::key k_lower, k_upper;
                k_lower.T1_T_CA_ID = acct_id;
                k_lower.T1_T_DTS = 0;//= std::numeric_limits<int_date_time>::min();
                k_lower.T1_T_ID_K = 0;//= std::numeric_limits<TTrade>::min();
                k_upper.T1_T_CA_ID = acct_id;
                k_upper.T1_T_DTS = std::numeric_limits<int_date_time>::max();
                k_upper.T1_T_ID_K = std::numeric_limits<TTrade>::max();
                static_limit_callback<10> c(s_arena.get(), true);
                tbl_TRADE_T_CA_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                for (auto &it : c.values) {
                    const TRADE_T_CA_ID::value *v = Decode(*it.second, v_temp);
                    tid_list[tid_index] = v->T1_T_ID_V;
                    //tid_used[tid_index++] = false;
                }
            }

            std::vector<trade_history_t> th_list;
            th_list.reserve(50);
            // P9: [TRADE_HISTORY]
            {
                TRADE_HISTORY::value v_temp;
                TRADE_HISTORY::key k_temp, k_lower, k_upper;
                k_lower.TH_ST_ID.assign(min_str.c_str(), TPCE::sm_cST_ID_len);
                k_upper.TH_ST_ID.assign(max_str.c_str(), TPCE::sm_cST_ID_len);
                for (int i = 0; i < tid_index; i++) {
                    k_lower.TH_T_ID = tid_list[i];
                    k_upper.TH_T_ID = tid_list[i];
                    static_limit_callback<5> c(s_arena.get(), false);
                    tbl_TRADE_HISTORY(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &it : c.values) {
                        const TRADE_HISTORY::key *k = Decode(*it.first, k_temp);
                        const TRADE_HISTORY::value *v = Decode(*it.second,
                                v_temp);
                        th_list.emplace_back(k->TH_T_ID, k->TH_ST_ID, v->TH_DTS);
                        if (status_types_index < 5) {
                            int sti;
                            for (sti = 0; sti < status_types_index; sti++) {
                                if (status_types[sti] == k->TH_ST_ID) break;
                            }
                            if (sti == status_types_index) {
                                status_types[status_types_index++] = k->TH_ST_ID;
                            }
                        }
                    }
                }
            }
            std::sort(th_list.begin(), th_list.end());

            // P8: [TRADE]
            {
                for (int i = 0; i < std::min(30, (int)th_list.size()); i++) {
                    int j = th_list.size() - 1 - i;
                    const TRADE::key k(th_list[j].t_id);
                    TRADE::value v_temp;
                    ALWAYS_ASSERT(tbl_TRADE(1)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const TRADE::value *v = Decode(obj_v, v_temp);
                    ALWAYS_ASSERT(v);
                    //v->T_DTS;
                    //v->T_QTY
                }
            }

            // P9: [STATUS_TYPE]
            {
                for (int i = 0; i < status_types_index; i++) {
                    const STATUS_TYPE::key k(status_types[i]);
                    STATUS_TYPE::value v_temp;
                    ALWAYS_ASSERT(tbl_STATUS_TYPE(1)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const STATUS_TYPE::value *v = Decode(obj_v, v_temp);
                    ALWAYS_ASSERT(v);
                }
            }
        }

        if (likely(db->commit_txn(txn))){
            return txn_result(true, ret);
        }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}
    //fprintf(stderr, "customer position: %d\n", customer_position);
    return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_market_feed()
{
#ifdef MANUAL_MARKET_FEED
    if (marketfeedIndex <= 0) {
        return txn_trade_order();
    } else if (marketfeedIndex < TPCE::max_feed_len) {
        std::vector<TTickerEntry>::iterator erase = this->limitorderqueue.begin();
        for (auto &it : this->limitorderqueue) {
            copyTTickerEntry(this->marketfeedInput.Entries[this->marketfeedIndex++], it);
            erase++;
            if (this->marketfeedIndex >= TPCE::max_feed_len) {
                break;
            }
        }
        this->limitorderqueue.erase(this->limitorderqueue.begin(), erase);
    }
#endif

	//ndb_wrapper_default *ndb = (ndb_wrapper_default *)worker_->db;
	CDateTime current;
	int_date_time now_dts = current.GetTime();
	int mfi = 0;
	void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCE_MARKET_FEED);

    // db->init_txn(txn, cgraph, market_feed_type);
	scoped_str_arena s_arena(arena);

	try{
		ssize_t ret = 0;

		// Conflict Piece: P1 -- w [LAST_TRADE]
        // MARKET_FEED: 1
market_feed_last_trade:
        // try 
        {
            // db->mul_ops_begin(txn);
            for (mfi = 0; mfi < marketfeedIndex; mfi++) {
                TTickerEntry *entry = &marketfeedInput.Entries[mfi];
                //ic3_txn->start_conflict_piece();
                LAST_TRADE::key k;
                k.LT_S_SYMB.assign(entry->symbol);
                if (!(tbl_LAST_TRADE(1/*TODO*/)->get(txn, Encode(obj_key0, k), obj_v))) {
                    printf("symbol: %s\n", k.LT_S_SYMB.data());
                    printf("mfi: %d, feedIndex: %d\n", mfi, marketfeedIndex);
                    ALWAYS_ASSERT(0);
                }
                LAST_TRADE::value v_lt_temp;
                const LAST_TRADE::value *v_lt = Decode(obj_v, v_lt_temp);
                //checker::SanityCheckStock(&k, v_lt);

                LAST_TRADE::value v_lt_new(*v_lt);
                v_lt_new.LT_PRICE = (float)entry->price_quote;
                v_lt_new.LT_VOL += entry->trade_qty;
                v_lt_new.LT_DTS = now_dts;

                tbl_LAST_TRADE(1/*TODO*/)->put(txn, Encode(str(), k), Encode(str(), v_lt_new));
            }
            // if (!db->mul_ops_end(txn)) {
                // goto market_feed_last_trade;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto market_feed_last_trade;
        // }

		std::vector<trade_request_t> request_list;
		// Conflict Piece: P2 -- d [TRADE_REQUEST]
        // MARKET_FEED: 2
market_feed_trade_request:
        request_list.clear();
        // try 
        {
            // db->mul_ops_begin(txn);
            for (mfi = 0; mfi < marketfeedIndex; mfi++) {
                TTickerEntry *entry = &marketfeedInput.Entries[mfi];

                trade_request_scan_callback cb_stop_loss(false), cb_limit_sell(false), cb_limit_buy(false);
                TRADE_REQUEST_TR_S_SYMB::key k_lower, k_upper;
                k_lower.TR2_TR_S_SYMB.assign(entry->symbol);
                k_upper.TR2_TR_S_SYMB.assign(entry->symbol);
                k_lower.TR2_TR_T_ID_K = 0;//= std::numeric_limits<TTrade>::min();
                k_upper.TR2_TR_T_ID_K = std::numeric_limits<TTrade>::max();

                // stop_loss
                k_lower.TR2_TR_TT_ID = type_stop_loss;
                k_lower.TR2_TR_BID_PRICE = (int64_t)(entry->price_quote * 100);
                k_upper.TR2_TR_TT_ID = type_stop_loss;
                k_upper.TR2_TR_BID_PRICE = std::numeric_limits<int64_t>::max();
                //ic3_txn->scan(worker_->tbl_TRADE_REQUEST_TR_S_SYMB(1/*TODO*/)->GetTree(),
                //		Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper), cb_stop_loss);
                tbl_TRADE_REQUEST_TR_S_SYMB(1)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
                        cb_stop_loss, s_arena.get());

                // limit_sell
                k_lower.TR2_TR_TT_ID = type_limit_sell;
                k_lower.TR2_TR_BID_PRICE = 0;//= std::numeric_limits<int64_t>::min();
                k_upper.TR2_TR_TT_ID = type_limit_sell;
                k_upper.TR2_TR_BID_PRICE = (int64_t)(entry->price_quote * 100);
                //ic3_txn->scan(worker_->tbl_TRADE_REQUEST_TR_S_SYMB(1/*TODO*/)->GetTree(),
                //		Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper), cb_limit_sell);
                tbl_TRADE_REQUEST_TR_S_SYMB(1)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
                        cb_limit_sell, s_arena.get());

                // limit_buy
                k_lower.TR2_TR_TT_ID = type_limit_buy;
                k_lower.TR2_TR_BID_PRICE = (int64_t)(entry->price_quote * 100);
                k_upper.TR2_TR_TT_ID = type_limit_buy;
                k_upper.TR2_TR_BID_PRICE = std::numeric_limits<int64_t>::max();
                //ic3_txn->scan(worker_->tbl_TRADE_REQUEST_TR_S_SYMB(1/*TODO*/)->GetTree(),
                //		Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper), cb_limit_buy);
                tbl_TRADE_REQUEST_TR_S_SYMB(1)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
                        cb_limit_buy, s_arena.get());

                request_list.reserve(cb_stop_loss.size() + cb_limit_sell.size()
                        + cb_limit_buy.size());
                TRADE_REQUEST_TR_S_SYMB::key *decode_k;
                for (auto &it : cb_stop_loss.values) {
                    decode_k = it.first;
                    request_list.emplace_back(TTrade(decode_k->TR2_TR_T_ID_K),
                            float(decode_k->TR2_TR_BID_PRICE/100.0f), mftt_stop_loss);
                }
                for (auto &it : cb_limit_sell.values) {
                    decode_k = it.first;
                    request_list.emplace_back(TTrade(decode_k->TR2_TR_T_ID_K),
                            float(decode_k->TR2_TR_BID_PRICE/100.0f), mftt_limit_sell);
                }
                for (auto &it : cb_limit_buy.values) {
                    decode_k = it.first;
                    request_list.emplace_back(TTrade(decode_k->TR2_TR_T_ID_K),
                            float(decode_k->TR2_TR_BID_PRICE/100.0f), mftt_limit_buy);
                }

                std::sort(request_list.begin(), request_list.end());

                for (auto &it : request_list) {
                    TRADE_REQUEST::key k;
                    k.TR_T_ID = it.trade_id;

                    if (unlikely(!(tbl_TRADE_REQUEST(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v)))) {
#ifdef EARLY_ABORT
                        db->abort_txn(txn);
                        is_user_initiate_abort = true;
                        return txn_result(false, 0);
#else
                        it.mark_deleted = true;
                        continue;
#endif
                    }
                    TRADE_REQUEST::value v_tr_temp;
                    const TRADE_REQUEST::value *v_tr = Decode(obj_v, v_tr_temp);
                    it.broker_id = v_tr->TR_B_ID;
                    it.trade_qty = v_tr->TR_QTY;
                    //TRADE_REQUEST::value v_tr_new(*v_tr);
                }

                for (auto &it : request_list) {
                    if (!it.mark_deleted)
                        remove_trade_request(it, entry->symbol, txn);
                }
            }

            // if (!db->mul_ops_end(txn)) {
                // goto market_feed_trade_request;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto market_feed_trade_request;
        // }

		// Raw Piece: P3 -- w [TRADE]
		{
			for (auto &it : request_list) {
                if (it.mark_deleted) continue;
				TRADE::key k;
				k.T_ID = it.trade_id;
	      		ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->get(txn, Encode(obj_key0, k), obj_v));
			    TRADE::value v_t_temp;
			    const TRADE::value *v_t = Decode(obj_v, v_t_temp);

			    TRADE::value v_t_new(*v_t);
				v_t_new.T_DTS = now_dts;
				v_t_new.T_ST_ID = st_submitted_id;
			    tbl_TRADE(1/*TODO*/)->put(txn, Encode(str(), k), Encode(str(), v_t_new));
			}
		}

		// Conflict Piece: P4 -- i [TRADE_HISTORY]
        // MARKET_FEED: 3
market_feed_trade_history:
		// try 
        {
            // db->mul_ops_begin(txn);
			TRADE_HISTORY::key k;
			k.TH_ST_ID = st_submitted_id;
			TRADE_HISTORY::value v;
			for (auto &it : request_list) {
				k.TH_T_ID = it.trade_id;
				v.TH_DTS = now_dts;
				tbl_TRADE_HISTORY(1/*TODO*/)->insert(txn, Encode(str(), k), Encode(str(), v));
				ret += Size(v);
			}
            // if (!db->mul_ops_end(txn)) {
                // goto market_feed_trade_history;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto market_feed_trade_history;
        // }

        if (likely(db->commit_txn(txn))){
#ifdef MANUAL_MARKET_FEED
            market_feed_success();
#endif
            return txn_result(true, ret);
        }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}

	return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_market_watch()
{
	TIdent			iCustomerId;
	eCustomerTier	iCustomerTier;
	INT32			iThreshold;
	INT32			iWeek;
	INT32			iDailyMarketDay;
	CDateTime		StartDate(iDailyMarketBaseYear, iDailyMarketBaseMonth,
							  iDailyMarketBaseDay, iDailyMarketBaseHour,
							  iDailyMarketBaseMinute, iDailyMarketBaseSecond, iDailyMarketBaseMsec);

	iThreshold = RndGenerateIntegerPercentage(r);

	TMarketWatchTxnInput TxnReq;

	//have some distribution on what inputs to send
	if (iThreshold <= m_pDriverCETxnSettings->MW_settings.cur.by_industry)
	{
		//send industry name
		strncpy(TxnReq.industry_name,
			dfm->IndustryDataFile().at(RandomNumber(r, 0, dfm->IndustryDataFile().size()-1)).IN_NAME_CSTR(),
			sizeof(TxnReq.industry_name));

		TxnReq.c_id = TxnReq.acct_id = 0;
		TIdent m_iStartFromCompany = dfm->CompanyFile().GetCompanyId(0), m_iActiveCompanyCount = dfm->CompanyFile().GetSize();

		if( iBaseCompanyCount < m_iActiveCompanyCount)
		{
			TxnReq.starting_co_id = RndInt64Range(r, m_iStartFromCompany,
													m_iStartFromCompany +
													m_iActiveCompanyCount - ( iBaseCompanyCount - 1 ));
			TxnReq.ending_co_id = TxnReq.starting_co_id + ( iBaseCompanyCount - 1 );
		}
		else
		{
			TxnReq.starting_co_id = m_iStartFromCompany;
			TxnReq.ending_co_id = m_iStartFromCompany + m_iActiveCompanyCount - 1;
		}
	}
	else
	{
		TxnReq.industry_name[0] = '\0';
		TxnReq.starting_co_id = 0;
		TxnReq.ending_co_id = 0;

		if (iThreshold <= (m_pDriverCETxnSettings->MW_settings.cur.by_industry + m_pDriverCETxnSettings->MW_settings.cur.by_watch_list))
		{
			// Send customer id
			GenerateNonUniformRandomCustomerId(TxnReq.c_id, iCustomerTier, m_CustomerSelection);
			TxnReq.acct_id = 0;
		}
		else
		{
			// Send account id
			GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier, m_CustomerSelection);
			GenerateRandomAccountId(iCustomerId, iCustomerTier, this, &TxnReq.acct_id, NULL);
			TxnReq.c_id = 0;
		}
	}

	// Set start_day for both cases of the 'if'.
	//
	iWeek = (INT32)NURnd(r, 0, 255, 255, 0) + 5; // A = 255, S = 0
	// Week is now between 5 and 260.
	// Select a day within the week.
	//
	iThreshold = RndGenerateIntegerPercentage(r);
	if (iThreshold > 40)
	{
		iDailyMarketDay = iWeek * DaysPerWeek + 4;	  // Friday
	}
	else	// 1..40 case
	{
		if (iThreshold <= 20)
		{
			iDailyMarketDay = iWeek * DaysPerWeek;	  // Monday
		}
		else
		{
			if (iThreshold <= 27)
			{
				iDailyMarketDay = iWeek * DaysPerWeek + 1;	  // Tuesday
			}
			else
			{
				if (iThreshold <= 33)
				{
					iDailyMarketDay = iWeek * DaysPerWeek + 2;	  // Wednesday
				}
				else
				{
					iDailyMarketDay = iWeek * DaysPerWeek + 3;	  // Thursday
				}
			}
		}
	}

	// Go back 256 weeks and then add our calculated day.
	//
	StartDate.Add(iDailyMarketDay, 0);
	StartDate.GetTimeStamp(&TxnReq.start_day);

    const uint64_t read_only_mask =
        g_disable_read_only_scans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
	void *txn = db->new_txn(
            txn_flags | read_only_mask, arena, txn_buf(), abstract_db::HINT_TPCE_MARKET_WATCH);
	scoped_str_arena s_arena(arena);
    try {
        ssize_t ret = 0;
        std::vector<inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len> > stock_list;
        if (TxnReq.c_id != 0) {

            std::vector<TIdent> wl_id_list;
            // [WATCH_LIST_WL_C_ID]
            {
                WATCH_LIST_WL_C_ID::value v_temp;
                WATCH_LIST_WL_C_ID::key k_lower, k_upper;
                k_lower.WL1_WL_C_ID = TxnReq.c_id;
                k_upper.WL1_WL_C_ID = TxnReq.c_id;
                k_lower.WL1_WL_ID_K = 0;//= std::numeric_limits<TIdent>::min();
                k_upper.WL1_WL_ID_K = std::numeric_limits<TIdent>::max();

                limit_callback c;
                tbl_WATCH_LIST_WL_C_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                for (auto &it : c.values) {
                    const WATCH_LIST_WL_C_ID::value *v = Decode(it.second,
                            v_temp);
                    wl_id_list.push_back(v->WL1_WL_ID_V);
                }
            }

            // [WATCH_ITEM]
            {
                WATCH_ITEM::key k_lower, k_upper, k_temp;
                k_lower.WI_S_SYMB.assign(min_str.c_str(), TPCE::sm_cSYMBOL_len);
                k_upper.WI_S_SYMB.assign(max_str.c_str(), TPCE::sm_cSYMBOL_len);
                for (auto wl_id : wl_id_list) {
                    k_lower.WI_WL_ID = wl_id;
                    k_upper.WI_WL_ID = wl_id;

                    limit_callback c;
                    tbl_WATCH_ITEM(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &it : c.values) {
                        const WATCH_ITEM::key *k = Decode(it.first, k_temp);
                        stock_list.push_back(k->WI_S_SYMB);
                    }
                }
            }
        } else if (TxnReq.industry_name[0] != '\0') {
            inline_str_fixed_tpce<TPCE::sm_cIN_ID_len> in_id;
            // [INDUSTRY_IN_NAME]
            {
                INDUSTRY_IN_NAME::value v_temp;
                INDUSTRY_IN_NAME::key k_lower, k_upper;
                k_lower.IN2_IN_NAME.assign(TxnReq.industry_name);
                k_lower.IN2_IN_ID_K.assign(min_str.c_str(), TPCE::sm_cIN_ID_len);
                k_upper.IN2_IN_NAME.assign(TxnReq.industry_name);
                k_upper.IN2_IN_ID_K.assign(max_str.c_str(), TPCE::sm_cIN_ID_len);

                static_limit_callback<1> c(s_arena.get(), true);
                tbl_INDUSTRY_IN_NAME(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                ALWAYS_ASSERT(c.values.size() == 1);
                const INDUSTRY_IN_NAME::value *v = Decode(*c.values[0].second,
                        v_temp);
                in_id = v->IN2_IN_ID_V;
            }

            std::vector<TIdent> co_id_list;
            // [COMPANY_CO_IN_ID]
            {
                COMPANY_CO_IN_ID::value v_temp;
                COMPANY_CO_IN_ID::key k_lower, k_upper;
                k_lower.CO1_CO_IN_ID = in_id;
                k_lower.CO1_CO_ID_K = 0;//= std::numeric_limits<TIdent>::min();
                k_upper.CO1_CO_IN_ID = in_id;
                k_upper.CO1_CO_ID_K = std::numeric_limits<TIdent>::max();

                limit_callback c;
                tbl_COMPANY_CO_IN_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                for (auto &it : c.values) {
                    const COMPANY_CO_IN_ID::value *v = Decode(it.second,
                            v_temp);
                    co_id_list.push_back(v->CO1_CO_ID_V);
                }
            }

            // [SECURITY_S_CO_ID]
            {
                SECURITY_S_CO_ID::value v_temp;
                SECURITY_S_CO_ID::key k_lower, k_upper;
                k_lower.S1_S_ISSUE.assign(min_str.c_str(), TPCE::sm_cS_ISSUE_len);
                k_lower.S1_S_SYMB_K.assign(min_str.c_str(), TPCE::sm_cSYMBOL_len);
                k_upper.S1_S_ISSUE.assign(max_str.c_str(), TPCE::sm_cS_ISSUE_len);
                k_upper.S1_S_SYMB_K.assign(max_str.c_str(), TPCE::sm_cSYMBOL_len);
                for (auto co_id : co_id_list) {
                    k_lower.S1_S_CO_ID = co_id;
                    k_upper.S1_S_CO_ID = co_id;

                    limit_callback c;
                    tbl_SECURITY_S_CO_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &it : c.values) {
                        const SECURITY_S_CO_ID::value *v = Decode(it.second,
                                v_temp);
                        stock_list.push_back(v->S1_S_SYMB_V);
                    }
                }
            }
        } else if (TxnReq.acct_id != 0) {
            // [HOLDING_SUMMARY]
            {
                HOLDING_SUMMARY::key k_lower, k_upper, k_temp;
                k_lower.HS_S_SYMB.assign(min_str.c_str(), TPCE::sm_cSYMBOL_len);
                k_lower.HS_CA_ID = TxnReq.acct_id;
                k_upper.HS_S_SYMB.assign(max_str.c_str(), TPCE::sm_cSYMBOL_len);
                k_upper.HS_CA_ID = TxnReq.acct_id;

                limit_callback c;
                tbl_HOLDING_SUMMARY(1)->scan(txn, Encode(obj_key0, k_lower),
                        &Encode(obj_key1, k_upper), c, s_arena.get());
                for (auto &it : c.values) {
                    const HOLDING_SUMMARY::key *k = Decode(it.first, k_temp);
                    stock_list.push_back(k->HS_S_SYMB);
                }
            }
        } else ALWAYS_ASSERT(0);

        float old_mkt_cap = 0.0;
        float new_mkt_cap = 0.0;

        std::vector<mkt_cap_t> mkt_cap_list(stock_list.size());
        // [LAST_TRADE]
        {
            int index = 0;
            for (auto &symbol : stock_list) {
                const LAST_TRADE::key k(symbol);
                LAST_TRADE::value v_temp;
                ALWAYS_ASSERT(tbl_LAST_TRADE(1/*TODO*/)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const LAST_TRADE::value *v = Decode(obj_v, v_temp);
                mkt_cap_list[index++].new_price = v->LT_PRICE;
            }
        }

        int_date_time date = CDateTime(&TxnReq.start_day).GetTime();
        // [DAILY_MARKET]
        {
            int index = 0;
            for (auto &symbol : stock_list) {
                DAILY_MARKET::key k;
                k.DM_S_SYMB = symbol;
                k.DM_DATE = date;
                DAILY_MARKET::value v_temp;
                ALWAYS_ASSERT(tbl_DAILY_MARKET(1/*TODO*/)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const DAILY_MARKET::value *v = Decode(obj_v, v_temp);
                mkt_cap_list[index++].old_price = v->DM_CLOSE;
            }
        }

        // [SECURITY]
        {
            int index = 0;
            for (auto &symbol : stock_list) {
                const SECURITY::key k(symbol);
                SECURITY::value v_temp;
                ALWAYS_ASSERT(tbl_SECURITY(1/*TODO*/)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const SECURITY::value *v = Decode(obj_v, v_temp);
                mkt_cap_list[index++].num_out = v->S_NUM_OUT;
            }
        }

        for (auto &it : mkt_cap_list) {
            old_mkt_cap += it.num_out * it.old_price;
            new_mkt_cap += it.num_out * it.new_price;
        }

        float output = old_mkt_cap > 0.0 ?
            100.0 * (new_mkt_cap / old_mkt_cap - 1.0) :
            0.0;
        ALWAYS_ASSERT(&output != (float *)0);

        if (likely(db->commit_txn(txn))){
            return txn_result(true, ret);
        }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}
	//fprintf(stderr, "market_watch: %d\n", market_watch);
	return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_security_detail()
{
	CDateTime	StartDate(iDailyMarketBaseYear, iDailyMarketBaseMonth,
						  iDailyMarketBaseDay, iDailyMarketBaseHour,
						  iDailyMarketBaseMinute, iDailyMarketBaseSecond, iDailyMarketBaseMsec);
	INT32		iStartDay;	// day from the StartDate
	TIdent m_iActiveSecurityCount = dfm->SecurityFile().GetSize();

	TSecurityDetailTxnInput TxnReq;

	dfm->SecurityFile().CreateSymbol(RndInt64Range(r, 0, m_iActiveSecurityCount-1), TxnReq.symbol, static_cast<int>(sizeof(TxnReq.symbol)));

	// Whether or not to access the LOB.
	TxnReq.access_lob_flag = RandomPercent(r, m_pDriverCETxnSettings->SD_settings.cur.LOBAccessPercentage );

	// random number of financial rows to return
	TxnReq.max_rows_to_return = RandomNumber(r, iSecurityDetailMinRows, iSecurityDetailMaxRows);

	iStartDay = RandomNumber(r, 0, iDailyMarketTotalRows - TxnReq.max_rows_to_return);

	// add the offset
	StartDate.Add(iStartDay, 0);

	StartDate.GetTimeStamp(&TxnReq.start_day);

    const uint64_t read_only_mask =
        g_disable_read_only_scans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
	void *txn = db->new_txn(
            txn_flags | read_only_mask, arena, txn_buf(), abstract_db::HINT_TPCE_SECURITY_DETAIL);
	scoped_str_arena s_arena(arena);
    try {
        ssize_t ret = 0;

        TIdent s_co_id;
        inline_str_fixed_tpce<TPCE::sm_cEX_ID_len> s_ex_id;
        // P1: [SECURITY]
        {
            SECURITY::key k;
            k.S_SYMB.assign(TxnReq.symbol);
            SECURITY::value v_temp;
            ALWAYS_ASSERT(tbl_SECURITY(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const SECURITY::value *v = Decode(obj_v, v_temp);
            //v->xxxxxxxx
            s_co_id = v->S_CO_ID;
            s_ex_id = v->S_EX_ID;
        }

        TIdent co_ad_id;
        // P2: [COMPANY]
        {
            const COMPANY::key k(s_co_id);
            COMPANY::value v_temp;
            ALWAYS_ASSERT(tbl_COMPANY(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const COMPANY::value *v = Decode(obj_v, v_temp);
            //v->xxxxxxxx
            co_ad_id = v->CO_AD_ID;
        }

        TIdent ex_ad_id;
        // P3: [EXCHANGE]
        {
            const EXCHANGE::key k(s_ex_id);
            EXCHANGE::value v_temp;
            ALWAYS_ASSERT(tbl_EXCHANGE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const EXCHANGE::value *v = Decode(obj_v, v_temp);
            //v->xxxxxxx
            ex_ad_id = v->EX_AD_ID;
        }

        inline_str_fixed_tpce<TPCE::sm_cAD_ZIP_len> co_ad_zc_code, ex_ad_zc_code;
        // P4: [ADDRESS]
        {
            ADDRESS::value v_temp;

            const ADDRESS::key k1(co_ad_id);
            ALWAYS_ASSERT(tbl_ADDRESS(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k1), obj_v));
            const ADDRESS::value *v1 = Decode(obj_v, v_temp);
            co_ad_zc_code = v1->AD_ZC_CODE;

            const ADDRESS::key k2(ex_ad_id);
            ALWAYS_ASSERT(tbl_ADDRESS(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k2), obj_v));
            const ADDRESS::value *v2 = Decode(obj_v, v_temp);
            ex_ad_zc_code = v2->AD_ZC_CODE;
        }

        // P5: [ZIP_CODE]
        {
            ZIP_CODE::value v_temp;

            const ZIP_CODE::key k1(co_ad_zc_code);
            ALWAYS_ASSERT(tbl_ZIP_CODE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k1), obj_v));
            const ZIP_CODE::value *v1 = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v1);

            const ZIP_CODE::key k2(ex_ad_zc_code);
            ALWAYS_ASSERT(tbl_ZIP_CODE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k2), obj_v));
            const ZIP_CODE::value *v2 = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v2);
        }

        TIdent cp_comp_list[max_comp_len];
        inline_str_fixed_tpce<TPCE::sm_cIN_ID_len> cp_in_list[max_comp_len];
        int cp_index = 0;
        // P6: [COMPANY_COMPETITOR]
        {
            COMPANY_COMPETITOR::key k_temp, k_lower, k_upper;
            k_lower.CP_CO_ID = s_co_id;
            k_lower.CP_COMP_CO_ID = 0;//= std::numeric_limits<TIdent>::min();
            k_lower.CP_IN_ID.assign(min_str.c_str(), TPCE::sm_cIN_ID_len);
            k_upper.CP_CO_ID = s_co_id;
            k_upper.CP_COMP_CO_ID = std::numeric_limits<TIdent>::max();
            k_upper.CP_IN_ID.assign(max_str.c_str(), TPCE::sm_cIN_ID_len);

            static_limit_callback<max_comp_len> c(s_arena.get(), false);
            tbl_COMPANY_COMPETITOR(1)->scan(txn, Encode(obj_key0, k_lower),
                    &Encode(obj_key1, k_upper), c, s_arena.get());
            for (auto &it : c.values) {
                const COMPANY_COMPETITOR::key *k = Decode(*it.first, k_temp);
                cp_comp_list[cp_index] = k->CP_COMP_CO_ID;
                cp_in_list[cp_index] = k->CP_IN_ID;
                cp_index++;
            }
        }

        // P7: [COMPANY]
        {
            for (int i = 0; i < cp_index; i++) {
                COMPANY::value v_temp;
                const COMPANY::key k(cp_comp_list[i]);
                ALWAYS_ASSERT(tbl_COMPANY(1/*TODO*/)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const COMPANY::value *v = Decode(obj_v, v_temp);
                ALWAYS_ASSERT(v);
            }
        }

        // P8: [INDUSTRY]
        {
            for (int i = 0; i < cp_index; i++) {
                INDUSTRY::value v_temp;
                const INDUSTRY::key k(cp_in_list[i]);
                ALWAYS_ASSERT(tbl_INDUSTRY(1/*TODO*/)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const INDUSTRY::value *v = Decode(obj_v, v_temp);
                ALWAYS_ASSERT(v);
            }
        }

        // P9: [FINANCIAL]
        {
            FINANCIAL::key k_temp, k_lower, k_upper;
            FINANCIAL::value v_temp;
            k_lower.FI_CO_ID = s_co_id;
            k_lower.FI_YEAR = 0;//= std::numeric_limits<int32_t>::min();
            k_lower.FI_QTR = 0;//= std::numeric_limits<int32_t>::min();
            k_upper.FI_CO_ID = s_co_id;
            k_upper.FI_YEAR = std::numeric_limits<int32_t>::max();
            k_upper.FI_QTR = std::numeric_limits<int32_t>::max();

            static_limit_callback<max_fin_len> c(s_arena.get(), false);
            tbl_FINANCIAL(1)->scan(txn, Encode(obj_key0, k_lower),
                    &Encode(obj_key1, k_upper), c, s_arena.get());
            for (auto &it : c.values) {
                const FINANCIAL::key *k = Decode(*it.first, k_temp);
                const FINANCIAL::value *v = Decode(*it.second, v_temp);
                ALWAYS_ASSERT(k && v);
            }
        }

        // P10: [DAILY_MARKET]
        int_date_time date = CDateTime(&TxnReq.start_day).GetTime();
        {
            DAILY_MARKET::key k_temp, k_lower, k_upper;
            k_lower.DM_S_SYMB.assign(TxnReq.symbol);
            k_lower.DM_DATE = date;
            k_upper.DM_S_SYMB.assign(TxnReq.symbol);
            k_upper.DM_DATE = std::numeric_limits<int_date_time>::max();
            DAILY_MARKET::value v_temp;

            limit_callback c(TxnReq.max_rows_to_return);
            tbl_DAILY_MARKET(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower),
                    &Encode(obj_key1, k_upper), c, s_arena.get());
            for (auto &it : c.values) {
                const DAILY_MARKET::key *k = Decode(it.first, k_temp);
                const DAILY_MARKET::value *v = Decode(it.second, v_temp);
                ALWAYS_ASSERT(k && v);
            }
        }

        // P11: [LAST_TRADE]
        {
            LAST_TRADE::value v_temp;
            LAST_TRADE::key k;
            k.LT_S_SYMB.assign(TxnReq.symbol);
            ALWAYS_ASSERT(tbl_LAST_TRADE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const LAST_TRADE::value *v = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v);
        }

        TIdent nx_ni_id_list[max_news_len];
        int nx_ni_id_index = 0;
        // P12: [NEWS_XREF]
        {
            NEWS_XREF::key k_temp, k_lower, k_upper;
            k_lower.NX_CO_ID = s_co_id;
            k_lower.NX_NI_ID = 0;//= std::numeric_limits<TIdent>::min();
            k_upper.NX_CO_ID = s_co_id;
            k_upper.NX_NI_ID = std::numeric_limits<TIdent>::max();

            static_limit_callback<max_news_len> c(s_arena.get(), false);
            tbl_NEWS_XREF(1)->scan(txn, Encode(obj_key0, k_lower),
                    &Encode(obj_key1, k_upper), c, s_arena.get());
            for (auto &it : c.values) {
                const NEWS_XREF::key *k = Decode(*it.first, k_temp);
                nx_ni_id_list[nx_ni_id_index] = k->NX_NI_ID;
                nx_ni_id_index++;
            }
        }

        // P13: [NEWS_ITEM]
        {
            for (int i = 0; i < nx_ni_id_index; i++) {
                const NEWS_ITEM::key k(nx_ni_id_list[i]);
                NEWS_ITEM::value v_temp;
                ALWAYS_ASSERT(tbl_NEWS_ITEM(1/*TODO*/)->
                        get(txn, Encode(obj_key0, k), obj_v));
                const NEWS_ITEM::value *v = Decode(obj_v, v_temp);
                ALWAYS_ASSERT(v);
            }
        }

        if (likely(db->commit_txn(txn))){
            return txn_result(true, ret);
        }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}
	//fprintf(stderr, "security_detail: %d\n", security_detail);
	return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_trade_lookup()
{
	INT32           iThreshold;
	TTradeLookupTxnInput TxnReq;

    iThreshold = RndGenerateIntegerPercentage(r);

    if( iThreshold <= m_pDriverCETxnSettings->TL_settings.cur.do_frame1 )
    {
        // Frame 1
        TxnReq.frame_to_execute = 1;
        TxnReq.max_trades = m_pDriverCETxnSettings->TL_settings.cur.MaxRowsFrame1;

        // Generate list of unique trade id's
        int     ii, jj;
        bool    Accepted;
        TTrade  TID;

        for( ii = 0; ii < TxnReq.max_trades; ii++ )
        {
            Accepted = false;
            while( ! Accepted )
            {
                TID = GenerateNonUniformTradeID(TradeLookupAValueForTradeIDGenFrame1,
                                                TradeLookupSValueForTradeIDGenFrame1, this);
                jj = 0;
                while( jj < ii && TxnReq.trade_id[jj] != TID )
                {
                    jj++;
                }
                if( jj == ii )
                {
                    // We have a unique TID for this batch
                    TxnReq.trade_id[ii] = TID;
                    Accepted = true;
                }
            }
        }

        // Params not used by this frame /////////////////////////////////////////
        TxnReq.acct_id = 0;                                                     //
        TxnReq.max_acct_id = 0;                                                 //
        memset(TxnReq.symbol, 0, sizeof(TxnReq.symbol ));                     //
        memset(&TxnReq.start_trade_dts, 0, sizeof(TxnReq.start_trade_dts ));  //
        memset(&TxnReq.end_trade_dts, 0, sizeof(TxnReq.end_trade_dts ));      //
        //////////////////////////////////////////////////////////////////////////
    }
    else if( iThreshold <=  m_pDriverCETxnSettings->TL_settings.cur.do_frame1 +
                            m_pDriverCETxnSettings->TL_settings.cur.do_frame2 )
    {
        // Frame 2
        TxnReq.frame_to_execute = 2;
        TxnReq.acct_id = GenerateRandomCustomerAccountId(this);
        TxnReq.max_trades = m_pDriverCETxnSettings->TL_settings.cur.MaxRowsFrame2;

		INT64 m_iTradeLookupFrame2MaxTimeInMilliSeconds = (INT64)(( iHoursOfInitialTrades*SecondsPerHour ) -
			(m_pDriverCETxnSettings->TL_settings.cur.BackOffFromEndTimeFrame2 )) * MsPerSecond;
        GenerateNonUniformTradeDTS(TxnReq.start_trade_dts,
                                    m_iTradeLookupFrame2MaxTimeInMilliSeconds,
                                    TradeLookupAValueForTimeGenFrame2,
                                    TradeLookupSValueForTimeGenFrame2, this);

        // Set to the end of initial trades.
        m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);

        // Params not used by this frame /////////////////////////
        TxnReq.max_acct_id = 0;
        memset(TxnReq.symbol, 0, sizeof(TxnReq.symbol));     //
        memset(TxnReq.trade_id, 0, sizeof(TxnReq.trade_id)); //
        //////////////////////////////////////////////////////////
    }
    else if( iThreshold <=  m_pDriverCETxnSettings->TL_settings.cur.do_frame1 +
                            m_pDriverCETxnSettings->TL_settings.cur.do_frame2 +
                            m_pDriverCETxnSettings->TL_settings.cur.do_frame3 )
    {
        // Frame 3
        TxnReq.frame_to_execute = 3;
        TxnReq.max_trades = m_pDriverCETxnSettings->TL_settings.cur.MaxRowsFrame3;
		TIdent m_iActiveSecurityCount = dfm->SecurityFile().GetSize();

        dfm->SecurityFile().CreateSymbol(NURnd(r, 0, m_iActiveSecurityCount-1,
                                                TradeLookupAValueForSymbolFrame3,
                                                TradeLookupSValueForSymbolFrame3 ),
                                    TxnReq.symbol,
                                    static_cast<int>(sizeof(TxnReq.symbol)));

		INT64 m_iTradeLookupFrame3MaxTimeInMilliSeconds = (INT64)((iHoursOfInitialTrades*SecondsPerHour)-
				(m_pDriverCETxnSettings->TL_settings.cur.BackOffFromEndTimeFrame3 )) * MsPerSecond;
        GenerateNonUniformTradeDTS(TxnReq.start_trade_dts,
                                    m_iTradeLookupFrame3MaxTimeInMilliSeconds,
                                    TradeLookupAValueForTimeGenFrame3,
                                    TradeLookupSValueForTimeGenFrame3, this);

        // Set to the end of initial trades.
        m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);

        TxnReq.max_acct_id = GetEndingCA_ID( customer_selection_count );
        // Params not used by this frame /////////////////////////
        TxnReq.acct_id = 0;                                     //
        memset(TxnReq.trade_id, 0, sizeof(TxnReq.trade_id )); //
        //////////////////////////////////////////////////////////
    }
    else
    {
        // Frame 4
        TxnReq.frame_to_execute = 4;
        TxnReq.acct_id = GenerateRandomCustomerAccountId(this);
		INT64 m_iTradeLookupFrame4MaxTimeInMilliSeconds = (INT64)((iHoursOfInitialTrades*SecondsPerHour)-
			(this->m_pDriverCETxnSettings->TL_settings.cur.BackOffFromEndTimeFrame4 )) * MsPerSecond;
        GenerateNonUniformTradeDTS(TxnReq.start_trade_dts,
                                    m_iTradeLookupFrame4MaxTimeInMilliSeconds,
                                    TradeLookupAValueForTimeGenFrame4,
                                    TradeLookupSValueForTimeGenFrame4, this);

        // Params not used by this frame /////////////////////////
        TxnReq.max_trades = 0;                                  //
        TxnReq.max_acct_id = 0;
        memset(TxnReq.symbol, 0, sizeof(TxnReq.symbol));     //
        memset(TxnReq.trade_id, 0, sizeof(TxnReq.trade_id)); //
        //////////////////////////////////////////////////////////
    }

    const uint64_t read_only_mask =
        g_disable_read_only_scans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
	void *txn = db->new_txn(
            txn_flags | read_only_mask, arena, txn_buf(), abstract_db::HINT_TPCE_TRADE_LOOKUP);
	scoped_str_arena s_arena(arena);
    try {
        ssize_t ret = 0;
        switch (TxnReq.frame_to_execute) {
            case 1:
            {
                std::vector<bool> is_cash_list;
                is_cash_list.resize(TxnReq.max_trades);
                std::vector<inline_str_fixed_tpce<TPCE::sm_cTT_ID_len> > t_tt_id_list;
                t_tt_id_list.resize(TxnReq.max_trades);
                // [TRADE]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    const TRADE::key k(TxnReq.trade_id[i]);
                    TRADE::value v_temp;
                    ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const TRADE::value *v = Decode(obj_v, v_temp);
                    is_cash_list[i] = char2bool(v->T_IS_CASH);
                    t_tt_id_list[i] = v->T_TT_ID;
                }

                // [TRADE_TYPE]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    const TRADE_TYPE::key k(t_tt_id_list[i]);
                    TRADE_TYPE::value v_temp;
                    ALWAYS_ASSERT(tbl_TRADE_TYPE(1/*TODO*/)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const TRADE_TYPE::value *v = Decode(obj_v, v_temp);
                    ALWAYS_ASSERT(v);
                }

                // [SETTLEMENT]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    const SETTLEMENT::key k(TxnReq.trade_id[i]);
                    SETTLEMENT::value v_temp;
                    ALWAYS_ASSERT(tbl_SETTLEMENT(1/*TODO*/)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const SETTLEMENT::value *v = Decode(obj_v, v_temp);
                    ALWAYS_ASSERT(v);
                }

                // [CASH_TRANSACTION]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    if (is_cash_list[i]) {
                        const CASH_TRANSACTION::key k(TxnReq.trade_id[i]);
                        CASH_TRANSACTION::value v_temp;
                        ALWAYS_ASSERT(tbl_CASH_TRANSACTION(1/*TODO*/)->
                                get(txn, Encode(obj_key0, k), obj_v));
                        const CASH_TRANSACTION::value *v = Decode(obj_v, v_temp);
                        ALWAYS_ASSERT(v);
                    }
                }

                // [TRADE_HISTORY]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    TRADE_HISTORY::key k_temp, k_lower, k_upper;
                    TRADE_HISTORY::value v_temp;
                    k_lower.TH_T_ID = TxnReq.trade_id[i];
                    k_lower.TH_ST_ID.assign(min_str.c_str(), TPCE::sm_cST_ID_len);
                    k_upper.TH_T_ID = TxnReq.trade_id[i];
                    k_upper.TH_ST_ID.assign(max_str.c_str(), TPCE::sm_cST_ID_len);

                    static_limit_callback<5> c(s_arena.get(), false);
                    tbl_TRADE_HISTORY(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &it : c.values) {
                        const TRADE_HISTORY::key *k = Decode(*it.first, k_temp);
                        const TRADE_HISTORY::value *v = Decode(*it.second, v_temp);
                        ALWAYS_ASSERT(k && v);
                    }
                }
                break;
            }
            case 2:
            {
                int_date_time start_date = CDateTime(&TxnReq.start_trade_dts).GetTime();
                int_date_time end_date = CDateTime(&TxnReq.end_trade_dts).GetTime();
                // [TRADE_T_CA_ID]
                {
                    TRADE_T_CA_ID::key k_lower, k_upper;
                    TRADE_T_CA_ID::value v_temp;
                    k_lower.T1_T_CA_ID = TxnReq.acct_id;
                    k_lower.T1_T_DTS = start_date;
                    k_lower.T1_T_ID_K = 0;//= std::numeric_limits<TTrade>::min();
                    k_upper.T1_T_CA_ID = TxnReq.acct_id;
                    k_upper.T1_T_DTS = end_date;
                    k_upper.T1_T_ID_K = std::numeric_limits<TTrade>::max();

                    limit_callback c(TxnReq.max_trades);
                    tbl_TRADE_T_CA_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    int i = 0;
                    for (auto &it : c.values) {
                        const TRADE_T_CA_ID::value *v = Decode(it.second, v_temp);
                        TxnReq.trade_id[i++] = v->T1_T_ID_V;
                    }
                    TxnReq.max_trades = i;
                }

                std::vector<bool> is_cash_list;
                is_cash_list.resize(TxnReq.max_trades);
                // [TRADE]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    const TRADE::key k(TxnReq.trade_id[i]);
                    TRADE::value v_temp;
                    ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const TRADE::value *v = Decode(obj_v, v_temp);
                    is_cash_list[i] = char2bool(v->T_IS_CASH);
                }

                // [SETTLEMENT]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    const SETTLEMENT::key k(TxnReq.trade_id[i]);
                    SETTLEMENT::value v_temp;
                    ALWAYS_ASSERT(tbl_SETTLEMENT(1/*TODO*/)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const SETTLEMENT::value *v = Decode(obj_v, v_temp);
                    ALWAYS_ASSERT(v);
                }

                // [CASH_TRANSACTION]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    if (is_cash_list[i]) {
                        const CASH_TRANSACTION::key k(TxnReq.trade_id[i]);
                        CASH_TRANSACTION::value v_temp;
                        ALWAYS_ASSERT(tbl_CASH_TRANSACTION(1/*TODO*/)->
                                get(txn, Encode(obj_key0, k), obj_v));
                        const CASH_TRANSACTION::value *v = Decode(obj_v, v_temp);
                        ALWAYS_ASSERT(v);
                    }
                }

                // [TRADE_HISTORY]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    TRADE_HISTORY::key k_temp, k_lower, k_upper;
                    TRADE_HISTORY::value v_temp;
                    k_lower.TH_T_ID = TxnReq.trade_id[i];
                    k_lower.TH_ST_ID.assign(min_str.c_str(), TPCE::sm_cST_ID_len);
                    k_upper.TH_T_ID = TxnReq.trade_id[i];
                    k_upper.TH_ST_ID.assign(max_str.c_str(), TPCE::sm_cST_ID_len);

                    static_limit_callback<5> c(s_arena.get(), false);
                    tbl_TRADE_HISTORY(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &it : c.values) {
                        const TRADE_HISTORY::key *k = Decode(*it.first, k_temp);
                        const TRADE_HISTORY::value *v = Decode(*it.second, v_temp);
                        ALWAYS_ASSERT(k && v);
                    }
                }
                break;
            }
            case 3:
            {
                int_date_time start_date = CDateTime(&TxnReq.start_trade_dts).GetTime();
                int_date_time end_date = CDateTime(&TxnReq.end_trade_dts).GetTime();
                // [TRADE_T_S_SYMB]
                {
                    TRADE_T_S_SYMB::key k_lower, k_upper;
                    TRADE_T_S_SYMB::value v_temp;
                    k_lower.T2_T_S_SYMB.assign(TxnReq.symbol);
                    k_lower.T2_T_DTS = start_date;
                    k_lower.T2_T_ID_K = 0;//= std::numeric_limits<TTrade>::min();
                    k_upper.T2_T_S_SYMB.assign(TxnReq.symbol);
                    k_upper.T2_T_DTS = end_date;
                    k_upper.T2_T_ID_K = std::numeric_limits<TTrade>::max();

                    limit_callback c(TxnReq.max_trades);
                    tbl_TRADE_T_S_SYMB(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    int i = 0;
                    for (auto &it : c.values) {
                        const TRADE_T_S_SYMB::value *v = Decode(it.second, v_temp);
                        TxnReq.trade_id[i++] = v->T2_T_ID_V;
                    }
                    TxnReq.max_trades = i;
                }

                std::vector<bool> is_cash_list;
                is_cash_list.resize(TxnReq.max_trades);
                // [TRADE]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    const TRADE::key k(TxnReq.trade_id[i]);
                    TRADE::value v_temp;
                    ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const TRADE::value *v = Decode(obj_v, v_temp);
                    is_cash_list[i] = char2bool(v->T_IS_CASH);
                }

                // [SETTLEMENT]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    const SETTLEMENT::key k(TxnReq.trade_id[i]);
                    SETTLEMENT::value v_temp;
                    ALWAYS_ASSERT(tbl_SETTLEMENT(1/*TODO*/)->
                            get(txn, Encode(obj_key0, k), obj_v));
                    const SETTLEMENT::value *v = Decode(obj_v, v_temp);
                    ALWAYS_ASSERT(v);
                }

                // [CASH_TRANSACTION]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    if (is_cash_list[i]) {
                        const CASH_TRANSACTION::key k(TxnReq.trade_id[i]);
                        CASH_TRANSACTION::value v_temp;
                        ALWAYS_ASSERT(tbl_CASH_TRANSACTION(1/*TODO*/)->
                                get(txn, Encode(obj_key0, k), obj_v));
                        const CASH_TRANSACTION::value *v = Decode(obj_v, v_temp);
                        ALWAYS_ASSERT(v);
                    }
                }

                // [TRADE_HISTORY]
                for (int i = 0; i < TxnReq.max_trades; i++) {
                    TRADE_HISTORY::key k_temp, k_lower, k_upper;
                    TRADE_HISTORY::value v_temp;
                    k_lower.TH_T_ID = TxnReq.trade_id[i];
                    k_lower.TH_ST_ID.assign(min_str.c_str(), TPCE::sm_cST_ID_len);
                    k_upper.TH_T_ID = TxnReq.trade_id[i];
                    k_upper.TH_ST_ID.assign(max_str.c_str(), TPCE::sm_cST_ID_len);

                    static_limit_callback<5> c(s_arena.get(), false);
                    tbl_TRADE_HISTORY(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &it : c.values) {
                        const TRADE_HISTORY::key *k = Decode(*it.first, k_temp);
                        const TRADE_HISTORY::value *v = Decode(*it.second, v_temp);
                        ALWAYS_ASSERT(k && v);
                    }
                }
                break;
            }
            case 4:
            {
                int_date_time start_date = CDateTime(&TxnReq.start_trade_dts).GetTime();
                TTrade tid;
                // [TRADE_T_CA_ID]
                {
                    TRADE_T_CA_ID::key k_temp, k_lower, k_upper;
                    k_lower.T1_T_CA_ID = TxnReq.acct_id;
                    k_lower.T1_T_DTS = start_date;
                    k_lower.T1_T_ID_K = 0;//= std::numeric_limits<TTrade>::min();
                    k_upper.T1_T_CA_ID = TxnReq.acct_id;
                    k_upper.T1_T_DTS = std::numeric_limits<int_date_time>::max();
                    k_upper.T1_T_ID_K = std::numeric_limits<TTrade>::max();

                    std::string *s = s_arena.get()->next();
                    latest_key_callback c(*s, 1);
                    tbl_TRADE_T_CA_ID(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    const TRADE_T_CA_ID::key *k = Decode(*s, k_temp);
                    tid = k->T1_T_ID_K;
                }

                // [HOLDING_HISTORY]
                {
                    HOLDING_HISTORY::key k_temp, k_lower, k_upper;
                    HOLDING_HISTORY::value v_temp;
                    k_lower.HH_T_ID = tid;
                    k_lower.HH_H_T_ID = 0;//= std::numeric_limits<TTrade>::min();
                    k_upper.HH_T_ID = tid;
                    k_upper.HH_H_T_ID = std::numeric_limits<TTrade>::max();

                    static_limit_callback<20> c(s_arena.get(), false);
                    tbl_HOLDING_HISTORY(1)->scan(txn, Encode(obj_key0, k_lower),
                            &Encode(obj_key1, k_upper), c, s_arena.get());
                    for (auto &it : c.values) {
                        const HOLDING_HISTORY::key *k = Decode(*it.first, k_temp);
                        const HOLDING_HISTORY::value *v = Decode(*it.second, v_temp);
                        ALWAYS_ASSERT(k && v);
                    }
                }
                break;
            }
            default: ALWAYS_ASSERT(0);
        }

        if (likely(db->commit_txn(txn))){
            return txn_result(true, ret);
        }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}
    //fprintf(stderr, "trade_lookup: %d\n", trade_lookup);
    return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_trade_order()
{
	TIdent          iCustomerId;    //owner
    eCustomerTier   iCustomerTier;
    TIdent          CID_1, CID_2;
    bool            bMarket;
    INT32           iAdditionalPerms;
    int             iSymbIndex;
    TIdent          iFlatFileSymbIndex;
    eTradeTypeID    eTradeType;

	TTradeOrderTxnInput TxnReq;
	UNUSED INT32 iTradeType;
	UNUSED bool bExecutorIsAccountOwner;
    // Generate random customer
    //
    GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier, m_CustomerSelection);
    // Generate random account id and security index
    //
    GenerateRandomAccountSecurity(iCustomerId,iCustomerTier, this,
                                  &TxnReq.acct_id, &iFlatFileSymbIndex, &iSymbIndex);
#ifdef NO_CONFLICT_ON_SYMBOL
    iFlatFileSymbIndex += worker_id % nthreads;
#endif
    //find out how many permission rows there are for this account (in addition to the owner's)
    iAdditionalPerms = GetNumPermsForCA(TxnReq.acct_id, this);
    //distribution same as in the loader for now
    if (iAdditionalPerms == 0)
    {   //select the owner
        m_person->GetFirstLastAndTaxID(iCustomerId, TxnReq.exec_f_name,
                                    TxnReq.exec_l_name, TxnReq.exec_tax_id);
        bExecutorIsAccountOwner = true;
    }
    else
    {
        // If there is more than one permission set on the account,
        // have some distribution on whether the executor is still
        // the account owner, or it is one of the additional permissions.
        // Here we must take into account the fact that we've excluded
        // a large portion of customers that don't have any additional
        // executors in the above code (iAdditionalPerms == 0); the
        // "exec_is_owner" percentage implicity includes such customers
        // and must be factored out here.
        int exec_is_owner = (m_pDriverCETxnSettings->TO_settings.cur.exec_is_owner - iPercentAccountAdditionalPermissions_0) * 100 / (100 - iPercentAccountAdditionalPermissions_0);

        if (RandomPercent(r, exec_is_owner) )
        {
            m_person->GetFirstLastAndTaxID(iCustomerId, TxnReq.exec_f_name,
                                        TxnReq.exec_l_name, TxnReq.exec_tax_id);
            bExecutorIsAccountOwner = true;
        }
        else
        {
            if (iAdditionalPerms == 1)
            {
                //select the first non-owner
                GetCIDsForPermissions(TxnReq.acct_id, iCustomerId, this, &CID_1, NULL);
                m_person->GetFirstLastAndTaxID(CID_1, TxnReq.exec_f_name,
                    TxnReq.exec_l_name, TxnReq.exec_tax_id);
            }
            else
            {
                //select the second non-owner
                GetCIDsForPermissions(TxnReq.acct_id, iCustomerId, this, &CID_1, &CID_2);
                //generate third account permission row
                m_person->GetFirstLastAndTaxID(CID_2, TxnReq.exec_f_name,
                    TxnReq.exec_l_name, TxnReq.exec_tax_id);
            }
            bExecutorIsAccountOwner = false;
        }
    }

    // Select either stock symbol or company from the securities flat file.
    //
    //have some distribution on the company/symbol input preference
    if (RandomPercent(r, m_pDriverCETxnSettings->TO_settings.cur.security_by_symbol))
    {
        //Submit the symbol
        dfm->SecurityFile().CreateSymbol( iFlatFileSymbIndex, TxnReq.symbol, static_cast<int>(sizeof( TxnReq.symbol )));
        TxnReq.co_name[0] = '\0';
        TxnReq.issue[0] = '\0';
    }
    else
    {
        //Submit the company name
        dfm->CompanyFile().CreateName( dfm->SecurityFile().GetCompanyIndex( iFlatFileSymbIndex ), TxnReq.co_name, static_cast<int>(sizeof( TxnReq.co_name )));
		strncpy(TxnReq.issue, dfm->SecurityFile().GetRecord(iFlatFileSymbIndex).S_ISSUE_CSTR(),
                sizeof(TxnReq.issue));
        TxnReq.symbol[0] = '\0';
    }
    TxnReq.trade_qty = cTRADE_QTY_SIZES[RandomNumber(r, 0, cNUM_TRADE_QTY_SIZES - 1)];
    TxnReq.requested_price = RndDoubleIncrRange(r, fMinSecPrice, fMaxSecPrice, 0.01);
    // Determine whether Market or Limit order
    bMarket = RandomPercent(r, m_pDriverCETxnSettings->TO_settings.cur.market);
    //Determine whether Buy or Sell trade
    if (RandomPercent(r, m_pDriverCETxnSettings->TO_settings.cur.buy_orders))
    {
        if (bMarket)
        {
            //Market Buy
            eTradeType = eMarketBuy;
        }
        else
        {
            //Limit Buy
            eTradeType = eLimitBuy;
        }

        // Set margin or cash for Buy
        TxnReq.type_is_margin = RandomPercent(r, // type_is_margin is specified for all orders, but used only for buys
                                         m_pDriverCETxnSettings->TO_settings.cur.type_is_margin *
                                         100 /m_pDriverCETxnSettings->TO_settings.cur.buy_orders);
    }
    else
    {
        if (bMarket)
        {
            //Market Sell
            eTradeType = eMarketSell;
        }
        else
        {
            // determine whether the Limit Sell is a Stop Loss
            if (RandomPercent(r, m_pDriverCETxnSettings->TO_settings.cur.stop_loss))
            {
                //Stop Loss
                eTradeType = eStopLoss;
            }
            else
            {
                //Limit Sell
                eTradeType = eLimitSell;
            }
        }

        TxnReq.type_is_margin = false;  //all sell orders are cash
    }
    iTradeType = eTradeType;
    // Distribution of last-in-first-out flag
    TxnReq.is_lifo = RandomPercent(r, m_pDriverCETxnSettings->TO_settings.cur.lifo);
    // Copy the trade type id from the flat file
    strncpy(TxnReq.trade_type_id,
            GetTradeType(eTradeType), //(m_pTradeType->GetRecord(eTradeType))->TT_ID,
            sizeof(TxnReq.trade_type_id));
    // Copy the status type id's from the flat file
    strncpy(TxnReq.st_pending_id,
            GetStatusType(ePending), //(m_pStatusType->GetRecord(ePending))->ST_ID,
            sizeof(TxnReq.st_pending_id));
    strncpy(TxnReq.st_submitted_id,
            GetStatusType(eSubmitted),//(m_pStatusType->GetRecord(eSubmitted))->ST_ID,
            sizeof(TxnReq.st_submitted_id));
	INT32 m_iTradeOrderRollbackLevel = m_pDriverCETxnSettings->TO_settings.cur.rollback;
	INT32 m_iTradeOrderRollbackLimit = m_pDriverCETxnSettings->TxnMixGenerator_settings.cur.TradeOrderMixLevel;
    TxnReq.roll_it_back = ( m_iTradeOrderRollbackLevel >= RandomNumber(r, 1, m_iTradeOrderRollbackLimit ));

    void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCE_TRADE_ORDER);

    // db->init_txn(txn, cgraph, trade_order_type);

	scoped_str_arena s_arena(arena);
	ssize_t ret = 0;

	try{
    // Frame 1 (P1 - P3)
    // Raw Piece: P1 -- r [CUSTOMER_ACCOUNT](CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST)
    TIdent broker_id;
    TIdent cust_id;
    char tax_status;
    {
        //ic3_txn->start_raw_piece();
        CUSTOMER_ACCOUNT::key k;
        k.CA_ID = TxnReq.acct_id;
		ALWAYS_ASSERT(tbl_CUSTOMER_ACCOUNT(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        CUSTOMER_ACCOUNT::value v_ca_temp;
        const CUSTOMER_ACCOUNT::value *v = Decode(obj_v, v_ca_temp);
        broker_id = v->CA_B_ID;
        cust_id = v->CA_C_ID;
        tax_status = v->CA_TAX_ST;
        //ic3_txn->end_raw_piece();
    }

    // Raw Piece: P2 -- r [CUSTOMER](C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID)
    inline_str_fixed_tpce<TPCE::sm_cF_NAME_len> cust_f_name;
    inline_str_fixed_tpce<TPCE::sm_cL_NAME_len> cust_l_name;
    char cust_tier;
    inline_str_fixed_tpce<TPCE::sm_cTAX_ID_len> tax_id;
    {
        //ic3_txn->start_raw_piece();
        CUSTOMER::key k;
        k.C_ID = cust_id;
		ALWAYS_ASSERT(tbl_CUSTOMER(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
		CUSTOMER::value v_c_temp;
        const CUSTOMER::value *v = Decode(obj_v, v_c_temp);
        cust_f_name = v->C_F_NAME;
        cust_l_name = v->C_L_NAME;
        cust_tier = v->C_TIER;
        tax_id = v->C_TAX_ID;
        //ic3_txn->end_raw_piece();
    }

    // Raw Piece: P3 -- r [BROKER](B_NAME)
    {
        //ic3_txn->start_raw_piece();
        BROKER::key k;
        k.B_ID = broker_id;
		ALWAYS_ASSERT(tbl_BROKER(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
		BROKER::value v_b_temp;
        const BROKER::value *v = Decode(obj_v, v_b_temp);
        ALWAYS_ASSERT(v);
        //ic3_txn->end_raw_piece();
    }
    // end Frame 1

    // Frame 2 (P4)
    // Raw Piece: P4 -- r [ACCOUNT_PERMISSION](AP_F_NAME, AP_L_NAME, AP_ACL)
    {
        //ic3_txn->start_raw_piece();
        if (strcmp(cust_l_name.data(), TxnReq.exec_l_name) != 0 ||
            strcmp(cust_f_name.data(), TxnReq.exec_f_name) != 0 ||
            strcmp(tax_id.data(), TxnReq.exec_tax_id) != 0) {
            ACCOUNT_PERMISSION::key k;
            k.AP_CA_ID = TxnReq.acct_id;
            k.AP_TAX_ID.assign(TxnReq.exec_tax_id);
            if (unlikely(!tbl_ACCOUNT_PERMISSION(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v))) {
		        db->abort_txn(txn);
                is_user_initiate_abort = true;
    		    return txn_result(false, 0);
    	  	} else {
    	  		ACCOUNT_PERMISSION::value v_ap_temp;
                const ACCOUNT_PERMISSION::value *v = Decode(obj_v, v_ap_temp);
                if (v->AP_ACL.data()[0] == '\0' ||
                    strcmp(v->AP_L_NAME.data(), TxnReq.exec_l_name) != 0 ||
                    strcmp(v->AP_F_NAME.data(), TxnReq.exec_f_name) != 0) {
                    // abort
                    db->abort_txn(txn);
                    is_user_initiate_abort = true;
    		    	return txn_result(false, 0);
                }
            }
        }
        //ic3_txn->end_raw_piece();
    }
    // end Frame 2

    // Frame 3 (P5 - P12)
    double buy_value = 0.0, sell_value = 0.0;
    int32_t needed_qty = TxnReq.trade_qty;
    inline_str_fixed_tpce<TPCE::sm_cEX_ID_len> exch_id;
    inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len> symbol;
    TIdent co_id;

    // Raw Piece: P5 -- scan [SECURITY, COMPANY]
    {
        //ic3_txn->start_raw_piece();
        if (TxnReq.symbol[0] == '\0') {
            // scan [COMPANY_CO_NAME]
            {
                COMPANY_CO_NAME::key k_lower, k_upper;
                k_lower.CO2_CO_NAME.assign(TxnReq.co_name);
                k_lower.CO2_CO_ID_K = 0;//numeric_limits<TIdent>::min();
                k_upper.CO2_CO_NAME.assign(TxnReq.co_name);
                k_upper.CO2_CO_ID_K = numeric_limits<TIdent>::max();
                scan_limit_callback<1> cb(true);
				tbl_COMPANY_CO_NAME(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
                ALWAYS_ASSERT(cb.size() == 1);
                std::string *mcompany= cb.values[0].second;
				COMPANY_CO_NAME::value v_co_temp;
                const COMPANY_CO_NAME::value *v = Decode(*mcompany, v_co_temp);
                co_id = v->CO2_CO_ID_V;
            }

            // scan [SECURITY_S_CO] r [SECURITY]
            {
                static_assert(TPCE::sm_cSYMBOL_len <= str_len, "xx");
                SECURITY_S_CO_ID::key k_lower, k_upper;
                k_lower.S1_S_CO_ID = co_id;
                k_lower.S1_S_ISSUE.assign(TxnReq.issue);
                k_lower.S1_S_SYMB_K.assign(min_str.c_str(), TPCE::sm_cSYMBOL_len);
                k_upper.S1_S_CO_ID = co_id;
                k_upper.S1_S_ISSUE.assign(TxnReq.issue);
                k_upper.S1_S_SYMB_K.assign(max_str.c_str(), TPCE::sm_cSYMBOL_len);
                scan_limit_callback<1> cb(true);
				tbl_SECURITY_S_CO_ID(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
                ALWAYS_ASSERT(cb.size() == 1);
                std::string *msecurity = cb.values[0].second;
				SECURITY_S_CO_ID::value v_se_temp;
                const SECURITY_S_CO_ID::value *v = Decode(*msecurity, v_se_temp);

                const SECURITY::key sk(v->S1_S_SYMB_V);
				ALWAYS_ASSERT(tbl_SECURITY(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, sk), obj_v));
				SECURITY::value sv_temp;
                const SECURITY::value *sv = Decode(obj_v, sv_temp);
                exch_id = sv->S_EX_ID;
                symbol = v->S1_S_SYMB_V;
            }
        } else {
            symbol.assign(TxnReq.symbol);
            // r [SECURITY]
            {
                const SECURITY::key k(symbol);
				ALWAYS_ASSERT(tbl_SECURITY(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
                SECURITY::value v_se_temp;
                const SECURITY::value *v = Decode(obj_v, v_se_temp);
                co_id = v->S_CO_ID;
                exch_id = v->S_EX_ID;
            }

            // r [COMPANY]
            {
                const COMPANY::key k(co_id);
				ALWAYS_ASSERT(tbl_COMPANY(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
                COMPANY::value v_co_temp;
                const COMPANY::value *v = Decode(obj_v, v_co_temp);
                ALWAYS_ASSERT(v);
            }
        }
        //ic3_txn->end_raw_piece();
    }

    bool type_is_market, type_is_sell;
    // Raw Piece: P6 -- r [TRADE_TYPE]
    {
        //ic3_txn->start_raw_piece();
        TRADE_TYPE::key k;
        k.TT_ID.assign(TxnReq.trade_type_id);
		ALWAYS_ASSERT(tbl_TRADE_TYPE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        TRADE_TYPE::value v_tt_temp;
        const TRADE_TYPE::value *v = Decode(obj_v, v_tt_temp);
        type_is_market = char2bool(v->TT_IS_MRKT);
        type_is_sell = char2bool(v->TT_IS_SELL);
        //ic3_txn->end_raw_piece();
    }

    int32_t hs_qty = 0;
    std::list<std::pair<int32_t, inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len> > > hs_list;
    // Conflict Piece: P7 -- r [HOLDING_SUMMARY]
    // TRADE_ORDER: 1
trade_order_holding_summary:
    hs_list.clear();
    hs_qty = 0;
    // try 
    {
        // db->mul_ops_begin(txn);
        if (TxnReq.type_is_margin) {
            HOLDING_SUMMARY::key k_lower, k_upper;
            k_lower.HS_CA_ID = TxnReq.acct_id;
            k_lower.HS_S_SYMB.assign(min_str.c_str(), TPCE::sm_cSYMBOL_len);
            k_upper.HS_CA_ID = TxnReq.acct_id;
            k_upper.HS_S_SYMB.assign(max_str.c_str(), TPCE::sm_cSYMBOL_len);
            scan_callback_normal cb(false);
			tbl_HOLDING_SUMMARY(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
            bool hs_found = false;
            for (size_t si = 0; si < cb.size(); si++) {
                HOLDING_SUMMARY::key k_temp;
                const HOLDING_SUMMARY::key *kk = Decode(*cb.values[si].first, k_temp);
				HOLDING_SUMMARY::value v_hs_temp;
                const HOLDING_SUMMARY::value *v = Decode(*cb.values[si].second, v_hs_temp);
                int qty_tmp = v->HS_QTY;
                if (symbol == kk->HS_S_SYMB) {
                    hs_qty = v->HS_QTY;
                    hs_found = true;
                    hs_list.emplace_back(qty_tmp, kk->HS_S_SYMB);
                } else if (qty_tmp != 0) {
                    hs_list.emplace_back(qty_tmp, kk->HS_S_SYMB);
                }
            }
            if (!hs_found) {
                hs_list.insert(hs_list.begin(), std::make_pair(0, symbol));
            }
        } else {
            const HOLDING_SUMMARY::key k(TxnReq.acct_id, symbol);
			bool has_hs = tbl_HOLDING_SUMMARY(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v);
			if(has_hs){
            	HOLDING_SUMMARY::value v_hs_temp;
            	const HOLDING_SUMMARY::value *v = Decode(obj_v, v_hs_temp);
            	hs_qty = v->HS_QTY;
			}
			else
				hs_qty = 0;
        }
        // if (!db->mul_ops_end(txn)) {
            // goto trade_order_holding_summary;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_order_holding_summary;
    // }

    std::vector<hold_t> hold_tuple_list;
    // Conflict Piece: P8 -- r [HOLDING]
    // TRADE_ORDER: 2
trade_order_holding:
    hold_tuple_list.clear();
    // try 
    {
        // db->mul_ops_begin(txn);
        //ic3_txn->start_conflict_piece();
        if (type_is_sell) { // sell order
            if (hs_qty > 0) {
                HOLDING_H_CA_ID::key k_lower, k_upper;
                k_lower.H1_H_CA_ID = TxnReq.acct_id;
                k_lower.H1_H_S_SYMB = symbol;
                k_lower.H1_H_T_ID_K = 0;//numeric_limits<TTrade>::min();
                k_upper.H1_H_CA_ID = TxnReq.acct_id;
                k_upper.H1_H_S_SYMB = symbol;
                k_upper.H1_H_T_ID_K = numeric_limits<TTrade>::max();
                scan_callback_normal cb(true);
				tbl_HOLDING_H_CA_ID(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
                std::set<TTrade> tid_set;
#ifdef EARLY_ABORT
                if (unlikely(cb.size() == 0)) {
                    db->abort_txn(txn);
                    is_user_initiate_abort = true;
                    return txn_result(false, 0);
                }
#endif

                // XXX hacky: have to sort by H_T_ID
                for (size_t cb_i = 0; cb_i < cb.size(); cb_i++) {
					HOLDING_H_CA_ID::value v_h_temp;
                    const HOLDING_H_CA_ID::value *v = Decode(*cb.values[cb_i].second, v_h_temp);
                    tid_set.insert(v->H1_H_T_ID_V);
                }

                hold_tuple_list.reserve(tid_set.size());
                for (auto h_t_id : tid_set) {
                    const HOLDING::key k(h_t_id);
                    if (unlikely(!tbl_HOLDING(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v))) {
#ifdef EARLY_ABORT
                        db->abort_txn(txn);
                        is_user_initiate_abort = true;
                        return txn_result(false, 0);
#else
                        continue;
#endif
                    }
                    HOLDING::value v_h_temp;
                    const HOLDING::value *v = Decode(obj_v, v_h_temp);
                    int_date_time dts_tmp = v->H_DTS;
                    float price_tmp = v->H_PRICE;
                    int qty_tmp = v->H_QTY;
                    hold_tuple_list.emplace_back(h_t_id, dts_tmp, price_tmp, qty_tmp);
                }
            }
        } else { // buy order
            if (hs_qty < 0) {
                HOLDING_H_CA_ID::key k_lower, k_upper;
                k_lower.H1_H_CA_ID = TxnReq.acct_id;
                k_lower.H1_H_S_SYMB = symbol;
                k_lower.H1_H_T_ID_K = 0;//numeric_limits<TTrade>::min();
                k_upper.H1_H_CA_ID = TxnReq.acct_id;
                k_upper.H1_H_S_SYMB = symbol;
                k_upper.H1_H_T_ID_K = numeric_limits<TTrade>::max();
                scan_callback_normal cb(true);
				tbl_HOLDING_H_CA_ID(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
                std::set<TTrade> tid_set;

#ifdef EARLY_ABORT
                if (unlikely(cb.size() == 0)) {
                    db->abort_txn(txn);
                    is_user_initiate_abort = true;
                    return txn_result(false, 0);
                }
#endif
                // XXX hacky: have to sort by H_T_ID
                for (size_t cb_i = 0; cb_i < cb.size(); cb_i++) {
                    HOLDING_H_CA_ID::value v_h_temp;
                    const HOLDING_H_CA_ID::value *v = Decode(*cb.values[cb_i].second, v_h_temp);
                    tid_set.insert(v->H1_H_T_ID_V);
                }

                hold_tuple_list.reserve(tid_set.size());
                for (auto h_t_id : tid_set) {
                    const HOLDING::key k(h_t_id);
                    if (unlikely(!tbl_HOLDING(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v))) {
#ifdef EARLY_ABORT
                        db->abort_txn(txn);
                        is_user_initiate_abort = true;
                        return txn_result(false, 0);
#else
                        continue;
#endif
                    }
                    HOLDING::value v_h_temp;
                    const HOLDING::value *v = Decode(obj_v, v_h_temp);
                    int_date_time dts_tmp = v->H_DTS;
                    float price_tmp = v->H_PRICE;
                    int qty_tmp = v->H_QTY;
                    hold_tuple_list.emplace_back(h_t_id, dts_tmp, price_tmp, qty_tmp);
                }
            }
        }
        //ic3_txn->end_conflict_piece();
        // if (!db->mul_ops_end(txn)) {
            // goto trade_order_holding;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_order_holding;
    // }

    float market_price = 0.0;
    double hold_assets = 0.0;
    float requested_price = 0.0;
    // Conflict Piece: P9 -- r [LAST_TRADE]
    // TRADE_ORDER: 3
trade_order_last_trade:
    market_price = 0.0;
    hold_assets = 0.0;
    requested_price = 0.0;
    // try 
    {
        // db->mul_ops_begin(txn);
        //ic3_txn->start_conflict_piece();
        if (TxnReq.type_is_margin) {
            bool hs_found = false;
            for (auto &it : hs_list) {
                const LAST_TRADE::key k(it.second);
				ALWAYS_ASSERT(tbl_LAST_TRADE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
                LAST_TRADE::value v_lt_temp;
                const LAST_TRADE::value *v = Decode(obj_v, v_lt_temp);
                hold_assets += v->LT_PRICE * it.first;
                if (!hs_found && symbol == it.second) {
                    market_price = v->LT_PRICE;
                    requested_price = market_price;
                }
            }
        } else {
            const LAST_TRADE::key k(symbol);
            ALWAYS_ASSERT(tbl_LAST_TRADE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
            LAST_TRADE::value v_lt_temp;
            const LAST_TRADE::value *v = Decode(obj_v, v_lt_temp);
            market_price = v->LT_PRICE;
        }
        //ic3_txn->end_conflict_piece();
        // if (!db->mul_ops_end(txn)) {
            // goto trade_order_last_trade;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_order_last_trade;
    // }

    {
        std::sort(hold_tuple_list.begin(), hold_tuple_list.end());
        if (type_is_sell) {
            if (TxnReq.is_lifo) {
                // reverse
                for (auto it = hold_tuple_list.rbegin();
                        it != hold_tuple_list.rend(); it++) {
                    if (it->hold_qty >= needed_qty) {
                        buy_value += needed_qty * it->hold_price;
                        sell_value += needed_qty * requested_price;
                        needed_qty = 0;
                        break;
                    } else {
                        buy_value += it->hold_qty * it->hold_price;
                        sell_value += it->hold_qty * requested_price;
                        needed_qty -= it->hold_qty;
                    }
                }
            } else {
                for (auto it = hold_tuple_list.begin();
                        it != hold_tuple_list.end(); it++) {
                    if (it->hold_qty >= needed_qty) {
                        buy_value += needed_qty * it->hold_price;
                        sell_value += needed_qty * requested_price;
                        needed_qty = 0;
                        break;
                    } else {
                        buy_value += it->hold_qty * it->hold_price;
                        sell_value += it->hold_qty * requested_price;
                        needed_qty -= it->hold_qty;
                    }
                }
            }
        } else {
            if (TxnReq.is_lifo) {
                // reverse
                for (auto it = hold_tuple_list.rbegin();
                        it != hold_tuple_list.rend(); it++) {
                    if (it->hold_qty + needed_qty <= 0) {
                        sell_value += needed_qty * it->hold_price;
                        buy_value += needed_qty * requested_price;
                        needed_qty = 0;
                        break;
                    } else {
                        it->hold_qty = -it->hold_qty;
                        sell_value += it->hold_qty * it->hold_price;
                        buy_value += it->hold_qty * requested_price;
                        needed_qty -= it->hold_qty;
                    }
                }
            } else {
                for (auto it = hold_tuple_list.begin();
                        it != hold_tuple_list.end(); it++) {
                    if (it->hold_qty + needed_qty <= 0) {
                        sell_value += needed_qty * it->hold_price;
                        buy_value += needed_qty * requested_price;
                        needed_qty = 0;
                        break;
                    } else {
                        it->hold_qty = -it->hold_qty;
                        sell_value += it->hold_qty * it->hold_price;
                        buy_value += it->hold_qty * requested_price;
                        needed_qty -= it->hold_qty;
                    }
                }
            }
        }
    }

    float tax_amount = 0.0;
    // Raw Piece: P10 -- r [CUSTOMER_TAXRATE, TAX_RATE]
    {
        //ic3_txn->start_raw_piece();
        if (sell_value > buy_value && (tax_status == 1 || tax_status == 2)) {
            CUSTOMER_TAXRATE::key k_lower, k_upper;
            k_lower.CX_C_ID = cust_id;
            k_lower.CX_TX_ID.assign(min_str.c_str(), TPCE::sm_cTX_ID_len);
            k_upper.CX_C_ID = cust_id;
            k_upper.CX_TX_ID.assign(max_str.c_str(), TPCE::sm_cTX_ID_len);

            scan_callback_normal cb(false);
			tbl_CUSTOMER_TAXRATE(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
            float tax_rates = 0.0;
            for (size_t si = 0; si < cb.size(); si++) {
                CUSTOMER_TAXRATE::key k_temp;
                const CUSTOMER_TAXRATE::key *kk = Decode(*cb.values[si].first, k_temp);
                const TAX_RATE::key k(kk->CX_TX_ID);
				ALWAYS_ASSERT(tbl_TAX_RATE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
                TAX_RATE::value v_tr_temp;
                const TAX_RATE::value *v = Decode(obj_v, v_tr_temp);
                tax_rates += v->TX_RATE;
            }
            tax_amount += (sell_value - buy_value) * tax_rates;
        }
        //ic3_txn->end_raw_piece();
    }

    float comm_rate;
    // Raw Piece: P11 -- r [COMMISSION_RATE]
    {
        //ic3_txn->start_raw_piece();
        COMMISSION_RATE::key k_lower, k_upper;
        k_lower.CR_C_TIER = cust_tier;
        k_lower.CR_TT_ID.assign(TxnReq.trade_type_id);
        k_lower.CR_EX_ID = exch_id;
        k_lower.CR_FROM_QTY = 0;//numeric_limits<int32_t>::min();
        k_upper.CR_C_TIER = cust_tier;
        k_upper.CR_TT_ID.assign(TxnReq.trade_type_id);
        k_upper.CR_EX_ID = exch_id;
        k_upper.CR_FROM_QTY = TxnReq.trade_qty;

        scan_limit_callback<1> cb(true);
		tbl_COMMISSION_RATE(1/*TODO*/)->rscan(txn, Encode(obj_key0, k_upper), &Encode(obj_key1, k_lower),
					cb, s_arena.get());
        ALWAYS_ASSERT(cb.size() == 1);
		COMMISSION_RATE::value v_cr_temp;
        const COMMISSION_RATE::value *v = Decode(*cb.values[0].second, v_cr_temp);
        ALWAYS_ASSERT(v->CR_TO_QTY >= TxnReq.trade_qty);
        comm_rate = v->CR_RATE;
        //ic3_txn->end_raw_piece();
    }

    float charge_amount = 0.0;
    // Raw Piece: P12 -- r [CHARGE]
    {
        //ic3_txn->start_raw_piece();
        CHARGE::key k;
        k.CH_TT_ID.assign(TxnReq.trade_type_id);
        k.CH_C_TIER = cust_tier;
		ALWAYS_ASSERT(tbl_CHARGE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        CHARGE::value v_c_temp;
        const CHARGE::value *v = Decode(obj_v, v_c_temp);
        charge_amount = v->CH_CHRG;
        //ic3_txn->end_raw_piece();
    }

    char *status_id = NULL;
    if (type_is_market)
        status_id = TxnReq.st_submitted_id;
    else
        status_id = TxnReq.st_pending_id;
    // end frame 3

    // Frame 4 (P13 - P15)
    const TTrade trade_id = get_unique_trade_id();
    int_date_time now_dts;
    // Conflict Piece: P13 -- i [TRADE]
    // TRADE_ORDER: 4
trade_order_trade:
    // try 
    {
        // db->mul_ops_begin(txn);
        // comm_amount
        float comm_amount = (comm_rate / 100) * TxnReq.trade_qty * requested_price;
        // exec_name
        char exec_name[TPCE::sm_cL_NAME_len + TPCE::sm_cF_NAME_len + 2];
        exec_name[0] = 0;
        strcat(exec_name, TxnReq.exec_f_name);
        strcat(exec_name, " ");
        strcat(exec_name, TxnReq.exec_l_name);
        // is_cash
        char is_cash = bool2char(!TxnReq.type_is_margin);
        // trade id
        /*{
            TRADE::key k(trade_id);
            if (tbl_TRADE(1)->get(txn, Encode(obj_key0, k), obj_v)) {
                printf("FFFFFFFFFF\n");
                TRADE_HISTORY::key kk(trade_id, st_submitted_id);
                if (tbl_TRADE_HISTORY(1)->get(txn, Encode(obj_key0, kk), obj_v)) {
                    printf("OK\n");
                }
                ALWAYS_ASSERT(0);
            }
        }*/
        //ic3_txn->start_conflict_piece();
        TRADE::key k;
        k.T_ID = trade_id;
        TRADE::value v;
        CDateTime now;
        now_dts = now.GetTime();
        v.T_DTS = now_dts;
        v.T_ST_ID.assign(status_id);
        v.T_TT_ID.assign(TxnReq.trade_type_id);
        v.T_IS_CASH = is_cash;
        v.T_S_SYMB = symbol;
        v.T_QTY = TxnReq.trade_qty;
        v.T_BID_PRICE = requested_price;
        v.T_CA_ID = TxnReq.acct_id;
        v.T_EXEC_NAME.assign(exec_name);
        v.T_TRADE_PRICE = 0.0;
        v.T_CHRG = charge_amount;
        v.T_COMM = comm_amount;
        v.T_TAX = 0;
        v.T_LIFO = bool2char(TxnReq.is_lifo);

        insert_trade(k, v, txn);
		TRADE_T_CA_ID::value t1;
		TRADE_T_S_SYMB::value t2;
		ret += Size(v);
		ret += Size(t1);
		ret += Size(t2);

        // if (!db->mul_ops_end(txn)) {
            // goto trade_order_trade;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_order_trade;
    // }

    // Conflict Piece: P14 -- i [TRADE_REQUEST]
    // TRADE_ORDER: 5
trade_order_trade_request:
    // try 
    {
        // db->mul_ops_begin(txn);
        if (type_is_market) {
            TRADE_REQUEST::key k;
            k.TR_T_ID = trade_id;
            TRADE_REQUEST::value v;
            v.TR_TT_ID.assign(TxnReq.trade_type_id);
            v.TR_S_SYMB = symbol;
            v.TR_QTY = TxnReq.trade_qty;
            v.TR_BID_PRICE = requested_price;
            v.TR_B_ID = broker_id;

			TRADE_REQUEST_TR_B_ID::value t1;
			TRADE_REQUEST_TR_S_SYMB::value t2;
            insert_trade_request(k, v, txn);
			ret += Size(v);
			ret += Size(t1);
			ret += Size(t2);
        }
        // if (!db->mul_ops_end(txn)) {
            // goto trade_order_trade_request;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_order_trade_request;
    // }

    // Conflict Piece: P15 -- i [TRADE_HISTORY]
    // TRADE_ORDER: 6
trade_order_trade_history:
    // try 
    {
        // db->one_op_begin(txn);
        TRADE_HISTORY::key k;
        k.TH_T_ID = trade_id;
        k.TH_ST_ID.assign(status_id);
        TRADE_HISTORY::value v;
        v.TH_DTS = now_dts;
		tbl_TRADE_HISTORY(1/*TODO*/)->insert(txn, Encode(str(), k), Encode(str(), v));
		ret += Size(v);
        // if (!db->one_op_end(txn)) {
            // goto trade_order_trade_history;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_order_trade_history;
    // }
    // // end Frame 4

    // re-ordered Frame 3
    UNUSED double acct_assets = 0.0;
    // Conflict Piece: P16 -- r [CUSTOMER_ACCOUNT]
    // TRADE_ORDER: 7
trade_order_customer_account:
    // try 
    {
        // db->one_op_begin(txn);
        if (TxnReq.type_is_margin) {
            CUSTOMER_ACCOUNT::key k;
            k.CA_ID = TxnReq.acct_id;
            ALWAYS_ASSERT(tbl_CUSTOMER_ACCOUNT(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
			CUSTOMER_ACCOUNT::value v_ca_temp;
            const CUSTOMER_ACCOUNT::value *v = Decode(obj_v, v_ca_temp);
            acct_assets = hold_assets + v->CA_BAL;
            INVARIANT(&acct_assets != (double *)0x01); // make the compiler happy
        }
        // if (!db->one_op_end(txn)) {
            // goto trade_order_customer_account;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_order_customer_account;
    // }
    // end re-ordered Frame 3

    // Frame 5 (rollback)
    if (TxnReq.roll_it_back) {
       //user init abort
       db->abort_txn(txn);
       is_user_initiate_abort = true;
       return txn_result(false, 0);
    }
    // end Frame 5

    // Frame 6
	if (likely(db->commit_txn(txn))){
		if (type_is_market) {
	        // process order, add to traderesultqueue
#ifndef MANUAL_MARKET_FEED
            TTradeResultTxnInput trti;
            trti.trade_price = requested_price;
            trti.trade_id = trade_id;
            traderesultqueue.push(trti);
#endif
    	} else {
            if (iFlatFileSymbIndex % 1000 >= market_ignore_rate) {
                // limit order, add to limitorderqueue wait for market feed
                limitorderqueue.emplace_back();
                TTickerEntry &last = limitorderqueue.back();
                last.trade_qty = TxnReq.trade_qty;
                last.price_quote = requested_price;
                memcpy(last.symbol, symbol.data(), TPCE::sm_cSYMBOL_len);
            }
    	}
        // end Frame 6

      	return txn_result(true, ret);
	}
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}

	return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_trade_result()
{
    TTradeResultTxnInput &TxnReq = traderesultqueue.front();
    //traderesultqueue.pop(); should not pop queue until this txn succeed

    void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCE_TRADE_RESULT);
    // db->init_txn(txn, cgraph, trade_result_type);
/*     {
 *         TRADE::key k;
 *         k.T_ID = 200000000872681;
 *         std::string buf;
 *         bool result = tbl_TRADE(1)->get(txn, "1", buf);
 * 
 *         // TRADE::value v_t_temp;
 *         // const TRADE::value *v = Decode(buf, v_t_temp);
 *         printf("trade_result > res: %d, id: %ld, value: %s\n", result ? 1 : 0, 0, buf.c_str());
 *         db->commit_txn(txn);
 * 
 *         txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCE_TRADE_RESULT);
 *     } */


	scoped_str_arena s_arena(arena);

	try{
	ssize_t ret = 0;
    TIdent acct_id;
    inline_str_fixed_tpce<TPCE::sm_cTT_ID_len> type_id;
    inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len> symbol;
    int32_t trade_qty;
    float charge;
    bool is_lifo;
    bool trade_is_cash;
    CDateTime current;
    int_date_time trade_dts = current.GetTime();
    // Frame 1 (P1 - P3)
    // Raw Piece: P1 -- r [TRADE]
    {
        //ic3_txn->start_raw_piece();
        TRADE::key k;
        k.T_ID = TxnReq.trade_id;
        ALWAYS_ASSERT(tbl_TRADE(1)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        TRADE::value v_t_temp;
        const TRADE::value *v = Decode(obj_v, v_t_temp);
        acct_id = v->T_CA_ID;
        type_id = v->T_TT_ID;
        symbol = v->T_S_SYMB;
        trade_qty = v->T_QTY;
        charge = v->T_CHRG;
        is_lifo = char2bool(v->T_LIFO);
        trade_is_cash = char2bool(v->T_IS_CASH);
        //ic3_txn->end_raw_piece();
    }

    inline_str_fixed_tpce<TPCE::sm_cTT_NAME_len> type_name;
    bool type_is_sell/*, type_is_market*/;
    // Raw Piece: P2 -- r [TRADE_TYPE]
    {
        //ic3_txn->start_raw_piece();
        const TRADE_TYPE::key k(type_id);
        ALWAYS_ASSERT(tbl_TRADE_TYPE(1)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        TRADE_TYPE::value v_tt_temp;
        const TRADE_TYPE::value *v = Decode(obj_v, v_tt_temp);
        type_name = v->TT_NAME;
        type_is_sell = char2bool(v->TT_IS_SELL);
        //type_is_market = char2bool(v->TT_IS_MRKT);
        //ic3_txn->end_raw_piece();
    }

    TIdent broker_id, cust_id;
    char tax_status;
    // Raw Piece: P3 -- r [CUSTOMER_ACCOUNT]
    {
        //ic3_txn->start_raw_piece();
        const CUSTOMER_ACCOUNT::key k(acct_id);
		ALWAYS_ASSERT(tbl_CUSTOMER_ACCOUNT(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        CUSTOMER_ACCOUNT::value v_ca_temp;
        const CUSTOMER_ACCOUNT::value *v = Decode(obj_v, v_ca_temp);
        broker_id = v->CA_B_ID;
        cust_id = v->CA_C_ID;
        tax_status = v->CA_TAX_ST;
        //ic3_txn->end_raw_piece();
    }

    // end Frame 1 (P4 - P6)

    // Frame 2
    int32_t hs_qty = 0;
    int32_t needed_qty = trade_qty;
    float buy_value = 0.0, sell_value = 0.0;
    // Conflict Piece: P4 -- r & i/w [HOLDING_SUMMARY]
    // TRADE_RESULT: 1
trade_result_holding_summary:
    hs_qty = 0;
    needed_qty = trade_qty;
    buy_value = 0.0;
    sell_value = 0.0;
    // try 
    {
        // db->mul_ops_begin(txn);
        HOLDING_SUMMARY::key k;
        k.HS_CA_ID = acct_id;
        k.HS_S_SYMB = symbol;
        bool has_hs = tbl_HOLDING_SUMMARY(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v);
        HOLDING_SUMMARY::value *v = NULL;
        if (has_hs) {
			HOLDING_SUMMARY::value v_hs_temp;
            v = (HOLDING_SUMMARY::value *)Decode(obj_v, v_hs_temp);
            hs_qty = v->HS_QTY;
        }
        if (type_is_sell) {
            if (!has_hs) {
                HOLDING_SUMMARY::value vv;
                vv.HS_QTY = -trade_qty;
				tbl_HOLDING_SUMMARY(1/*TODO*/)->insert(txn, Encode(str(), k), Encode(str(), vv));
            } else {
            	HOLDING_SUMMARY::value v_hs_new(*v);
                v_hs_new.HS_QTY = hs_qty - trade_qty;
		        tbl_HOLDING_SUMMARY(1/*TODO*/)->put(txn, Encode(str(), k), Encode(str(), v_hs_new));
            }
        } else {
            if (!has_hs) {
                HOLDING_SUMMARY::value vv;
                vv.HS_QTY = trade_qty;
				tbl_HOLDING_SUMMARY(1/*TODO*/)->insert(txn, Encode(str(), k), Encode(str(), vv));
            } else {
            	HOLDING_SUMMARY::value v_hs_new(*v);
                v_hs_new.HS_QTY = hs_qty + trade_qty;
		        tbl_HOLDING_SUMMARY(1/*TODO*/)->put(txn, Encode(str(), k), Encode(str(), v_hs_new));
            }
        }
        // if (!db->mul_ops_end(txn)) {
            // goto trade_result_holding_summary;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_holding_summary;
    // }

    std::list<std::pair<HOLDING_HISTORY::key, HOLDING_HISTORY::value> > hh_kv;
    std::vector<hold_kv_t> hold_tuple_list;
    // Conflict Piece: P5 -- r [HOLDING]
    // TRADE_RESULT: 2
trade_result_holding:
    hh_kv.clear();
    hold_tuple_list.clear();
    // try 
    {
        // db->mul_ops_begin(txn);
        //ic3_txn->start_conflict_piece();
        if (type_is_sell) { // sell order
            if (hs_qty > 0) {
                // scan secondary index of HOLDING
                HOLDING_H_CA_ID::key k_lower, k_upper;
                k_lower.H1_H_CA_ID = acct_id;
                k_lower.H1_H_S_SYMB = symbol;
                k_lower.H1_H_T_ID_K = 0;//numeric_limits<TTrade>::min();
                k_upper.H1_H_CA_ID = acct_id;
                k_upper.H1_H_S_SYMB = symbol;
                k_upper.H1_H_T_ID_K = numeric_limits<TTrade>::max();
                scan_callback_normal cb(true);
				tbl_HOLDING_H_CA_ID(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
                std::set<TTrade> tid_set;
#ifdef EARLY_ABORT
                if (unlikely(cb.size() == 0)) {
                    db->abort_txn(txn);
                    is_user_initiate_abort = true;
                    return txn_result(false, 0);
                }
#endif

                // get the query output
                for (size_t cb_i = 0; cb_i < cb.size(); cb_i++) {
					HOLDING_H_CA_ID::value v_h_temp;
                    const HOLDING_H_CA_ID::value *v = Decode(*cb.values[cb_i].second, v_h_temp);
                    // XXX hacky: have to sort by H_T_ID
                    tid_set.insert(v->H1_H_T_ID_V);
                }

                // get HOLDING for w/d
                hold_tuple_list.reserve(tid_set.size());
				HOLDING::value v_h_temp[tid_set.size()];
				int index = 0;
                for (auto h_t_id : tid_set) {
                    const HOLDING::key k(h_t_id);
					//need put afterwards
					if (unlikely(!(tbl_HOLDING(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v)))) {
#ifdef EARLY_ABORT
                        db->abort_txn(txn);
                        is_user_initiate_abort = true;
                        return txn_result(false, 0);
#else
                        continue;
#endif
                    }
                    const HOLDING::value *v = Decode(obj_v, v_h_temp[index++]);
                    hold_tuple_list.emplace_back(k, (HOLDING::value *)v);
                }

                std::sort(hold_tuple_list.begin(), hold_tuple_list.end());
                if (is_lifo) {
                    // reverse
                    for (auto it = hold_tuple_list.rbegin();
                            it != hold_tuple_list.rend(); it++) {
                        int32_t hold_qty = it->value->H_QTY;
                        double hold_price = it->value->H_PRICE;
                        if (hold_qty >= needed_qty) {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_front();
                            hh_kv.front().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.front().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.front().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.front().second.HH_AFTER_QTY = hold_qty - needed_qty;

                            if (hold_qty == needed_qty) {
                                // delete HOLDING
                                remove_holding((*it), txn);
                            } else {
                            	HOLDING::value v_h_new(*(it->value));
                                v_h_new.H_QTY -= needed_qty;
								//add put here
								tbl_HOLDING(1/*TODO*/)->put(txn, Encode(str(), it->key), Encode(str(), v_h_new));
                            }
                            buy_value += needed_qty * hold_price;
                            sell_value += needed_qty * TxnReq.trade_price;
                            needed_qty = 0;
                            break;
                        } else {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_front();
                            hh_kv.front().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.front().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.front().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.front().second.HH_AFTER_QTY = 0;

                            // delete HOLDING
                            remove_holding((*it), txn);
                            buy_value += hold_qty * hold_price;
                            sell_value += hold_qty * TxnReq.trade_price;
                            needed_qty -= hold_qty;
                        }
                    }
                } else {
                    for (auto it = hold_tuple_list.begin();
                            it != hold_tuple_list.end(); it++) {
                        int32_t hold_qty = it->value->H_QTY;
                        double hold_price = it->value->H_PRICE;
                        if (hold_qty >= needed_qty) {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_back();
                            hh_kv.back().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.back().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.back().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.back().second.HH_AFTER_QTY = hold_qty - needed_qty;

                            if (hold_qty == needed_qty) {
                                // delete HOLDING
                                remove_holding((*it), txn);
                            } else {
								HOLDING::value v_h_new(*(it->value));
                                v_h_new.H_QTY -= needed_qty;
								//add put here
								tbl_HOLDING(1/*TODO*/)->put(txn, Encode(str(), it->key), Encode(str(), v_h_new));
                            }
                            buy_value += needed_qty * hold_price;
                            sell_value += needed_qty * TxnReq.trade_price;
                            needed_qty = 0;
                            break;
                        } else {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_back();
                            hh_kv.back().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.back().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.back().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.back().second.HH_AFTER_QTY = 0;

                            // delete HOLDING
                            remove_holding((*it), txn);
                            buy_value += hold_qty * hold_price;
                            sell_value += hold_qty * TxnReq.trade_price;
                            needed_qty -= hold_qty;
                        }
                    }
                }
            }
            if (needed_qty > 0) {
                // insert HOLDING_HISTORY
                hh_kv.emplace_back();
                hh_kv.back().first.HH_H_T_ID = TxnReq.trade_id;
                hh_kv.back().first.HH_T_ID = TxnReq.trade_id;
                hh_kv.back().second.HH_BEFORE_QTY = 0;
                hh_kv.back().second.HH_AFTER_QTY = -needed_qty;

                // insert HOLDING && secondary index
                HOLDING::key k(TxnReq.trade_id);
                HOLDING::value v;
                v.H_CA_ID = acct_id;
                v.H_S_SYMB = symbol;
                v.H_DTS = trade_dts;
                v.H_PRICE = TxnReq.trade_price;
                v.H_QTY = -needed_qty;
                insert_holding(k, v, txn);
            }
        } else { // buy order
            if (hs_qty < 0) {
                // scan secondary index of HOLDING
                HOLDING_H_CA_ID::key k_lower, k_upper;
                k_lower.H1_H_CA_ID = acct_id;
                k_lower.H1_H_S_SYMB = symbol;
                k_lower.H1_H_T_ID_K = 0;//numeric_limits<TTrade>::min();
                k_upper.H1_H_CA_ID = acct_id;
                k_upper.H1_H_S_SYMB = symbol;
                k_upper.H1_H_T_ID_K = numeric_limits<TTrade>::max();
                scan_callback_normal cb(true);
				tbl_HOLDING_H_CA_ID(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
                std::set<TTrade> tid_set;

#ifdef EARLY_ABORT
                if (unlikely(cb.size() == 0)) {
                    db->abort_txn(txn);
                    is_user_initiate_abort = true;
                    return txn_result(false, 0);
                }
#endif
                // get the query output
                for (size_t cb_i = 0; cb_i < cb.size(); cb_i++) {
					HOLDING_H_CA_ID::value v_h_temp;
                    const HOLDING_H_CA_ID::value *v = Decode(*cb.values[cb_i].second, v_h_temp);
                    // XXX hacky: have to sort by H_T_ID
                    tid_set.insert(v->H1_H_T_ID_V);
                }

                // get HOLDING for w/d
                hold_tuple_list.reserve(tid_set.size());
				HOLDING::value v_h_temp[tid_set.size()];
				int index = 0;
                for (auto h_t_id : tid_set) {
                    const HOLDING::key k(h_t_id);
					if (unlikely(!(tbl_HOLDING(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v)))) {
#ifdef EARLY_ABORT
                        db->abort_txn(txn);
                        is_user_initiate_abort = true;
                        return txn_result(false, 0);
#else
                        continue;
#endif
                    }
                    const HOLDING::value *v = Decode(obj_v, v_h_temp[index++]);
                    hold_tuple_list.emplace_back(k, (HOLDING::value *)v);
                }

                std::sort(hold_tuple_list.begin(), hold_tuple_list.end());
                if (is_lifo) {
                    // reverse
                    for (auto it = hold_tuple_list.rbegin();
                            it != hold_tuple_list.rend(); it++) {
                        int32_t hold_qty = it->value->H_QTY;
                        double hold_price = it->value->H_PRICE;
                        if (hold_qty + needed_qty <= 0) {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_front();
                            hh_kv.front().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.front().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.front().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.front().second.HH_AFTER_QTY = hold_qty + needed_qty;

                            if (hold_qty + needed_qty == 0) {
                                // delete HOLDING
                                remove_holding((*it), txn);
                            } else {
								HOLDING::value v_h_new(*(it->value));
                                v_h_new.H_QTY += needed_qty;
								//add put here
								tbl_HOLDING(1/*TODO*/)->put(txn, Encode(str(), it->key), Encode(str(), v_h_new));
                            }
                            sell_value += needed_qty * hold_price;
                            buy_value += needed_qty * TxnReq.trade_price;
                            needed_qty = 0;
                            break;
                        } else {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_front();
                            hh_kv.front().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.front().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.front().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.front().second.HH_AFTER_QTY = 0;

                            // delete HOLDING
                            remove_holding((*it), txn);
                            hold_qty = -hold_qty;
                            sell_value += hold_qty * hold_price;
                            buy_value += hold_qty * TxnReq.trade_price;
                            needed_qty -= hold_qty;
                        }
                    }
                } else {
                    for (auto it = hold_tuple_list.begin();
                            it != hold_tuple_list.end(); it++) {
                        int32_t hold_qty = it->value->H_QTY;
                        double hold_price = it->value->H_PRICE;
                        if (hold_qty + needed_qty <= 0) {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_back();
                            hh_kv.back().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.back().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.back().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.back().second.HH_AFTER_QTY = hold_qty + needed_qty;

                            if (hold_qty + needed_qty == 0) {
                                // delete HOLDING
                                remove_holding((*it), txn);
                            } else {
                                HOLDING::value v_h_new(*(it->value));
                                v_h_new.H_QTY += needed_qty;
								//add put here
								tbl_HOLDING(1/*TODO*/)->put(txn, Encode(str(), it->key), Encode(str(), v_h_new));
                            }
                            sell_value += needed_qty * hold_price;
                            buy_value += needed_qty * TxnReq.trade_price;
                            needed_qty = 0;
                            break;
                        } else {
                            // insert HOLDING_HISTORY
                            hh_kv.emplace_back();
                            hh_kv.back().first.HH_H_T_ID = it->key.H_T_ID;
                            hh_kv.back().first.HH_T_ID = TxnReq.trade_id;
                            hh_kv.back().second.HH_BEFORE_QTY = hold_qty;
                            hh_kv.back().second.HH_AFTER_QTY = 0;

                            // delete HOLDING
                            remove_holding((*it), txn);
                            hold_qty = -hold_qty;
                            sell_value += hold_qty * hold_price;
                            buy_value += hold_qty * TxnReq.trade_price;
                            needed_qty -= hold_qty;
                        }
                    }
                }
            }
            if (needed_qty > 0) {
                // insert HOLDING_HISTORY
                hh_kv.emplace_back();
                hh_kv.back().first.HH_H_T_ID = TxnReq.trade_id;
                hh_kv.back().first.HH_T_ID = TxnReq.trade_id;
                hh_kv.back().second.HH_BEFORE_QTY = 0;
                hh_kv.back().second.HH_AFTER_QTY = -needed_qty;

                // insert HOLDING && secondary index
                HOLDING::key k(TxnReq.trade_id);
                HOLDING::value v;
                v.H_CA_ID = acct_id;
                v.H_S_SYMB = symbol;
                v.H_DTS = trade_dts;
                v.H_PRICE = TxnReq.trade_price;
                v.H_QTY = -needed_qty;
                insert_holding(k, v, txn);
            }
        }
        // if (!db->mul_ops_end(txn)) {
            // goto trade_result_holding;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_holding;
    // }

    // Raw Piece: P6 -- i [HOLDING_HISTORY]
    // declared as 'std::list<std::pair<HOLDING_HISTORY::key, HOLDING_HISTORY::value> > hh_kv'
    {
        //ic3_txn->start_raw_piece();
        for (auto &it : hh_kv) {
			tbl_HOLDING_HISTORY(1/*TODO*/)->insert(txn, Encode(str(), it.first), Encode(str(), it.second));
        }
        //ic3_txn->end_raw_piece();
    }
    // end Frame 2

    // Frame 3 (P7)
    // Raw Piece: P7 -- r [CUSTOMER_TAXRATE, TAX_RATE]
    float tax_amount = 0.0;
    {
        //ic3_txn->start_raw_piece();
        if (sell_value > buy_value && (tax_status == 1 || tax_status == 2)) {
            CUSTOMER_TAXRATE::key k_lower, k_upper;
            k_lower.CX_C_ID = cust_id;
            k_lower.CX_TX_ID.assign(min_str.c_str(), TPCE::sm_cTX_ID_len);
            k_upper.CX_C_ID = cust_id;
            k_upper.CX_TX_ID.assign(max_str.c_str(), TPCE::sm_cTX_ID_len);

            scan_callback_normal cb(false);
			tbl_CUSTOMER_TAXRATE(1/*TODO*/)->scan(txn, Encode(obj_key0, k_lower), &Encode(obj_key1, k_upper),
					cb, s_arena.get());
            float tax_rates = 0.0;
            for (size_t si = 0; si < cb.size(); si++) {
                CUSTOMER_TAXRATE::key k_temp;
                const CUSTOMER_TAXRATE::key *kk = Decode(*cb.values[si].first, k_temp);
                const TAX_RATE::key k(kk->CX_TX_ID);
                ALWAYS_ASSERT(tbl_TAX_RATE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
				TAX_RATE::value v_tr_temp;
                const TAX_RATE::value *v = Decode(obj_v, v_tr_temp);
                tax_rates += v->TX_RATE;
            }
            tax_amount += (sell_value - buy_value) * tax_rates;
        }
        //ic3_txn->end_raw_piece();
    }
    // end Frame 3

    inline_str_fixed_tpce<TPCE::sm_cEX_ID_len> exch_id;
    inline_str_fixed_tpce<TPCE::sm_cS_NAME_len> s_name;
    // Frame 4 (P8 - P10)
    // Raw Piece: P8 -- r [SECURITY]
    {
        //ic3_txn->start_raw_piece();
        const SECURITY::key k(symbol);
		ALWAYS_ASSERT(tbl_SECURITY(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        SECURITY::value v_s_temp;
        const SECURITY::value *v = Decode(obj_v, v_s_temp);
        s_name = v->S_NAME;
        exch_id = v->S_EX_ID;
        //ic3_txn->end_raw_piece();
    }

    char cust_tier;
    // Raw Piece: P9 -- r [CUSTOMER]
    {
        //ic3_txn->start_raw_piece();
        const CUSTOMER::key k(cust_id);
		ALWAYS_ASSERT(tbl_CUSTOMER(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k), obj_v));
        CUSTOMER::value v_c_temp;
        const CUSTOMER::value *v = Decode(obj_v, v_c_temp);
        cust_tier = v->C_TIER;
        //ic3_txn->end_raw_piece();
    }

    float comm_rate = 0.0;
    // Raw Piece: P10 -- r [COMMISSION_RATE]
    {
        //ic3_txn->start_raw_piece();
        COMMISSION_RATE::key k_lower, k_upper;
        k_lower.CR_C_TIER = cust_tier;
        k_lower.CR_TT_ID = type_id;
        k_lower.CR_EX_ID = exch_id;
        k_lower.CR_FROM_QTY = 0;//numeric_limits<int32_t>::min();
        k_upper.CR_C_TIER = cust_tier;
        k_upper.CR_TT_ID = type_id;
        k_upper.CR_EX_ID = exch_id;
        k_upper.CR_FROM_QTY = trade_qty;

        scan_limit_callback<1> cb(true);
		tbl_COMMISSION_RATE(1/*TODO*/)->rscan(txn, Encode(obj_key0, k_upper), &Encode(obj_key1, k_lower),
					cb, s_arena.get());
        ALWAYS_ASSERT(cb.size() == 1);
        COMMISSION_RATE::value v_cr_temp;
        const COMMISSION_RATE::value *v = Decode(*cb.values[0].second, v_cr_temp);
        ALWAYS_ASSERT(v->CR_TO_QTY >= trade_qty);
        comm_rate = v->CR_RATE;
        //ic3_txn->end_raw_piece();
    }

    // Frame 5 (P11 - P13)
    float comm_amount = (comm_rate / 100) * (trade_qty * TxnReq.trade_price);
    // Conflict Piece: P11 -- w [TRADE]
    // TRADE_RESULT: 3
trade_result_trade:
    // try 
    {
        // db->one_op_begin(txn);
        //ic3_txn->start_conflict_piece();
        const TRADE::key k(TxnReq.trade_id);
		ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->get(txn, Encode(obj_key0, k), obj_v));
        TRADE::value v_t_temp;
        const TRADE::value *v = Decode(obj_v, v_t_temp);
		TRADE::value v_t_new(*v);
        v_t_new.T_COMM = comm_amount;
        v_t_new.T_DTS = trade_dts;
        v_t_new.T_ST_ID = st_completed_id;
        v_t_new.T_TRADE_PRICE = TxnReq.trade_price;
        if (sell_value > buy_value && (tax_status == 1 || tax_status == 2))
            v_t_new.T_TAX = tax_amount;
		tbl_TRADE(1/*TODO*/)->put(txn, Encode(str(), k), Encode(str(), v_t_new));
        //ic3_txn->end_conflict_piece();
        // if (!db->one_op_end(txn)) {
            // goto trade_result_trade;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_trade;
    // }

    // Conflict Piece: P12 -- i [TRADE_HISTORY]
    // TRADE_RESULT: 4
trade_result_trade_history:
    // try 
    {
        // db->one_op_begin(txn);
        TRADE_HISTORY::key k;
        k.TH_T_ID = TxnReq.trade_id;
        k.TH_ST_ID = st_completed_id;
        TRADE_HISTORY::value v;
        v.TH_DTS = trade_dts;
		tbl_TRADE_HISTORY(1/*TODO*/)->insert(txn, Encode(str(), k), Encode(str(), v));
        // if (!db->one_op_end(txn)) {
            // goto trade_result_trade_history;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_trade_history;
    // }

    // Conflict Piece: P13 -- w [BROKER]
    // TRADE_RESULT: 5
trade_result_broker:
    // try 
    { //TODO commutative
        // db->one_op_begin(txn);
        BROKER::key k;
        k.B_ID = broker_id;
		ALWAYS_ASSERT(tbl_BROKER(1/*TODO*/)->get(txn, Encode(obj_key0, k), obj_v));
        BROKER::value v_b_temp;
        const BROKER::value *v = Decode(obj_v, v_b_temp);
		BROKER::value v_b_new(*v);
        v_b_new.B_COMM_TOTAL += comm_amount;
        v_b_new.B_NUM_TRADES++;
		tbl_BROKER(1/*TODO*/)->put(txn, Encode(str(), k), Encode(str(), v_b_new));
        // if (!db->one_op_end(txn)) {
            // goto trade_result_broker;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_broker;
    // }

    // end Frame 5

    // Frame 6 (P14 - P16)
    current.Add(2, 0); // add 2 days
    int_date_time due_date = current.GetTime();
    float se_amount = 0.0;
    if (type_is_sell)
        se_amount = (trade_qty * TxnReq.trade_price) - charge - comm_amount;
    else
        se_amount = -((trade_qty * TxnReq.trade_price) + charge + comm_amount);
    if (tax_status == 1)
        se_amount -= tax_amount;
    inline_str_fixed_tpce<TPCE::sm_cSE_CASH_TYPE_len> cash_type;
    if (trade_is_cash)
        cash_type.assign("Cash Account");
    else
        cash_type.assign("Margin");

    // Conflict Piece: P14 -- i [SETTLEMENT]
    // TRADE_RESULT: 6
trade_result_settlement:
    // try 
    {
        // db->one_op_begin(txn);
        SETTLEMENT::key k;
        k.SE_T_ID = TxnReq.trade_id;
        SETTLEMENT::value v;
        v.SE_CASH_TYPE = cash_type;
        v.SE_CASH_DUE_DATE = due_date;
        v.SE_AMT = se_amount;
		tbl_SETTLEMENT(1/*TODO*/)->insert(txn, Encode(str(), k), Encode(str(), v));
        // if (!db->one_op_end(txn)) {
            // goto trade_result_settlement;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_settlement;
    // }

    // Conflict Piece: P15 -- w [CUSTOMER_ACCOUNT]
    // TRADE_RESULT: 7
trade_result_customer_account:
    // try 
    {
        // db->one_op_begin(txn);
        CUSTOMER_ACCOUNT::key k;
        k.CA_ID = acct_id;
		ALWAYS_ASSERT(tbl_CUSTOMER_ACCOUNT(1/*TODO*/)->get(txn, Encode(obj_key0, k), obj_v));
		CUSTOMER_ACCOUNT::value v_ca_temp;
        const CUSTOMER_ACCOUNT::value *v = Decode(obj_v, v_ca_temp);
		CUSTOMER_ACCOUNT::value v_ca_new(*v);
        if (trade_is_cash)
            v_ca_new.CA_BAL += se_amount;
		tbl_CUSTOMER_ACCOUNT(1/*TODO*/)->put(txn, Encode(str(), k), Encode(str(), v_ca_new));
        // if (!db->one_op_end(txn)) {
            // goto trade_result_customer_account;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_customer_account;
    // }

    // Conflict Piece: P16 -- i [CASH_TRANSACTION]
    // TRADE_RESULT: 8
trade_result_cash_transaction:
    // try 
    {
        // db->one_op_begin(txn);
        if (trade_is_cash) {
            CASH_TRANSACTION::key k;
            k.CT_T_ID = TxnReq.trade_id;
            CASH_TRANSACTION::value v;
            v.CT_DTS = trade_dts;
            v.CT_AMT = se_amount;
            char ct_name[TPCE::sm_cCT_NAME_len];
            sprintf(ct_name, "%s %d shares of %s", type_name.data(), trade_qty, s_name.data());
            v.CT_NAME.assign(ct_name);
			tbl_CASH_TRANSACTION(1/*TODO*/)->insert(txn, Encode(str(), k), Encode(str(), v));
        }
        // if (!db->one_op_end(txn)) {
            // goto trade_result_cash_transaction;
        // }
    } 
    // catch (piece_abort_exception &ex) {
        // db->atomic_ops_abort(txn);
        // goto trade_result_cash_transaction;
    // }

	if (likely(db->commit_txn(txn))){
      	return txn_result(true, ret);
	}
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}

	return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_trade_status()
{
	TIdent          iCustomerId;
    eCustomerTier   iCustomerTier;
	TTradeStatusTxnInput TxnReq;

    //select customer id first
    GenerateNonUniformRandomCustomerId(iCustomerId, iCustomerTier, m_CustomerSelection);

    //select random account id
    GenerateRandomAccountId(iCustomerId, iCustomerTier, this, &TxnReq.acct_id, NULL);

    const uint64_t read_only_mask =
        g_disable_read_only_scans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
	void *txn = db->new_txn(
            txn_flags | read_only_mask, arena, txn_buf(), abstract_db::HINT_TPCE_TRADE_STATUS);
	scoped_str_arena s_arena(arena);
    try {
        ssize_t ret = 0;
        TTrade tid_list[50];
        int max_trades = 0;
        // [TRADE_T_CA_ID]
        {
            TRADE_T_CA_ID::key k_lower, k_upper;
            TRADE_T_CA_ID::value v_temp;
            k_lower.T1_T_CA_ID = TxnReq.acct_id;
            k_lower.T1_T_DTS = 0;//= std::numeric_limits<int_date_time>::min();
            k_lower.T1_T_ID_K = 0;//= std::numeric_limits<TTrade>::min();
            k_upper.T1_T_CA_ID = TxnReq.acct_id;
            k_upper.T1_T_DTS = std::numeric_limits<int_date_time>::max();
            k_upper.T1_T_ID_K = std::numeric_limits<TTrade>::max();

            static_limit_callback<50> c(s_arena.get(), true);
            tbl_TRADE_T_CA_ID(1)->rscan(txn, Encode(obj_key0, k_upper),
                    &Encode(obj_key1, k_lower), c, s_arena.get());
            for (auto &it : c.values) {
                const TRADE_T_CA_ID::value *v = Decode(*it.second, v_temp);
                tid_list[max_trades++] = v->T1_T_ID_V;
            }
        }

        inline_str_fixed_tpce<TPCE::sm_cST_ID_len> t_st_id_list[50];
        inline_str_fixed_tpce<TPCE::sm_cTT_ID_len> t_tt_id_list[50];
        inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len> symbol_list[50];
        inline_str_fixed_tpce<TPCE::sm_cEX_ID_len> s_ex_id_list[50];

        // [TRADE]
        for (int i = 0; i < max_trades; i++) {
            const TRADE::key k(tid_list[i]);
            TRADE::value v_temp;
            ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const TRADE::value *v = Decode(obj_v, v_temp);
            t_st_id_list[i] = v->T_ST_ID;
            t_tt_id_list[i] = v->T_TT_ID;
            symbol_list[i] = v->T_S_SYMB;
        }

        // [STATUS_TYPE]
        for (int i = 0; i < max_trades; i++) {
            const STATUS_TYPE::key k(t_st_id_list[i]);
            STATUS_TYPE::value v_temp;
            ALWAYS_ASSERT(tbl_STATUS_TYPE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const STATUS_TYPE::value *v = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v);
        }

        // [TRADE_TYPE]
        for (int i = 0; i < max_trades; i++) {
            const TRADE_TYPE::key k(t_tt_id_list[i]);
            TRADE_TYPE::value v_temp;
            ALWAYS_ASSERT(tbl_TRADE_TYPE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const TRADE_TYPE::value *v = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v);
        }

        // [SECURITY]
        for (int i = 0; i < max_trades; i++) {
            const SECURITY::key k(symbol_list[i]);
            SECURITY::value v_temp;
            ALWAYS_ASSERT(tbl_SECURITY(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const SECURITY::value *v = Decode(obj_v, v_temp);
            s_ex_id_list[i] = v->S_EX_ID;
        }

        // [EXCHANGE]
        for (int i = 0; i < max_trades; i++) {
            const EXCHANGE::key k(s_ex_id_list[i]);
            EXCHANGE::value v_temp;
            ALWAYS_ASSERT(tbl_EXCHANGE(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const EXCHANGE::value *v = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v);
        }

        TIdent c_id, broker_id;
        // [CUSTOMER_ACCOUNT]
        {
            const CUSTOMER_ACCOUNT::key k(TxnReq.acct_id);
            CUSTOMER_ACCOUNT::value v_temp;
            ALWAYS_ASSERT(tbl_CUSTOMER_ACCOUNT(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const CUSTOMER_ACCOUNT::value *v = Decode(obj_v, v_temp);
            c_id = v->CA_C_ID;
            broker_id = v->CA_B_ID;
        }

        // [CUSTOMER]
        {
            const CUSTOMER::key k(c_id);
            CUSTOMER::value v_temp;
            ALWAYS_ASSERT(tbl_CUSTOMER(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const CUSTOMER::value *v = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v);
        }

        // [BROKER]
        {
            const BROKER::key k(broker_id);
            BROKER::value v_temp;
            ALWAYS_ASSERT(tbl_BROKER(1/*TODO*/)->
                    get(txn, Encode(obj_key0, k), obj_v));
            const BROKER::value *v = Decode(obj_v, v_temp);
            ALWAYS_ASSERT(v);
        }

        if (likely(db->commit_txn(txn))){
            return txn_result(true, ret);
        }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}
    return txn_result(false, 0);
}

tpce_worker::txn_result
tpce_worker::txn_trade_update()
{
	INT32           iThreshold;
	TTradeUpdateTxnInput TxnReq;

    iThreshold = RndGenerateIntegerPercentage(r);

    if( iThreshold <= m_pDriverCETxnSettings->TU_settings.cur.do_frame1 )
    {
        // Frame 1
        TxnReq.frame_to_execute = 1;
        TxnReq.max_trades = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsFrame1;
        TxnReq.max_updates = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsToUpdateFrame1;

        // Generate list of unique trade id's
        int     ii, jj;
        bool    Accepted;
        TTrade  TID;

        for( ii = 0; ii < TxnReq.max_trades; ii++ )
        {
            Accepted = false;
            while( ! Accepted )
            {
                TID = GenerateNonUniformTradeID(TradeUpdateAValueForTradeIDGenFrame1,
                                                TradeUpdateSValueForTradeIDGenFrame1, this);
                jj = 0;
                while( jj < ii && TxnReq.trade_id[jj] != TID )
                {
                    jj++;
                }
                if( jj == ii )
                {
                    // We have a unique TID for this batch
                    TxnReq.trade_id[ii] = TID;
                    Accepted = true;
                }
            }
        }

        // Params not used by this frame /////////////////////////////////////////
        TxnReq.acct_id = 0;                                                     //
        TxnReq.max_acct_id = 0;                                                 //
        memset( TxnReq.symbol, 0, sizeof( TxnReq.symbol ));                     //
        memset( &TxnReq.start_trade_dts, 0, sizeof( TxnReq.start_trade_dts ));  //
        memset( &TxnReq.end_trade_dts, 0, sizeof( TxnReq.end_trade_dts ));      //
        //////////////////////////////////////////////////////////////////////////
    }
    else if( iThreshold <=  m_pDriverCETxnSettings->TU_settings.cur.do_frame1 +
                            m_pDriverCETxnSettings->TU_settings.cur.do_frame2 )
    {
        // Frame 2
        TxnReq.frame_to_execute = 2;
        TxnReq.max_trades = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsFrame2;
        TxnReq.max_updates = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsToUpdateFrame2;
        TxnReq.acct_id = GenerateRandomCustomerAccountId(this);

		INT64 m_iTradeUpdateFrame2MaxTimeInMilliSeconds = (INT64)((iHoursOfInitialTrades*SecondsPerHour)-
			(m_pDriverCETxnSettings->TU_settings.cur.BackOffFromEndTimeFrame2 )) * MsPerSecond;

        GenerateNonUniformTradeDTS( TxnReq.start_trade_dts,
                                    m_iTradeUpdateFrame2MaxTimeInMilliSeconds,
                                    TradeUpdateAValueForTimeGenFrame2,
                                    TradeUpdateSValueForTimeGenFrame2, this);

        // Set to the end of initial trades.
        m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);

        // Params not used by this frame /////////////////////////
        TxnReq.max_acct_id = 0;
        memset( TxnReq.symbol, 0, sizeof( TxnReq.symbol ));     //
        memset( TxnReq.trade_id, 0, sizeof( TxnReq.trade_id )); //
        //////////////////////////////////////////////////////////
    }
    else
    {
        // Frame 3
        TxnReq.frame_to_execute = 3;
        TxnReq.max_trades = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsFrame3;
        TxnReq.max_updates = m_pDriverCETxnSettings->TU_settings.cur.MaxRowsToUpdateFrame3;
		TIdent m_iActiveSecurityCount = dfm->SecurityFile().GetSize();
		INT64 m_iTradeUpdateFrame3MaxTimeInMilliSeconds = (INT64)((iHoursOfInitialTrades*SecondsPerHour)-
			(m_pDriverCETxnSettings->TU_settings.cur.BackOffFromEndTimeFrame3))*MsPerSecond;

        dfm->SecurityFile().CreateSymbol(NURnd(r, 0, m_iActiveSecurityCount-1,
                                                    TradeUpdateAValueForSymbolFrame3,
                                                    TradeUpdateSValueForSymbolFrame3 ),
                                    TxnReq.symbol,
                                    static_cast<int>(sizeof( TxnReq.symbol )));

        GenerateNonUniformTradeDTS( TxnReq.start_trade_dts,
                                    m_iTradeUpdateFrame3MaxTimeInMilliSeconds,
                                    TradeUpdateAValueForTimeGenFrame3,
                                    TradeUpdateSValueForTimeGenFrame3, this);

        // Set to the end of initial trades.
        m_EndTime.GetTimeStamp(&TxnReq.end_trade_dts);

        TxnReq.max_acct_id = GetEndingCA_ID( customer_selection_count ) ;

        // Params not used by this frame /////////////////////////
        TxnReq.acct_id = 0;                                     //
        memset( TxnReq.trade_id, 0, sizeof( TxnReq.trade_id )); //
        //////////////////////////////////////////////////////////
    }
	ssize_t ret = 0;
    void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_TPCE_TRADE_UPDATE);
    // db->init_txn(txn, cgraph, trade_update_type);
	scoped_str_arena s_arena(arena);

	try {
    UNUSED int frame_executed = 0;
	if (TxnReq.frame_to_execute == 1) {
		int num_updated = 0;

        std::vector<string> t_tt_id_list, t_exec_name_list;
        //UNUSED std::vector<float> t_bid_price_list, t_trade_price_list;
        std::vector<bool> t_is_cash_list;

        t_tt_id_list.resize(TxnReq.max_trades);
        t_exec_name_list.resize(TxnReq.max_trades);
        //t_bid_price_list.resize(TxnReq.max_trades);
        //t_trade_price_list.resize(TxnReq.max_trades);
        t_is_cash_list.resize(TxnReq.max_trades);

        // Conflict P1: [TRADE]
        // TRADE_UPDATE: 1
trade_update_trade_f1:
        num_updated = 0;
        // try 
        {
            // db->mul_ops_begin(txn);
            for (int i = 0; i < TxnReq.max_trades; i++) {
                if (num_updated < TxnReq.max_updates) {
                    const TRADE::key k_t(TxnReq.trade_id[i]);
                    ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->get(txn, Encode(obj_key0, k_t), obj_v));
                    TRADE::value v_t_temp;
                    const TRADE::value *v_t = Decode(obj_v, v_t_temp);
                    //checker::SanityCheckDistrict(&k_t, v_t);
                    t_exec_name_list[i] = v_t->T_EXEC_NAME.str();
                    size_t pos = t_exec_name_list[i].find(" X ");
                    if (pos != std::string::npos) {
                        t_exec_name_list[i].erase(pos+1,2);
                    } else {
                        pos = t_exec_name_list[i].find(" ");
                        t_exec_name_list[i].insert(pos+1, "X ");
                    }
                    TRADE::value v_t_new(*v_t);
                    v_t_new.T_EXEC_NAME.assign(t_exec_name_list[i].c_str());
                    t_tt_id_list[i].assign(v_t->T_TT_ID.data(), v_t->T_TT_ID.size());
                    //t_bid_price_list[i] = v_t->T_BID_PRICE;
                    //t_trade_price_list[i] = v_t->T_TRADE_PRICE;
                    t_is_cash_list[i] = char2bool(v_t->T_IS_CASH);
                    num_updated++;
                    tbl_TRADE(1/*TODO*/)->put(txn, Encode(str(), k_t), Encode(str(), v_t_new));
                } else {
                    const TRADE::key k_t(TxnReq.trade_id[i]);
                    ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k_t), obj_v));
                    TRADE::value v_t_temp;
                    const TRADE::value *v_t = Decode(obj_v, v_t_temp);
                    t_tt_id_list[i].assign(v_t->T_TT_ID.data(), v_t->T_TT_ID.size());
                    t_exec_name_list[i].assign(v_t->T_EXEC_NAME.data(), v_t->T_EXEC_NAME.size());
                    //t_bid_price_list[i] = v_t->T_BID_PRICE;
                    //t_trade_price_list[i] = v_t->T_TRADE_PRICE;
                    t_is_cash_list[i] = char2bool(v_t->T_IS_CASH);
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_trade_f1;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_trade_f1;
        // }

        // Conflict P2: [TRADE_HISTORY]
        // TRADE_UPDATE: 2
trade_update_trade_history_f1:
        // try 
        {
            // db->mul_ops_begin(txn);
            //UNUSED int_date_time th_dts[3];
            //UNUSED string th_st_id[3];
            //UNUSED int th_len = 0;

            for (int i = 0; i < TxnReq.max_trades; i++) {
                const char min_char = 0, max_char = 255;
                scan_limit_callback<3> c(false); // never more than 15 order_lines per order
                TRADE_HISTORY::key k_th_0;
                k_th_0.TH_T_ID = TxnReq.trade_id[i];
                NDB_MEMSET((char*)k_th_0.TH_ST_ID.data(), min_char, TPCE::sm_cST_ID_len);
                TRADE_HISTORY::key k_th_1;
                k_th_1.TH_T_ID = TxnReq.trade_id[i];
                NDB_MEMSET((char*)k_th_1.TH_ST_ID.data(), max_char, TPCE::sm_cST_ID_len);

                tbl_TRADE_HISTORY(1/*TODO*/)->scan(txn, Encode(obj_key0, k_th_0),
                        &Encode(obj_key1, k_th_1), c, s_arena.get());
                for (size_t j = 0; j < c.size(); j++) {
                    TRADE_HISTORY::key k_th_temp;
                    const TRADE_HISTORY::key *k_th_t = Decode(*c.values[j].first, k_th_temp);
                    //th_st_id[th_len].assign(k_th_t->TH_ST_ID.data(), k_th_t->TH_ST_ID.size());
                    ALWAYS_ASSERT(k_th_t);
                    TRADE_HISTORY::value v_th_temp;
                    const TRADE_HISTORY::value* v_th = Decode(*c.values[j].second, v_th_temp);
                    //th_dts[th_len] = v_th->TH_DTS;
                    ALWAYS_ASSERT(v_th);
                    //th_len++;
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_trade_history_f1;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_trade_history_f1;
        // }

        for (int i = 0; i < TxnReq.max_trades; i++) {
            UNUSED bool tt_is_mrkt;
            const TRADE_TYPE::key k_tt(t_tt_id_list[i].c_str());
            ALWAYS_ASSERT(tbl_TRADE_TYPE(1/*TODO*/)->get_for_read(txn,
                        Encode(obj_key0, k_tt), obj_v));
            TRADE_TYPE::value v_tt_temp;
            const TRADE_TYPE::value *v_tt = Decode(obj_v, v_tt_temp);
            tt_is_mrkt = char2bool(v_tt->TT_IS_MRKT);
        }

        // Conflict P2: [SETTLEMENT]
        // TRADE_UPDATE: 3
trade_update_settlement_f1:
        // try 
        {
            // db->mul_ops_begin(txn);
			for (int i = 0; i < TxnReq.max_trades; i++) {
				//UNUSED float se_amt = 0;
				//UNUSED int_date_time se_cash_due_date = 0;
				//UNUSED string se_cash_type;
				const SETTLEMENT::key k_se(TxnReq.trade_id[i]);
				ALWAYS_ASSERT(tbl_SETTLEMENT(1/*TODO*/)->get_for_read(txn,
                            Encode(obj_key0, k_se), obj_v));
				SETTLEMENT::value v_se_temp;
				const SETTLEMENT::value *v_se = Decode(obj_v, v_se_temp);
				//se_cash_type.assign(v_se->SE_CASH_TYPE.data(), v_se->SE_CASH_TYPE.size());
                ALWAYS_ASSERT(v_se);
			}
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_settlement_f1;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_settlement_f1;
        // }

        // Conflict P2: [CASH_TRANSACTION]
        // TRADE_UPDATE: 4
trade_update_cash_transaction_f1:
        // try 
        {
            // db->mul_ops_begin(txn);
			for (int i = 0; i < TxnReq.max_trades; i++) {
				//UNUSED string ct_name;
				//UNUSED float ct_amt = 0;
				//UNUSED int_date_time ct_dts = 0;

				if (t_is_cash_list[i]) {
					const CASH_TRANSACTION::key k_ct(TxnReq.trade_id[i]);
					ALWAYS_ASSERT(tbl_CASH_TRANSACTION(1/*TODO*/)->get_for_read(txn,
                                Encode(obj_key0, k_ct), obj_v));
					CASH_TRANSACTION::value v_ct_temp;
					const CASH_TRANSACTION::value *v_ct = Decode(obj_v, v_ct_temp);
					//ct_name.assign(v_ct->CT_NAME.data(), v_ct->CT_NAME.size());
                    ALWAYS_ASSERT(v_ct);
				}
			}
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_cash_transaction_f1;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_cash_transaction_f1;
        // }
		frame_executed = 1;
	} else if (TxnReq.frame_to_execute == 2) {
        std::vector<TTrade> trade_id;
        //std::vector<string> t_tt_id_list, t_exec_name_list;
        //UNUSED std::vector<float> t_bid_price_list, t_trade_price_list;
        std::vector<bool> t_is_cash_list;

        // Conflict Piece: P1 [TRADE]
        // TRADE_UPDATE: 1
trade_update_trade_f2:
        trade_id.clear();
        t_is_cash_list.clear();
        // try 
        {
            // db->mul_ops_begin(txn);
            scan_limit_callback<30> c(false);

            const TRADE_T_CA_ID::key k_tca_0(TxnReq.acct_id,
                    convertTime(TxnReq.start_trade_dts), min_int64);
            const TRADE_T_CA_ID::key k_tca_1(TxnReq.acct_id,
                    convertTime(TxnReq.end_trade_dts), max_int64);
            tbl_TRADE_T_CA_ID(1/*TODO*/)->scan(txn, Encode(obj_key0, k_tca_0),
                    &Encode(obj_key1, k_tca_1), c, s_arena.get());

            for (size_t i = 0; i < c.size(); i++){
                //UNUSED float t_bid_price, t_trade_price;
                {
                    TRADE_T_CA_ID::key k_tca_temp;
                    const TRADE_T_CA_ID::key *k_tca = Decode(*c.values[i].first, k_tca_temp);
                    trade_id.push_back(k_tca->T1_T_ID_K);
                    const TRADE::key k_t(k_tca->T1_T_ID_K);
                    ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->get_for_read(txn, Encode(obj_key0, k_t), obj_v));
                    TRADE::value v_t_temp;
                    const TRADE::value *v_t = Decode(obj_v, v_t_temp);
                    //t_exec_name_list[i].assign(v_t->T_EXEC_NAME.data(),
                    //v_t->T_EXEC_NAME.size());
                    //t_bid_price = v_t->T_BID_PRICE;
                    //t_trade_price = v_t->T_TRADE_PRICE;
                    t_is_cash_list.push_back(char2bool(v_t->T_IS_CASH));
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_trade_f2;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_trade_f2;
        // }

        // Conflict Piece: P2 [TRADE_HISTORY]
        // TRADE_UPDATE: 2
trade_update_trade_history_f2:
        // try 
        {
            // db->mul_ops_begin(txn);
            for (uint32_t i = 0; i < trade_id.size(); i++) {
                //UNUSED int_date_time th_dts[3];
                //UNUSED string th_st_id[3];
                //UNUSED int th_len = 0;

                char min_char = 0, max_char = 255;
                scan_limit_callback<3> c(false);
                TRADE_HISTORY::key k_th_0;
                k_th_0.TH_T_ID = trade_id[i];
                NDB_MEMSET((char*)k_th_0.TH_ST_ID.data(), min_char, TPCE::sm_cST_ID_len);
                TRADE_HISTORY::key k_th_1;
                k_th_1.TH_T_ID = trade_id[i];
                NDB_MEMSET((char*)k_th_1.TH_ST_ID.data(), max_char, TPCE::sm_cST_ID_len);
                tbl_TRADE_HISTORY(1/*TODO*/)->scan(txn, Encode(obj_key0, k_th_0),
                        &Encode(obj_key1, k_th_1), c, s_arena.get());
                for (size_t j = 0; j < c.size(); j++) {
                    TRADE_HISTORY::key k_th_temp;
                    const TRADE_HISTORY::key *k_th_t = Decode(*c.values[j].first, k_th_temp);
                    //th_st_id[th_len].assign(k_th_t->TH_ST_ID.data(), k_th_t->TH_ST_ID.size());
                    ALWAYS_ASSERT(k_th_t);
                    TRADE_HISTORY::value v_th_temp;
                    const TRADE_HISTORY::value* v_th = Decode(*c.values[j].second, v_th_temp);
                    //th_dts[th_len] = v_th->TH_DTS;
                    ALWAYS_ASSERT(v_th);
                    //th_len++;
                }
                //ic3_txn->end_conflict_piece();
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_trade_history_f2;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_trade_history_f2;
        // }


        // Conflict Piece: P3 [SETTLEMENT]
        // TRADE_UPDATE: 3
trade_update_settlement_f2:
        // try 
        {
            // db->mul_ops_begin(txn);
            int num_updated = 0;
            for (uint32_t i = 0; i < trade_id.size(); i++) {
                //UNUSED float se_amt = 0;
                //UNUSED int_date_time se_cash_due_date = 0;
                //UNUSED string se_cash_type;
                const SETTLEMENT::key k_se(trade_id[i]);
                if (num_updated < TxnReq.max_updates) {
                    ALWAYS_ASSERT(tbl_SETTLEMENT(1/*TODO*/)->get(txn,
                                Encode(obj_key0, k_se), obj_v));
                    SETTLEMENT::value v_se_temp;
                    const SETTLEMENT::value *v_se = Decode(obj_v, v_se_temp);
                    SETTLEMENT::value v_se_new(*v_se);
                    if (t_is_cash_list[i]) {
                        if (strcmp(v_se->SE_CASH_TYPE.data(), "Cash Account") == 0)
                            v_se_new.SE_CASH_TYPE.assign("Cash");
                        else
                            v_se_new.SE_CASH_TYPE.assign("Cash Account");
                    } else {
                        if (strcmp(v_se->SE_CASH_TYPE.data(), "Margin Account") == 0)
                            v_se_new.SE_CASH_TYPE.assign("Margin");
                        else
                            v_se_new.SE_CASH_TYPE.assign("Margin Account");
                    }
                    num_updated++;
                    //se_amt = v_se->SE_AMT;
                    //se_cash_due_date = v_se->SE_CASH_DUE_DATE;
                    //se_cash_type.assign(v_se->SE_CASH_TYPE.data(), v_se->SE_CASH_TYPE.size());
                    tbl_SETTLEMENT(1/*TODO*/)->put(txn, Encode(str(), k_se),
                            Encode(obj_v, v_se_new));
                } else {
                    ALWAYS_ASSERT(tbl_SETTLEMENT(1/*TODO*/)->get_for_read(txn,
                                Encode(obj_key0, k_se), obj_v));
                    SETTLEMENT::value v_se_temp;
                    const SETTLEMENT::value *v_se = Decode(obj_v, v_se_temp);
                    ALWAYS_ASSERT(v_se);
                    //se_amt = v_se->SE_AMT;
                    //se_cash_due_date = v_se->SE_CASH_DUE_DATE;
                    //se_cash_type.assign(v_se->SE_CASH_TYPE.data(), v_se->SE_CASH_TYPE.size());
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_settlement_f2;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_settlement_f2;
        // }

        // Conflict Piece: P4 [CASH_TRANSACTION]
        // TRADE_UPDATE: 4
trade_update_cash_transaction_f2:
        // try 
        {
            // db->mul_ops_begin(txn);
            for (uint32_t i = 0; i < trade_id.size(); i++) {
                //UNUSED string ct_name;
                //UNUSED float ct_amt = 0;
                //UNUSED int_date_time ct_dts = 0;

                if (t_is_cash_list[i]) {
                    const CASH_TRANSACTION::key k_ct(trade_id[i]);;
                    ALWAYS_ASSERT(tbl_CASH_TRANSACTION(1/*TODO*/)->get_for_read(txn,
                                Encode(obj_key0, k_ct), obj_v));
                    CASH_TRANSACTION::value v_ct_temp;
                    const CASH_TRANSACTION::value *v_ct = Decode(obj_v, v_ct_temp);
                    ALWAYS_ASSERT(v_ct);
                    //ct_name.assign(v_ct->CT_NAME.data(), v_ct->CT_NAME.size());
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_cash_transaction_f2;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_cash_transaction_f2;
        // }
		frame_executed = 2;
    } else { //should be 3
        INVARIANT(TxnReq.frame_to_execute == 3);
        std::vector<TTrade> trade_id;
        std::vector<std::string> t_tt_id_list;
        std::vector<bool> t_is_cash_list;

        // Conflict Piece: P1 [TRADE]
        // TRADE_UPDATE: 1
trade_update_trade_f3:
        trade_id.clear();
        t_tt_id_list.clear();
        t_is_cash_list.clear();
        // try 
        {
            // db->mul_ops_begin(txn);
            scan_limit_callback<30> c(false);

            const TRADE_T_S_SYMB::key k_ts_0(TxnReq.symbol,
                    convertTime(TxnReq.start_trade_dts), min_int64);
            const TRADE_T_S_SYMB::key k_ts_1(TxnReq.symbol,
                    convertTime(TxnReq.end_trade_dts), max_int64);
            tbl_TRADE_T_S_SYMB(1/*TODO*/)->scan(txn, Encode(obj_key0, k_ts_0),
                    &Encode(obj_key1, k_ts_1), c, s_arena.get());
            for(size_t i = 0; i < c.size(); i++){
                //UNUSED TIdent t_ca_id;
                //UNUSED int32_t t_qty;
                //UNUSED float t_trade_price;
                //string t_exec_name;

                {
                    TRADE_T_S_SYMB::key k_tca_temp;
                    const TRADE_T_S_SYMB::key *k_tca = Decode(*c.values[i].first, k_tca_temp);
                    trade_id.push_back(k_tca->T2_T_ID_K);
                    const TRADE::key k_t(k_tca->T2_T_ID_K);
                    ALWAYS_ASSERT(tbl_TRADE(1/*TODO*/)->get_for_read(txn,
                                Encode(obj_key0, k_t), obj_v));
                    TRADE::value v_t_temp;
                    const TRADE::value *v_t = Decode(obj_v, v_t_temp);
                    //t_ca_id = v_t->T_CA_ID;
                    //t_exec_name.assign(v_t->T_EXEC_NAME.data(), v_t->T_EXEC_NAME.size());
                    //t_trade_price = v_t->T_TRADE_PRICE;
                    t_is_cash_list.push_back(char2bool(v_t->T_IS_CASH));
                    //t_qty = v_t->T_QTY;
                    std::string t_tt_id;
                    t_tt_id.assign(v_t->T_TT_ID.data(), v_t->T_TT_ID.size());
                    t_tt_id_list.push_back(t_tt_id);
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_trade_f3;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_trade_f3;
        // }

        // Raw Piece: P2 [TRADE_TYPE]
        {
            for (uint32_t i = 0; i < trade_id.size(); i++) {
                //UNUSED string tt_name;
                const TRADE_TYPE::key k_tt(t_tt_id_list[i].c_str());
                ALWAYS_ASSERT(tbl_TRADE_TYPE(1/*TODO*/)->get_for_read(txn,
                            Encode(obj_key0, k_tt), obj_v));
                TRADE_TYPE::value v_tt_temp;
                const TRADE_TYPE::value *v_tt = Decode(obj_v, v_tt_temp);
                ALWAYS_ASSERT(v_tt);
                //tt_name.assign(v_tt->TT_NAME.data(), v_tt->TT_NAME.size());
            }
        }

        // Raw Piece: P3 [SECURITY]
        {
            //UNUSED string s_name;
            const SECURITY::key k_s(TxnReq.symbol);
            ALWAYS_ASSERT(tbl_SECURITY(1/*TODO*/)->get_for_read(txn,
                        Encode(obj_key0, k_s), obj_v));
            SECURITY::value v_s_temp;
            const SECURITY::value *v_s = Decode(obj_v, v_s_temp);
            ALWAYS_ASSERT(v_s);
            //s_name.assign(v_s->S_NAME.data(), v_s->S_NAME.size());
        }

        // Conflict Piece: P4 [TRADE_HISTORY]
        // TRADE_UPDATE: 2
trade_update_trade_history_f3:
        // try 
        {
            // db->mul_ops_begin(txn);
            for (uint32_t i = 0; i < trade_id.size(); i++) {
                //UNUSED int_date_time th_dts[3];
                //UNUSED string th_st_id[3];
                //UNUSED int th_len = 0;

                char min_char = 0, max_char = 255;
                scan_limit_callback<3> c(false); // never more than 15 order_lines per order
                TRADE_HISTORY::key k_th_0;
                k_th_0.TH_T_ID = trade_id[i];
                NDB_MEMSET((char*)k_th_0.TH_ST_ID.data(), min_char, TPCE::sm_cST_ID_len);
                TRADE_HISTORY::key k_th_1;
                k_th_1.TH_T_ID = trade_id[i];
                NDB_MEMSET((char*)k_th_1.TH_ST_ID.data(), max_char, TPCE::sm_cST_ID_len);

                tbl_TRADE_HISTORY(1/*TODO*/)->scan(txn, Encode(obj_key0, k_th_0),
                        &Encode(obj_key1, k_th_1), c, s_arena.get());
                for (size_t j = 0; j < c.size(); j++) {
                    TRADE_HISTORY::key k_th_temp;
                    const TRADE_HISTORY::key *k_th_t = Decode(*c.values[j].first, k_th_temp);
                    //th_st_id[th_len].assign(k_th_t->TH_ST_ID.data(), k_th_t->TH_ST_ID.size());
                    ALWAYS_ASSERT(k_th_t);
                    TRADE_HISTORY::value v_th_temp;
                    const TRADE_HISTORY::value* v_th = Decode(*c.values[j].second, v_th_temp);
                    //th_dts[th_len] = v_th->TH_DTS;
                    ALWAYS_ASSERT(v_th);
                    //th_len++;
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_trade_history_f3;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_trade_history_f3;
        // }

        // Conflict Piece: P5 [SETTLEMENT]
        // TRADE_UPDATE: 3
trade_update_settlement_f3:
        // try 
        {
            // db->mul_ops_begin(txn);
            for (uint32_t i = 0; i < trade_id.size(); i++) {
                //UNUSED float se_amt = 0;
                //UNUSED int_date_time se_cash_due_date = 0;
                //UNUSED string se_cash_type;
                const SETTLEMENT::key k_se(trade_id[i]);
                ALWAYS_ASSERT(tbl_SETTLEMENT(1/*TODO*/)->get_for_read(txn,
                            Encode(obj_key0, k_se), obj_v));
                SETTLEMENT::value v_se_temp;
                const SETTLEMENT::value *v_se = Decode(obj_v, v_se_temp);
                ALWAYS_ASSERT(v_se);
                //se_cash_type.assign(v_se->SE_CASH_TYPE.data(), v_se->SE_CASH_TYPE.size());
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_settlement_f3;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_settlement_f3;
        // }

        int num_updated;
        // Conflict Piece: P6 [CASH_TRANSACTION]
        // TRADE_UPDATE: 4
trade_update_cash_transaction_f3:
        num_updated = 0;
        // try 
        {
            // db->mul_ops_begin(txn);
            for (uint32_t i = 0; i < trade_id.size(); i++) {
                //UNUSED string ct_name;
                //UNUSED float ct_amt = 0;
                //UNUSED int_date_time ct_dts = 0;

                if (t_is_cash_list[i]) {
                    const CASH_TRANSACTION::key k_ct(trade_id[i]);
                    if(num_updated < TxnReq.max_updates){
                        ALWAYS_ASSERT(tbl_CASH_TRANSACTION(1/*TODO*/)->get(txn,
                                    Encode(obj_key0, k_ct), obj_v));
                        CASH_TRANSACTION::value v_ct_temp;
                        const CASH_TRANSACTION::value *v_ct = Decode(obj_v, v_ct_temp);

                        char *pch = strstr((char*)v_ct->CT_NAME.data()," shares of ");
                        if (pch != NULL) {
                            pch[1] = 'S';
                        } else {
                            pch = strstr((char*)v_ct->CT_NAME.data()," Shares of ");
                            pch[1] = 's';
                        }
                        //ct_name.assign(v_ct->CT_NAME.data(), v_ct->CT_NAME.size());
                        //ct_amt = v_ct->CT_AMT;
                        //ct_dts = v_ct->CT_DTS;
                        num_updated++;
                        CASH_TRANSACTION::value v_ct_new(*v_ct);
                        tbl_CASH_TRANSACTION(1/*TODO*/)->put(txn,
                                Encode(str(), k_ct), Encode(obj_v, v_ct_new));
                    } else {
                        ALWAYS_ASSERT(tbl_CASH_TRANSACTION(1/*TODO*/)->get_for_read(txn,
                                    Encode(obj_key0, k_ct), obj_v));
                        CASH_TRANSACTION::value v_ct_temp;
                        const CASH_TRANSACTION::value *v_ct = Decode(obj_v, v_ct_temp);
                        ALWAYS_ASSERT(v_ct);
                        //ct_name.assign(v_ct->CT_NAME.data(), v_ct->CT_NAME.size());
                        //ct_amt = v_ct->CT_AMT;
                        //ct_dts = v_ct->CT_DTS;
                    }
                }
            }
            // if (!db->mul_ops_end(txn)) {
                // goto trade_update_cash_transaction_f3;
            // }
        } 
        // catch (piece_abort_exception &ex) {
            // db->atomic_ops_abort(txn);
            // goto trade_update_cash_transaction_f3;
        // }
        frame_executed = 3;
    }

    if (likely(db->commit_txn(txn))){
        return txn_result(true, ret);
    }
	} catch (abstract_db::abstract_abort_exception &ex) {
    	db->abort_txn(txn);
  	}
	return txn_result(false, 0);
}

class tpce_bench_runner : public bench_runner {
private:

  static bool
  IsTableReadOnly(const char *name)
  {
      //TODO
    //return strcmp("item", name) == 0;
    return false;
  }

  static bool
  IsTableAppendOnly(const char *name)
  {
      //TODO
      return false;
  }

  static vector<abstract_ordered_index *>
  OpenTablesForTablespace(abstract_db *db, const char *name, size_t expected_size)
  {
    const bool is_read_only = IsTableReadOnly(name);
    const bool is_append_only = IsTableAppendOnly(name);
    const string s_name(name);
    vector<abstract_ordered_index *> ret(get_num_numa_nodes());
    if (g_enable_separate_tree_per_partition && !is_read_only) {
        for (size_t i = 0; i < get_num_numa_nodes(); i++) {
          ret[i] = db->open_index(s_name + "_" + to_string(i), expected_size, is_append_only);
        }
    } else {
      ALWAYS_ASSERT(is_read_only); // useless to us, if gets here, must be configured incorrectly
      abstract_ordered_index *idx = db->open_index(s_name, expected_size, is_append_only);
      for (size_t i = 0; i < get_num_numa_nodes(); i++)
        ret[i] = idx;
    }
    return ret;
  }

public:
  tpce_bench_runner(abstract_db *db)
    : bench_runner(db)
  {

#define OPEN_TABLESPACE_X(x) \
    partitions[#x] = OpenTablesForTablespace(db, #x, sizeof(x));

    TPCE_TABLE_LIST(OPEN_TABLESPACE_X);
    TPCE_SECONDARY_INDEX_TABLE_LIST(OPEN_TABLESPACE_X);

#undef OPEN_TABLESPACE_X

    for (auto &t : partitions) {
      auto v = unique_filter(t.second);
      for (size_t i = 0; i < v.size(); i++)
        open_tables[t.first + "_" + to_string(i)] = v[i];
    }

    if (g_enable_partition_locks) {
      static_assert(sizeof(aligned_padded_elem<spinlock>) == CACHELINE_SIZE, "xx");
      void * const px = memalign(CACHELINE_SIZE, sizeof(aligned_padded_elem<spinlock>) * nthreads);
      ALWAYS_ASSERT(px);
      ALWAYS_ASSERT(reinterpret_cast<uintptr_t>(px) % CACHELINE_SIZE == 0);
      g_partition_locks = reinterpret_cast<aligned_padded_elem<spinlock> *>(px);
      for (size_t i = 0; i < nthreads; i++) {
        new (&g_partition_locks[i]) aligned_padded_elem<spinlock>();
        ALWAYS_ASSERT(!g_partition_locks[i].elem.is_locked());
      }
    }
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
      //TODO
    vector<bench_loader *> ret;

    ret.push_back(new tpce_loader(32423, db, open_tables, partitions));

    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    const unsigned alignment = coreid::num_cpus_online();
    // printf("num cpus online: %u\n", alignment);
    const int blockstart = coreid::allocate_contiguous_aligned_block(nthreads, alignment);
    ALWAYS_ASSERT(blockstart >= 0);
    ALWAYS_ASSERT((blockstart % alignment) == 0);
    fast_random r(23984543);
    vector<bench_worker *> ret;
      for (size_t i = 0; i < nthreads; i++)
        ret.push_back(
          new tpce_worker(
            blockstart + i,
            r.next(), db, open_tables, partitions,
            &barrier_a, &barrier_b));
    return ret;
  }
/*
  virtual std::vector<bench_checker*> make_checkers()
  {
    vector<bench_checker *> ret;
//TODO: TPCE Consistency Check
#if CHECK
    //TODO checker
#endif

    return ret;
  }
*/
private:
  map<string, vector<abstract_ordered_index *>> partitions;
};

#undef TPCE_TXN_LIST
#undef TPCE_TXN_ID_ENUM
#undef TPCE_RUNNABLE_CLASS_NAME
//#undef TPCE_NEW_TXN_FN_NAME
//#undef TPCE_FREE_TXN_FN_NAME
//#undef TPCE_FREE_LIST_NAME
#undef TBL_LOADER_CLASS_NAME
#undef TBL2ROW

void
tpce_do_test(abstract_db *db, int argc, char **argv)
{
  // parse options
  optind = 1;
  bool did_spec_remote_pct = false;
  while (1) {
    static struct option long_options[] =
    {
      {"disable-cross-partition-transactions" , no_argument       , &g_disable_xpartition_txn             , 1}   ,
      {"disable-read-only-snapshots"          , no_argument       , &g_disable_read_only_scans            , 1}   ,
      {"enable-partition-locks"               , no_argument       , &g_enable_partition_locks             , 1}   ,
      {"enable-separate-tree-per-partition"   , no_argument       , &g_enable_separate_tree_per_partition , 1}   ,
      {"workload-mix"                         , required_argument , 0                                     , 'w'} ,
      {"max-security-range"                   , required_argument , 0                                     , 's'} ,
      {"max-customer-range"                   , required_argument , 0                                     , 'c'} ,
      {"market-order-rate"                    , required_argument , 0                                     , 'm'} ,
      {"market-ignore-rate"                   , required_argument , 0                                     , 'i'} ,
      {"zipf-theta"                           , required_argument , 0                                     , 'a'} ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "i:m:w:c:s:t:a:", long_options, &option_index);
    if (c == -1)
      break;
    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 'w':
      {
        const vector<string> toks = split(optarg, ',');
        ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
        unsigned s = 0;
        for (size_t i = 0; i < toks.size(); i++) {
          unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
          ALWAYS_ASSERT(p >= 0 && p <= 100);
          s += p;
          g_txn_workload_mix[i] = p;
        }
        ALWAYS_ASSERT(s == 100);
      }
      break;
    case 'c':
      {
        char *ptr = NULL;
        uint64_t mcr = strtoul(optarg, &ptr, 10);
        ALWAYS_ASSERT(*ptr == '\0');
        customer_selection_count = (TIdent) mcr;
      }
      break;
    case 's':
      {
#ifndef NO_CONFLICT_ON_SYMBOL
        char *ptr = NULL;
        uint64_t msr = strtoul(optarg, &ptr, 10);
        ALWAYS_ASSERT(*ptr == '\0');
        max_security_range = msr;
#endif
      }
      break;
    case 'a':
      {
        char *ptr = NULL;
        double msr = strtod(optarg, &ptr);
        ALWAYS_ASSERT(*ptr == '\0');
        g_zipf_theta = msr;
      }
      break;
    case 'm':
      {
        char *ptr = NULL;
        uint64_t mor = strtoul(optarg, &ptr, 10);
        ALWAYS_ASSERT(*ptr == '\0');
        market_order_rate = mor;
        ALWAYS_ASSERT(market_order_rate >= 0 && market_order_rate <= 100);
      }
      break;
    case 'i':
      {
        char *ptr = NULL;
        uint64_t mor = strtoul(optarg, &ptr, 10);
        ALWAYS_ASSERT(*ptr == '\0');
        market_ignore_rate = mor;
        ALWAYS_ASSERT(market_ignore_rate >= 0 && market_ignore_rate <= 1000);
      }
      break;
    case '?':
      /* getopt_long already printed an error message. */
      exit(1);

    default:
      fprintf(stderr, "%c\n", c);
      abort();
    }
  }

  if (did_spec_remote_pct && g_disable_xpartition_txn) {
    cerr << "WARNING: --new-order-remote-item-pct given with --disable-cross-partition-transactions" << endl;
    cerr << "  --new-order-remote-item-pct will have no effect" << endl;
  }

  if (verbose) {
    cerr << "tpce settings:" << endl;
    cerr << "  cross_partition_transactions : " << !g_disable_xpartition_txn << endl;
    cerr << "  read_only_snapshots          : " << !g_disable_read_only_scans << endl;
    cerr << "  partition_locks              : " << g_enable_partition_locks << endl;
    cerr << "  separate_tree_per_partition  : " << g_enable_separate_tree_per_partition << endl;
    cerr << "  workload_mix                 : " <<
      format_list(g_txn_workload_mix,
                  g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix)) << endl;
  }

  // cgraph = init_tpce_cgraph();

  tpce_bench_runner r(db);
  r.run();
}
