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

#include "seats.h"
#include "../txn.h"
#include "../macros.h"
#include "../scopedperf.hh"
#include "../spinlock.h"


#include "bench.h"
#include "../txn_btree.h"
//#include "../txn_proto2_impl.h"

#undef INVARIANT
#define INVARIANT(expr) ((void)0)

using namespace std;
using namespace util;


enum bank_tx_types
{
  delete_reservation_type = 1,
  new_reservation_type,
  update_reservation_type,
  update_customer_type,
  end_type
};


int CONFLICT_WINDOW_1 = 100;

int CONFLICT_WINDOW_2 = 100;
std::vector<int64_t> conflict_list;
std::vector<int64_t> conflict_list_flight;

const size_t type_num = (end_type - delete_reservation_type);

  static inline conflict_graph*
init_seats_cgraph()
{
  conflict_graph* g = new conflict_graph(type_num);
  g->init_txn(delete_reservation_type, 4);
  g->init_txn(new_reservation_type,4);
  g->init_txn(update_customer_type,2);
  g->init_txn(update_reservation_type,1);

  g->set_conflict(delete_reservation_type, 1, new_reservation_type, 1);
  g->set_conflict(delete_reservation_type, 1, update_customer_type, 1);
  g->set_conflict(delete_reservation_type, 2, new_reservation_type, 2);
  g->set_conflict(delete_reservation_type, 3, new_reservation_type, 3);
  g->set_conflict(delete_reservation_type, 3, update_reservation_type, 1);
  g->set_conflict(delete_reservation_type, 4, new_reservation_type, 4);
  g->set_conflict(delete_reservation_type, 4, update_customer_type, 2);

  g->set_conflict(new_reservation_type, 1, update_customer_type, 1);
  g->set_conflict(new_reservation_type, 3, update_reservation_type, 1);
  g->set_conflict(new_reservation_type, 4, update_customer_type, 2);



  return g;
}

static conflict_graph* cgraph = NULL;

#define SEATS_TABLE_LIST(x) \
  x(country) \
x(airport) \
x(airline) \
x(flight) \
x(flight_time_idx) \
x(frequent_flyer) \
x(frequent_flyer_name_idx) \
x(seats_customer) \
x(seats_customer_name_idx) \
x(reservation) \
x(airport_distance) \
x(reservation_seat_idx)

//int INFO::txn_read_abort = 0, INFO::txn_write_abort = 0, INFO::txn_absent_abort = 0;


string COUNTRY_FILE = "./table.country.csv";
string AIRPORT_FILE= "./table.airport.csv";
string AIRLINE_FILE="./table.airline.csv";
string FLIGHTS_PER_AIRPORT_FILE="./histogram.flights_per_airport";
static unsigned g_txn_workload_mix[] = {10,5,5,65,10,5};//{10,10,35,20,10,15};//{20, 5, 5, 20, 45, 5}; //{15,5,5,15,55,5};//{10,4,4,10,68,4};//{20, 5, 5, 20, 45, 5};
//del reserv/findflights/findopenseats/new reserv/update c/update reserv


//skip load the file
string FLIGHTS_PER_TIME_FILE="./histogram.flights_per_time";
int TIMES_PER_DAY = 94;
int PAST_DAY = 1;
int FUTURE_DAY = 101;//51;//including today
int TOTAL_DAY = PAST_DAY + FUTURE_DAY;
int start_thread_id = 0;
//So time ranges from 0..93(the past day) to 94*51..93+94*51
//int MIN_DEPART_TIME = 0;
//int MAX_DEPART_TIME = 94*52 - 1;
//int ARRIVE_TIME = 20;  //should be calculated by the distance of two airports and fly speed

std::vector<country::value*> country_list;
std::vector<airport::value*> airport_list;
std::vector<airline::value*> airline_list;
struct to_airport_distance{
  int64_t to_ap_id;
  float distance;
};
std::vector<std::vector<to_airport_distance*> *> airport_distances;
std::vector<std::vector<int64_t>*> flights_per_airport;
std::vector<std::vector<flight::value> > flight_list;

int upcoming_rid = -1;
std::vector<std::vector<uint>*> customers_per_airport;
bool *customer_loaded;

class reservation_entry {
  public:
    reservation_entry(){}

    reservation_entry(int64_t flight_id_, int64_t customer_id_, int64_t r_id_, uint seat_num_)
      : flight_id(flight_id_), customer_id(customer_id_), r_id(r_id_), seat_num(seat_num_)
    {}

    int64_t flight_id;
    int64_t customer_id;
    int64_t r_id;
    uint seat_num;
};

std::vector<std::vector<reservation_entry*>*> all_inserted;
std::vector<std::vector<reservation_entry*>*> all_toinsert;


static inline int NumPartitions(){
  return (int)scale_factor;
}

static void loadCountry(){
  customer_loaded = new bool[NumPartitions()];
  for(int i = 0 ; i < NumPartitions(); i++){
    all_inserted.push_back(new std::vector<reservation_entry*>());
    all_toinsert.push_back(new std::vector<reservation_entry*>());
    flight_list.push_back(std::vector<flight::value>());
    customer_loaded[i] = false;
  }

  if(country_list.size() > 0)
    country_list.clear();

  ifstream infile(COUNTRY_FILE);
  string line = "";
  //skip first line of fields
  getline(infile, line);
  string name = "";
  string dummy = "";
  getline(infile, dummy, '\"');
  while (getline(infile, name, '"'))
  {
    country::value* temp = new country::value();
    temp->co_name.assign(name);

    getline(infile, dummy, ',');

    string code2 = "";
    getline(infile, code2, ',');
    code2.erase(std::remove(code2.begin(), code2.end(), '\"' ), code2.end());
    temp->co_code_2.assign(code2);

    string code3 = "";
    getline(infile, code3);
    code3.erase(std::remove(code3.begin(), code3.end(), '\"' ), code3.end());
    temp->co_code_3.assign(code3);

    country_list.push_back(temp);
    getline(infile, dummy, '\"');
  }
  infile.close();
  ALWAYS_ASSERT(country_list.size() > 0);
  printf("country loaded\n");
}

static int searchCountryCode(string code){
  if(country_list.size()==0){
    printf("No Country List Read\n");
    ALWAYS_ASSERT(false);
  }
  for(uint i = 0; i < country_list.size(); i++){
    country::value* temp = country_list[i];
    if(memcmp(temp->co_code_3.data(), code.c_str(), temp->co_code_3.size())==0){
      return i+1;
    }
  }
  return -1;
}

static void loadAirport(){
  if(airport_list.size() > 0)
    airport_list.clear();

  ifstream infile(AIRPORT_FILE);
  string line = "";
  getline(infile, line);
  string code = "";
  while (getline(infile, code, ','))
  {
    code.erase(std::remove(code.begin(), code.end(), '\"' ), code.end());
    airport::value* temp = new airport::value();
    temp->ap_code.assign(code);

    string dummy = "";
    getline(infile, dummy, '\"');

    string name = "";
    getline(infile, name, '\"');
    temp->ap_name.assign(name);
    getline(infile, dummy, ',');

    getline(infile, dummy, '\"');

    string city = "";
    getline(infile, city, '\"');
    temp->ap_city.assign(city);
    getline(infile, dummy, ',');

    string post = "";
    getline(infile, post, ',');
    temp->ap_postal_code.assign(post);

    string country_code = "";
    getline(infile, country_code, ',');
    country_code.erase(std::remove(country_code.begin(), country_code.end(), '\"' ), country_code.end());
    int i = searchCountryCode(country_code);
    if(i < 0){
      printf("country not found\n");
      ALWAYS_ASSERT(false);
    }
    temp->ap_co_id = i;

    string longitude = "";
    getline(infile, longitude, ',');
    temp->ap_longitude = atof(longitude.c_str());

    string latitude = "";
    getline(infile, latitude, ',');
    temp->ap_latitude = atof(latitude.c_str());

    string gmt = "";
    getline(infile, gmt, ',');
    temp->ap_gmt_offset = atof(gmt.c_str());

    string wac = "";
    getline(infile, wac);
    temp->ap_wac = atol(wac.c_str());

    airport_list.push_back(temp);
  }
  infile.close();
  ALWAYS_ASSERT(airport_list.size() > 0);
  printf("airport loaded\n");
}

static void loadAirline(){
  if(airline_list.size() > 0)
    airline_list.clear();

  ifstream infile(AIRLINE_FILE); //"Iata_Code","Icao_Code","Call_Sign","Name","Country_Code"
  string line = "";
  getline(infile, line);
  string iata_code = "";
  while(getline(infile, iata_code, ',')){
    iata_code.erase(std::remove(iata_code.begin(), iata_code.end(), '\"' ), iata_code.end());
    airline::value* temp = new airline::value();
    temp->al_iata_code.assign(iata_code);

    string icao_code = "";
    getline(infile, icao_code, ',');
    icao_code.erase(std::remove(icao_code.begin(), icao_code.end(), '\"' ), icao_code.end());
    temp->al_icao_code.assign(icao_code);

    string dummy = "";
    getline(infile, dummy, '\"');

    string call_sign = "";
    getline(infile, call_sign, '\"');
    temp->al_call_sign.assign(call_sign);
    getline(infile, dummy, ',');

    getline(infile, dummy, '\"');
    string name = "";
    getline(infile, name, '\"');
    temp->al_name.assign(name);
    getline(infile, dummy, ',');

    string country_code = "";
    getline(infile, country_code);
    country_code.erase(std::remove(country_code.begin(), country_code.end(), '\"' ), country_code.end());
    int i = searchCountryCode(country_code);
    if(i < 0){
      printf("country not found\n");
      ALWAYS_ASSERT(false);
    }
    temp->al_co_id = i;

    airline_list.push_back(temp);
  }
  infile.close();
  ALWAYS_ASSERT(airline_list.size() > 0);
  printf("airline loaded\n");
}

static int64_t searchAirportCode(string code){
  if(airport_list.size()==0){
    printf("No Airport List Read\n");
    ALWAYS_ASSERT(false);
  }
  for(uint i = 0; i < airport_list.size(); i++){
    airport::value* temp = airport_list[i];
    if(temp->ap_code.str(true).compare(code)==0){
      return i+1;
    }
  }
  return -1;
}

static float deg2rad(float deg){
  return (deg*M_PI)/180.0f;
}

static float rad2deg(float rad) {
  return (rad * 180.0f / M_PI);
}


static float calc_distance(int64_t f_ap_id, int64_t t_ap_id){
  airport::value* f_ap = airport_list[f_ap_id-1];
  airport::value* t_ap = airport_list[t_ap_id-1];
  float f_long = f_ap->ap_longitude, f_lat = f_ap->ap_latitude,
	t_long = t_ap->ap_longitude, t_lat = t_ap->ap_latitude;
  float theta = f_long - t_long;
  float distance = sin(deg2rad(f_lat))*sin(deg2rad(t_lat)) + cos(deg2rad(f_lat))*cos(deg2rad(t_lat))*cos(deg2rad(theta));
  distance = acos(distance);
  distance = rad2deg(distance);
  return (distance*60*1.1515);
}

static void loadFlightsPerAirport(){
  if(flights_per_airport.size() > 0){
    printf("should not init twice\n");
    ALWAYS_ASSERT(false);
  }

  ifstream infile(FLIGHTS_PER_AIRPORT_FILE);
  string line = "";
  char fromA[4], toA[4];
  for(int i = 0; i < (int)scale_factor; i++){
    customers_per_airport.push_back(new std::vector<uint>());
    for(uint j = 0; j < airport_list.size(); j++)
      customers_per_airport[i]->push_back(0);
  }

  for(uint i = 0; i < airport_list.size(); i++) {
    flights_per_airport.push_back(new std::vector<int64_t>());
    airport_distances.push_back(new std::vector<to_airport_distance*>());
  }

  while(getline(infile, line)){
    if(sscanf(line.c_str(), " \"%3s-%3s\":", fromA, toA) > 1){
      string fA(fromA);
      string tA(toA);
      int f_ap_id = searchAirportCode(fA);
      int t_ap_id = searchAirportCode(tA);
      ALWAYS_ASSERT(f_ap_id > 0 && t_ap_id > 0);
      to_airport_distance* temp = new to_airport_distance();
      temp->to_ap_id = t_ap_id;
      temp->distance = calc_distance(f_ap_id, t_ap_id);
      airport_distances[f_ap_id-1]->push_back(temp);
      flights_per_airport[f_ap_id-1]->push_back(t_ap_id);
    }
  }
  infile.close();
  printf("FlightsPerAirport loaded\n");
  ALWAYS_ASSERT(flights_per_airport.size() > 0);
}

struct _dummy {};


class seats_worker_mixin : private _dummy {
#define DEFN_TBL_INIT_X(name) \
  , tbl_ ## name ## _vec(partitions.at(#name))

  public:
    seats_worker_mixin(const map<string, vector<abstract_ordered_index *>> &partitions) :
      _dummy() // so hacky...
      SEATS_TABLE_LIST(DEFN_TBL_INIT_X)
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

    SEATS_TABLE_LIST(DEFN_TBL_ACCESSOR_X);

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

    static const int FLIGHTS_DAYS_PAST = 1;
    static const int FLIGHTS_DAYS_FUTURE = 50;//50;
    static const int RESERVATION_PRICE_MIN = 100;
    static const int RESERVATION_PRICE_MAX = 1000;
    static const int NUM_AIRLINES = 1250;
    static const int FLIGHTS_NUM_SEATS = 150;
    static const int PROB_SEAT_OCCUPIED = 25;
    static const int CUSTOMERS_COUNT = 100000;//1000;//10000; //100000;
    static const int FLIGHTS_PER_DAY_MIN = 8125;//125;//325; //525; //1125;
    static const int FLIGHTS_PER_DAY_MAX = 8875;//375;//575; //775; //1875;
    static const int CUSTOMER_RETURN_FLIGHT_DAYS_MIN = 1;
    static const int CUSTOMER_RETURN_FLIGHT_DAYS_MAX = 14;
    static const int PROB_SINGLE_FLIGHT_RESERVATION = 10;
    static const int CUSTOMER_NUM_FREQUENTFLYERS_MIN = 0;
    static const int CUSTOMER_NUM_FREQUENTFLYERS_MAX = 10;
    static const int PROB_FIND_FLIGHTS_RANDOM_AIRPORTS = 10;
    static const int PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 25;
    static const int PROB_DELETE_WITH_CUSTOMER_ID_STR = 20;
    static const int PROB_DELETE_WITH_FREQUENTFLYER_ID_STR = 20;
    static const int NEARBY_AIRPORT_DISTANCE = 500;
    static const int PROB_UPDATE_FREQUENT_FLYER = 90;
    static const int PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20;
    static const int FLIGHTS_FIRST_CLASS_OFFSET = 10;
    static const int CACHE_LIMIT_PENDING_INSERTS = 10000;
    static const int CACHE_LIMIT_PENDING_UPDATES = 5000;
    static const int CACHE_LIMIT_PENDING_DELETES = 5000;
    static const int OPENSEATS_ADD_PERCENT = 50;
};

STATIC_COUNTER_DECL(scopedperf::tsc_ctr, seats_txn, seats_txn_cg);

class seats_worker : public bench_worker, public seats_worker_mixin {
  public:
    seats_worker(unsigned int worker_id,
	unsigned long seed, abstract_db *db,
	const map<string, abstract_ordered_index *> &open_tables,
	const map<string, vector<abstract_ordered_index *>> &partitions,
	spin_barrier *barrier_a, spin_barrier *barrier_b,
	uint partition_id)
      : bench_worker(worker_id, true, seed, db,
	  open_tables, barrier_a, barrier_b),
      seats_worker_mixin(partitions),
      partition_id(partition_id)
  {
    //int partid = (partition_id-1)%(int)scale_factor;
    obj_key0.reserve(str_arena::MinStrReserveLength);
    obj_key1.reserve(str_arena::MinStrReserveLength);
    obj_v.reserve(str_arena::MinStrReserveLength);

  }

    bool complete(){
      printf("newseatdrop %d newcustdrop %d urcon %d\n", newseatdrop, newcustdrop, urcon);
      //printf("read abort %d write abort %d absent abort %d\n", INFO::txn_read_abort, INFO::txn_write_abort, INFO::txn_absent_abort);
      return true;
    }

    txn_result txn_delete_reservation();
    static txn_result
      TxnDeleteRervation(bench_worker *w)
      {
	ANON_REGION("TxnDeleteRervation:", &seats_txn_cg);
	return static_cast<seats_worker *>(w)->txn_delete_reservation();
      }
    txn_result txn_find_flights();
    static txn_result
      TxnFindFlights(bench_worker *w)
      {
	ANON_REGION("TxnFindFlights:", &seats_txn_cg);
	return static_cast<seats_worker *>(w)->txn_find_flights();
      }
    txn_result txn_find_open_seats();
    static txn_result
      TxnFindOpenSeats(bench_worker *w)
      {
	ANON_REGION("TxnFindOpenSeats:", &seats_txn_cg);
	return static_cast<seats_worker *>(w)->txn_find_open_seats();
      }
    txn_result txn_new_reservation();
    static txn_result
      TxnNewReservation(bench_worker *w)
      {
	ANON_REGION("TxnNewReservation:", &seats_txn_cg);
	return static_cast<seats_worker *>(w)->txn_new_reservation();
      }
    txn_result txn_update_customer();
    static txn_result
      TxnUpdateCustomer(bench_worker *w)
      {
	ANON_REGION("TxnUpdateCustomer:", &seats_txn_cg);
	return static_cast<seats_worker *>(w)->txn_update_customer();
      }
    txn_result txn_update_reservation();
    static txn_result
      TxnUpdateReservation(bench_worker *w)
      {
	ANON_REGION("TxnUpdateReservation:", &seats_txn_cg);
	return static_cast<seats_worker *>(w)->txn_update_reservation();
      }

    void check_delay(){
      /*if(sdelc==0) sdelc = 1;
	if(sffc==0) sffc = 1;
	if(sfoc==0) sfoc = 1;
	if(snewc==0) snewc = 1;
	if(supcc==0) supcc = 1;
	if(suprc==0) suprc = 1;
	printf("del %ld ff %ld fo %ld new %ld upc %ld upr %ld\n", sdelt/sdelc, sfft/sffc, sfot/sfoc, snewt/snewc,
	supct/supcc, suprt/suprc);*/
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
	  w.push_back(workload_desc("DeleteRervation", double(g_txn_workload_mix[0])/100.0, TxnDeleteRervation));
	if (g_txn_workload_mix[1])
	  w.push_back(workload_desc("FindFlights", double(g_txn_workload_mix[1])/100.0, TxnFindFlights));
	if (g_txn_workload_mix[2])
	  w.push_back(workload_desc("FindOpenSeats", double(g_txn_workload_mix[2])/100.0, TxnFindOpenSeats));
	if (g_txn_workload_mix[3])
	  w.push_back(workload_desc("NewReservation", double(g_txn_workload_mix[3])/100.0, TxnNewReservation));
	if (g_txn_workload_mix[4])
	  w.push_back(workload_desc("UpdateCustomer", double(g_txn_workload_mix[4])/100.0, TxnUpdateCustomer));
	if (g_txn_workload_mix[5])
	  w.push_back(workload_desc("UpdateReservation", double(g_txn_workload_mix[5])/100.0, TxnUpdateReservation));
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
	size_t startid = start_thread_id;
	//nqueue_handle::nlist = NULL;
	//dqueue_handle::dlist = NULL;

	if(nthreads/num==8)
	  b = ((worker_id - startid)%num)*8 + (worker_id - startid)/num;

	rcu::s_instance.pin_current_thread(b);
	rcu::s_instance.fault_region();
	partnum = nthreads/num;
	if(partnum==0)
	  partnum = 1;
	//num of threads in the parition


	int threadpartid= ((worker_id-startid)/num)%partnum;
	int insert_num = all_inserted[partition_id-1]->size();
	//printf("thread %d size %ld step %d\n", threadpartid, all_inserted.size(), partnum);
	for(int i = threadpartid; i < insert_num; i+= 8){
	  reservation_entry *inserted = (*all_inserted[partition_id-1])[i];
	  //reservation_entry *toinsert = (*all_toinsert[partition_id-1])[i];
	  //	if (toinsert->customer_id == 8) printf("insert %ld\n", toinsert->customer_id);
	  //insert_queue.push(toinsert);
	  delete_queue.push(inserted);
	}
  for (int i = 0; i<CONFLICT_WINDOW_1;i++){
      reservation_entry *inserted = (*all_inserted[partition_id-1])[i];
      conflict_list.push_back(inserted->customer_id);
      //conflict_list_flight.push_back(inserted->flight_id);
  }
  for (int i = 0; i<CONFLICT_WINDOW_2;i++){
      reservation_entry *inserted = (*all_inserted[partition_id-1])[i];
      //conflict_list.push_back(inserted->customer_id);
      conflict_list_flight.push_back(inserted->flight_id);
  }
	rid_base = all_inserted[partition_id-1]->size()+ all_toinsert[partition_id-1]->size() +
	  all_inserted[partition_id-1]->size()*partnum;
	rid_range = all_inserted[partition_id-1]->size();


	//sdelt = sfft = sfot = snewt = supct = suprt = 0;
	//sdelc = sffc = sfoc = snewc = supcc = suprc = 0;
	//printf("size %ld\n", partition_list->size());
	//printf("partition %d threadinpart %d inqueue %ld dqueue %ld\n", partition_id, threadpartid, insert_queue.size(),
	//	delete_queue.size());
	newseatdrop = newcustdrop = urcon = 0;
	fsize = flight_list[partition_id-1].size();
	findex = threadpartid%fsize;
	usenum = threadpartid%3;
	if(usenum==0)
	  usenum = 1;
   }

  public:
    int count =0;
    int customerIndex = 0;
    int flightIndex = 0;
    std::vector<seats_worker*>* partition_list;
    std::queue<reservation_entry*> insert_queue;
    std::queue<reservation_entry*> delete_queue;
    uint partition_id;
    int partnum;
    //uint64_t sdelt, sfft, sfot, snewt, supct, suprt;
    //int sdelc, sffc, sfoc, snewc, supcc, suprc;
    int newseatdrop, newcustdrop, urcon;

  private:
    string obj_key0;
    string obj_key1;
    string obj_v;
    int64_t rid_base;
    int64_t rid_range;
    int64_t findex, fsize, usenum;
};



class seats_airline_loader : public bench_loader, public seats_worker_mixin {
  public:
    seats_airline_loader(unsigned long seed, abstract_db *db,
	const map<string, abstract_ordered_index *> &open_tables,
	const map<string, vector<abstract_ordered_index *>> &partitions)
      : bench_loader(seed, db, open_tables),
      seats_worker_mixin(partitions)
  {}

  protected:
    virtual void
      load()
      {
	string obj_buf;
	const ssize_t bsize = db->txn_max_batch_size();
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
	//try {
	for (uint i=1; i<=airline_list.size(); i++) {
	  const airline::key k(i);
	  airline::value* temp = airline_list[i-1];
	  //iattr left to be unitialized (iattr00-iattr15)
	  airline::value v(*temp);
	  tbl_airline(1)->insert(txn, Encode(k), Encode(obj_buf, v));
	  //ALWAYS_ASSERT(tbl_airline(1)->get(txn, Encode(k), obj_buf));
	  if (bsize !=-1 && !(i % bsize)) {
	    ALWAYS_ASSERT(db->commit_txn(txn));
	    txn = db->new_txn(txn_flags, arena, txn_buf());
	    arena.reset();
	  }
	}
	ALWAYS_ASSERT(db->commit_txn(txn));
	//}
	//catch(abstract_db::abstract_abort_exception &ex) {
	//	ALWAYS_ASSERT(false);
	//}
	//printf("airline loader loaded\n");
      }
};



class seats_airport_loader : public bench_loader, public seats_worker_mixin {
  public:
    seats_airport_loader(unsigned long seed, abstract_db *db,
	const map<string, abstract_ordered_index *> &open_tables,
	const map<string, vector<abstract_ordered_index *>> &partitions)
      : bench_loader(seed, db, open_tables),
      seats_worker_mixin(partitions)
  {}

  protected:
    virtual void
      load()
      {
	string obj_buf;
	//const ssize_t bsize = db->txn_max_batch_size();
	//bsize = 1;
	//cout<<airport_list.size()<<endl;
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
	for (uint i=1; i<=airport_list.size(); i++) {
	  const airport::key k(i);

	  airport::value* temp = airport_list[i-1];
	  airport::value v(*temp);
	  for(uint j = 1; j <= airport_distances[i-1]->size(); j++){
	    to_airport_distance* tad = (*airport_distances[i-1])[j-1];
	    airport_distance::key dk(i, tad->to_ap_id);
	    airport_distance::value dv(tad->distance);
	    tbl_airport_distance(1)->insert(txn, Encode(dk), Encode(obj_buf, dv));
	    ALWAYS_ASSERT(db->commit_txn(txn));
	    txn = db->new_txn(txn_flags, arena, txn_buf());
	    arena.reset();
	  }
	  tbl_airport(1)->insert(txn, Encode(k), Encode(obj_buf, v));
	  ALWAYS_ASSERT(db->commit_txn(txn));
	  txn = db->new_txn(txn_flags, arena, txn_buf());
	  arena.reset();
	  //ALWAYS_ASSERT(tbl_airport(1)->get(txn, Encode(k), obj_buf));
	  /*if (bsize !=-1 && !(i % bsize)) {
	    ALWAYS_ASSERT(db->commit_txn(txn));
	    txn = db->new_txn(txn_flags, arena, txn_buf());
	    arena.reset();
	    }*/
	}
	ALWAYS_ASSERT(db->commit_txn(txn));
	// ALWAYS_ASSERT(0);

	//printf("airport loader loaded\n");
      }

};

class country_callback : public abstract_ordered_index::scan_callback{
  public:
    country_callback() {}

    virtual bool invoke(
	const char *keyp, size_t keylen,
	const string &value)
    {
      country::key k_co;
      const country::key *k = Decode(keyp, k_co);
      k_cos.push_back(k);
      country::value v_co;
      const country::value *v = Decode(value, v_co);
      v_cos.push_back(v);
      return false;
    }

    vector<const country::key *>k_cos;
    vector<const country::value *>v_cos;

};


class seats_country_loader : public bench_loader, public seats_worker_mixin {
  public:
    seats_country_loader(unsigned long seed, abstract_db *db,
	const map<string, abstract_ordered_index *> &open_tables,
	const map<string, vector<abstract_ordered_index *>> &partitions)
      : bench_loader(seed, db, open_tables),
      seats_worker_mixin(partitions)
  {}

  protected:
    virtual void
      load()
      {
	string obj_buf, obj_key0, obj_key1;
	scoped_str_arena s_arena(arena);
	const ssize_t bsize = db->txn_max_batch_size();
	void *txn = db->new_txn(txn_flags, arena, txn_buf());
	try {
	  for (uint i=1; i<=country_list.size(); i++) {
	    const country::key k(i);
	    country::value* temp = country_list[i-1];
	    country::value v(*temp);
	    tbl_country(1)->insert(txn, Encode(k), Encode(obj_buf, v));
	    //ALWAYS_ASSERT(tbl_country(1)->get(txn, Encode(k), obj_buf));
	    if (bsize !=-1 && !(i % bsize)) {
	      ALWAYS_ASSERT(db->commit_txn(txn));
	      txn = db->new_txn(txn_flags, arena, txn_buf());
	      arena.reset();
	    }
	  }
	  ALWAYS_ASSERT(db->commit_txn(txn));
	}
	catch(abstract_db::abstract_abort_exception &ex) {
	  ALWAYS_ASSERT(false);
	}
	/*for (uint i=1; i<=country_list.size(); i++) {
	  bool com = false;
	  int retry = 0;
	  do{
	  void *txn = db->new_txn(txn_flags, arena, txn_buf());
	  const country::key k(i);
	  country::value* temp = country_list[i-1];
	  country::value v(*temp);
	  try{
	  if(i==16){
	  const country::key k_0(1);
	  const country::key k_1(15);
	  country_callback callback;
	  tbl_country(1)->scan(txn, Encode(obj_key0, k_0), &Encode(obj_key1, k_1), callback, s_arena.get());

	//const country::key ko(i);
	//tbl_country(1)->insert(txn, Encode(ko), Encode(obj_buf, v));
	}

	tbl_country(1)->insert(txn, Encode(k), Encode(obj_buf, v));

	bool found = tbl_country(1)->get(txn, Encode(k), obj_buf);
	com = db->commit_txn(txn);
	}catch(abstract_db::abstract_abort_exception &ex){
	db->abort_txn(txn);
	com = false;
	}
	if(!com){
	if(++retry%100==0)
	printf("retry insert %d i %d\n", retry, i);
	}
	}while(!com);
	}
	printf("country loader loaded\n");*/
      }
};


class seats_freqflyer_loader : public bench_loader, public seats_worker_mixin {
  public:
    seats_freqflyer_loader(unsigned long seed, abstract_db *db,
	const map<string, abstract_ordered_index *> &open_tables,
	const map<string, vector<abstract_ordered_index *>> &partitions,
	ssize_t part_id)
      : bench_loader(seed, db, open_tables),
      seats_worker_mixin(partitions),
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
	  if(nthreads/(int)scale_factor==8){
	    bp_id = p_id*8;
    }
	  PinToPartitionId(bp_id);

	  std::vector<uint>* custs_per_airport = customers_per_airport[p_id-1];
	  for (uint i=1 ; i<=custs_per_airport->size(); i++) {
	    for (uint j=1; j<=(*custs_per_airport)[i]; j++) {
	      uint64_t c_id = ((uint64_t)j) | ((uint64_t)i) << 48;
	      int ffnum = RandomNumber(r, CUSTOMER_NUM_FREQUENTFLYERS_MIN, CUSTOMER_NUM_FREQUENTFLYERS_MAX);
	      set<uint64_t> als;
	      for (int k=0; k<ffnum; k++) {
		uint64_t airline;
		do {
		  airline = RandomNumber(r, 1, airline_list.size());
		}while (als.find(airline)!=als.end());
		als.insert(airline);
		const frequent_flyer::key ffk(p_id, c_id, airline);
		frequent_flyer::value v;
		char id_buf[64];
		sprintf(id_buf, "%ld", c_id);
		v.ff_c_id_str.assign(id_buf);
		frequent_flyer_name_idx::key ki;
		ki.ff_partition = p_id;
		ki.ff_c_id_str.assign(id_buf);
		ki.ff_c_id = c_id;
		ki.ff_al_id = airline;
		frequent_flyer_name_idx::value vi;

		void *txn = db->new_txn(txn_flags, arena, txn_buf());
		tbl_frequent_flyer(p_id)->insert(txn, Encode(ffk), Encode(obj_buf,v));
		tbl_frequent_flyer_name_idx(p_id)->insert(txn, Encode(ki), Encode(obj_buf,vi));
		//ALWAYS_ASSERT(tbl_frequent_flyer(p_id)->get(txn, Encode(ffk), obj_buf));
		//ALWAYS_ASSERT(tbl_frequent_flyer_name_idx(p_id)->get(txn, Encode(ki), obj_buf));
		ALWAYS_ASSERT(db->commit_txn(txn));
	      }
	    }
	  }
	}
	//printf("ff loader loaded\n");
      }

  private:
    ssize_t part_id;

};


class seats_customer_loader : public bench_loader, public seats_worker_mixin {
  public:
    seats_customer_loader(unsigned long seed, abstract_db *db,
	const map<string, abstract_ordered_index *> &open_tables,
	const map<string, vector<abstract_ordered_index *>> &partitions,
	ssize_t part_id)
      : bench_loader(seed, db, open_tables),
      seats_worker_mixin(partitions),
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
	  if(nthreads/(int)scale_factor==8){
	    bp_id = p_id*8;
    }
	  PinToPartitionId(bp_id);
	  for (uint i=0 ; i<CUSTOMERS_COUNT; i++) {
	    seats_customer::value v;
	    v.c_balance = RandomNumber(r, 1000, 10000);
	    int64_t airport = RandomNumber(r, 1, airport_list.size());
	    int64_t cust_id = ++(*customers_per_airport[p_id-1])[airport -1];
	    int64_t id = cust_id | airport << 48;
	    seats_customer::key k;
	    k.c_partition = p_id ;

	    k.c_id = id;
	    char id_buf[64];

	    sprintf(id_buf, "%ld", id);

	    v.c_id_str.assign(id_buf);

	    v.c_base_ap_id = airport;
	    seats_customer_name_idx::key ki;
	    ki.c_partition = p_id;
	    ki.c_id_str.assign(id_buf);

	    seats_customer_name_idx::value vi(id);
	    //bool succ = false;
	    try {
	      void *txn = db->new_txn(txn_flags, arena, txn_buf());
	      tbl_seats_customer(p_id)->insert(txn, Encode(k), Encode(obj_buf, v));
	      tbl_seats_customer_name_idx(p_id)->insert(txn, Encode(ki), Encode(obj_buf, vi));
	      ALWAYS_ASSERT(db->commit_txn(txn));
	    }
	    catch(abstract_db::abstract_abort_exception &ex) {
	      ALWAYS_ASSERT(false);
	    }

#if 0

	    void *txn = db->new_txn(txn_flags, arena, txn_buf());
	    try{
	      tbl_seats_customer(p_id)->insert(txn, Encode(k), Encode(obj_buf, v));
	      tbl_seats_customer_name_idx(p_id)->insert(txn, Encode(ki), Encode(obj_buf, vi));
	      ALWAYS_ASSERT(tbl_seats_customer(p_id)->get(txn, Encode(k), obj_buf));
	      ALWAYS_ASSERT(tbl_seats_customer_name_idx(p_id)->get(txn, Encode(ki), obj_buf));
	      ALWAYS_ASSERT(db->commit_txn(txn));
	    }catch (abstract_db::abstract_abort_exception &ex) {
	      db->abort_txn(txn);
	    }

	    void *atxn = db->new_txn(txn_flags, arena, txn_buf());
	    ALWAYS_ASSERT(tbl_seats_customer(p_id)->get(atxn, Encode(k), obj_buf));
	    ALWAYS_ASSERT(tbl_seats_customer_name_idx(p_id)->get(atxn, Encode(ki), obj_buf));
	    ALWAYS_ASSERT(db->commit_txn(atxn));
	    if(id==0x4700000000000c){
	      printf("inserted id %lx %d\n", id, p_id);
	    }
#endif

	  }

	  customer_loaded[p_id-1] = true;
	}
      }

  private:
    ssize_t part_id;

};

class seats_reservation_loader : public bench_loader, public seats_worker_mixin {
  public:
    seats_reservation_loader(unsigned long seed, abstract_db *db,
	const map<string, abstract_ordered_index *> &open_tables,
	const map<string, vector<abstract_ordered_index *>> &partitions,
	ssize_t part_id)
      : bench_loader(seed, db, open_tables),
      seats_worker_mixin(partitions),
      part_id(part_id)
  {}

  protected:
    struct return_customer{
      int64_t c_id;
      int64_t return_time;
      int64_t ap_id;
    };

    bool inList(std::vector<int64_t> clist, int64_t c_id){
      for(int64_t ctemp : clist){
	if(ctemp==c_id)
	  return true;
      }
      return false;
    }

    std::vector<int64_t>* getReturnCustomer(int arrive_ap_id, int arrive_time){
      std::vector<int64_t>* ret = new std::vector<int64_t>();
      for(return_customer* rc : rclist){
	if(rc->ap_id==arrive_ap_id&&arrive_time >= rc->return_time)
	  ret->push_back(rc->c_id);
      }
      return ret;
    }

    virtual void
      load()
      {
	//return;
	string obj_buf;
	int st_part = part_id, ed_part = part_id;
	if(part_id == -1){
	  st_part = 1;
	  ed_part = NumPartitions();
	}
	int allbooked[ed_part-st_part+1];

	for(int p_id = st_part; p_id <= ed_part; p_id++){
	  int bp_id = p_id;
	  if(nthreads/(int)scale_factor==8){
	    bp_id = p_id*8;
    }
	  PinToPartitionId(bp_id);

	  uint64_t fid = 1;
	  allbooked[p_id-st_part] = 0;
      for(int i = 0; i < TOTAL_DAY; i++){
          int flights = RandomNumber(r, FLIGHTS_PER_DAY_MIN, FLIGHTS_PER_DAY_MAX);
          for(int fno = 0; fno < flights; fno++){
              int temp_fid = fid++;
              const flight::key k(p_id, temp_fid);
              flight::value v;
              std::vector<int64_t> *arrive_ap_id = NULL;
              do{
                  int d_ap_id = RandomNumber(r, 1, airport_list.size());
                  v.f_depart_ap_id = (uint64_t)(d_ap_id);
                  arrive_ap_id = flights_per_airport[d_ap_id-1];
              }while(arrive_ap_id->size()==0);

              v.f_arrive_ap_id = (*arrive_ap_id)[RandomNumber(r,1 ,arrive_ap_id->size()) - 1];
              v.f_depart_time = i*TIMES_PER_DAY + RandomNumber(r, 0, TIMES_PER_DAY-1);
              v.f_arrive_time = v.f_depart_time + RandomNumber(r,5,10);
              v.f_al_id = RandomNumber(r, 1, airline_list.size());
              v.f_status = 0;
              v.f_base_price = RandomNumber(r, RESERVATION_PRICE_MIN, RESERVATION_PRICE_MAX);
              v.f_seats_total = FLIGHTS_NUM_SEATS;
              v.f_seats_left = FLIGHTS_NUM_SEATS;
              for (int seatnum = 0; seatnum < FLIGHTS_NUM_SEATS; seatnum++){
                  bool occupied = RandomNumber(r, 1, 40000) < PROB_SEAT_OCCUPIED;
                  if (occupied){
                      v.f_seats_left--;
                      allbooked[p_id-st_part]++;
                  }
              }
              flight_list[p_id-1].push_back(v);
              flight_time_idx::key ki(p_id, v.f_depart_ap_id, v.f_arrive_ap_id,v.f_depart_time, temp_fid);
              flight_time_idx::value vi;
              void *txn = db->new_txn(txn_flags, arena, txn_buf());
              tbl_flight(p_id)->insert(txn, Encode(k), Encode(obj_buf, v));
              tbl_flight_time_idx(p_id)->insert(txn, Encode(ki), Encode(obj_buf, vi));
              //ALWAYS_ASSERT(tbl_flight(p_id)->get(txn, Encode(k), obj_buf));
              //ALWAYS_ASSERT(tbl_flight_time_idx(p_id)->get(txn, Encode(ki), obj_buf));
              ALWAYS_ASSERT(db->commit_txn(txn));
          }
      }
	  //printf("%d flights %ld booked %d\n", p_id, flight_list.size(), allbooked[p_id-st_part]);
	}
	//printf("flights loader %ld part 0 size %ld loaded\n", flight_list.size(), flight_list[0]->size());

	for(int p_id = st_part; p_id <= ed_part; p_id++){
	  int bp_id = p_id;
	  if(nthreads/(int)scale_factor==8){
	    bp_id = p_id*8;
    }
	  PinToPartitionId(bp_id);

	  if(rclist.size() > 0)
	    rclist.clear();
	  int fsize = flight_list[p_id-1].size()/50;
	  ALWAYS_ASSERT(fsize > 0);

	  uint64_t rid = 1;
	  while(!customer_loaded[p_id-1])
	    usleep(10);
	  //printf("part %d has %d reservation loader started\n", p_id, fsize);
	  for(int i = 0; i < fsize; i++){
	    flight::value &f_v = (flight_list[p_id-1])[i];
	    int64_t depart_ap_id = f_v.f_depart_ap_id;
	    int64_t depart_time = f_v.f_depart_time;
	    int64_t arrive_ap_id = f_v.f_arrive_ap_id;
	    int64_t arrive_time = f_v.f_arrive_time;
	    int bookseats = FLIGHTS_NUM_SEATS - f_v.f_seats_left;
	    std::vector<int64_t> *rcs = getReturnCustomer(arrive_ap_id, arrive_time);
	    //get returning customer
	    std::vector<int64_t> clist;
	    std::vector<int64_t> touse;
	    for(int seatnum = 0; seatnum < bookseats; seatnum++){
	      bool local = clist.size() < (*customers_per_airport[p_id-1])[depart_ap_id-1];
	      int64_t cid = -1;
	      bool isRet = false;
	      do{
		isRet = false;
    local = true;
		if(rcs->size() > 0){
		  cid = rcs->back();
		  isRet = true;
		  rcs->pop_back();
		}
		else if(local){
		  int size = (*customers_per_airport[p_id-1])[depart_ap_id-1];
      int restricted_size=3;
      if (size>restricted_size)
          size = restricted_size;
		  ALWAYS_ASSERT(size > 0);
		  cid = (depart_ap_id << 48) | RandomNumber(r, 1, size);
		}
		else{
		  int64_t some_ap = RandomNumber(r, 1, airport_list.size());
		  int size = (*customers_per_airport[p_id-1])[some_ap-1];
		  while(size==0){
		    some_ap = RandomNumber(r, 1, airport_list.size());
		    size = (*customers_per_airport[p_id-1])[some_ap-1];
		  }
		  cid = (some_ap << 48) | RandomNumber(r, 1, size);
		}
		//printf("f %d s %d cid %d in %d\n", i, seatnum, cid, inList(clist, cid));
	      }while(inList(clist, cid));
	      clist.push_back(cid);

	      if(!isRet){
		int rp = RandomNumber(r, 1,100);
		if(rp < PROB_SINGLE_FLIGHT_RESERVATION){}
		else{
		  int ret_time = depart_time + RandomNumber(r, CUSTOMER_RETURN_FLIGHT_DAYS_MIN,
		      CUSTOMER_RETURN_FLIGHT_DAYS_MAX)*TIMES_PER_DAY;
		  return_customer* rc = new return_customer();
		  rc->c_id = cid;
		  rc->ap_id = depart_ap_id;
		  rc->return_time = ret_time;
		  rclist.push_back(rc);
		}
	      }
	      const reservation::key k(p_id, i+1, cid, rid++);
	      {
		reservation_entry* inserted = new reservation_entry();
		inserted->customer_id = cid;
		inserted->flight_id = i+1;
		inserted->seat_num = seatnum;
		inserted->r_id = k.r_id;
		//if(RandomNumber(r, 1, 100) < 10)
		all_inserted[p_id-1]->push_back(inserted);
	      }
	      //for one to be inserted
	      /*{
		int64_t some_ap = RandomNumber(r, 1, airport_list.size());
		int size = (*customers_per_airport[p_id-1])[some_ap-1];
		while(size==0){
		some_ap = RandomNumber(r, 1, airport_list.size());
		size = (*customers_per_airport[p_id-1])[some_ap-1];
		}
		int64_t tcid = (some_ap << 48) | RandomNumber(r, 1, size);

		while(inList(clist, tcid)){
		some_ap = RandomNumber(r, 1, airport_list.size());
		size = (*customers_per_airport[p_id-1])[some_ap-1];
		while(size==0){
		some_ap = RandomNumber(r, 1, airport_list.size());
		size = (*customers_per_airport[p_id-1])[some_ap-1];
		}

		tcid = (some_ap << 48) | RandomNumber(r, 1, size);
		}
		clist.push_back(tcid);

		reservation_entry* toinsert = new reservation_entry();
		toinsert->customer_id = tcid;
		toinsert->flight_id = i+1;
		toinsert->r_id = k.r_id + allbooked[p_id-st_part];
		ALWAYS_ASSERT(f_v->f_seats_left > 0);
		int64_t toseat = bookseats + RandomNumber(r, 0, f_v->f_seats_left - 1);
		while(inList(touse, toseat)){
		toseat = bookseats + RandomNumber(r, 0, f_v->f_seats_left - 1);
		}
		touse.push_back(toseat);
		toinsert->seat_num = toseat;
		all_toinsert[p_id-1]->push_back(toinsert);
		}*/


	      reservation::value* r_v = new reservation::value();
	      r_v->r_seat = seatnum;
	      r_v->r_price = float(RandomNumber(r, RESERVATION_PRICE_MIN, RESERVATION_PRICE_MAX));
	      if(upcoming_rid < 0){
		int max_past_time = FLIGHTS_DAYS_PAST*TIMES_PER_DAY - 1;
		if(depart_time > max_past_time)
		  upcoming_rid = k.r_id;
	      }
	      const reservation_seat_idx::key k_ri(p_id, i+1, seatnum, k.r_id);
	      reservation_seat_idx::value v_ri;

	      void *txn = db->new_txn(txn_flags, arena, txn_buf());
	      tbl_reservation(p_id)->insert(txn, Encode(k), Encode(obj_buf, *r_v));
	      tbl_reservation_seat_idx(p_id)->insert(txn, Encode(k_ri), Encode(obj_buf, v_ri));
	      //ALWAYS_ASSERT(tbl_reservation(p_id)->get(txn, Encode(k), obj_buf));
	      //ALWAYS_ASSERT(tbl_reservation_seat_idx(p_id)->get(txn, Encode(k_ri), obj_buf));
	      ALWAYS_ASSERT(db->commit_txn(txn));
	    }
	  }
	  //printf("%d reservation loader loaded\n", p_id);
	}
	//printf("reservation loader loaded\n");
      }

  private:
    ssize_t part_id;
    std::vector<return_customer*> rclist;
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

class reservation_seat_idx_callback : public abstract_ordered_index::scan_callback{
  public:
    reservation_seat_idx_callback():k_c(0) {}
    virtual bool invoke(
	const char *keyp, size_t keylen,
	const string &value)
    {
      k_c = Decode(keyp, k_c_temp);
      return false;
    }
    inline const reservation_seat_idx::key *
      get_key() const
      {
	return k_c;
      }
  private:
    reservation_seat_idx::key k_c_temp;
    const reservation_seat_idx::key *k_c;
};

class ff_name_idx_callback : public abstract_ordered_index::scan_callback{
  public:
    ff_name_idx_callback():k_c(0) {}
    virtual bool invoke(
	const char *keyp, size_t keylen,
	const string &value)
    {
      k_c = Decode(keyp, k_c_temp);
      return false;
    }
    inline const frequent_flyer_name_idx::key *
      get_key() const
      {
	return k_c;
      }
  private:
    frequent_flyer_name_idx::key k_c_temp;
    const frequent_flyer_name_idx::key *k_c;
};

class reservation_callback : public abstract_ordered_index::scan_callback{
  public:
    reservation_callback():k_c(0), v_c(0) {}
    virtual bool invoke(
	const char *keyp, size_t keylen,
	const string &value)
    {
      k_c = Decode(keyp, k_c_temp);
      v_c = Decode(value, v_c_temp);
      return false;
    }
    inline const reservation::key *
      get_key() const
      {
	return k_c;
      }
    inline const reservation::value *
      get_value() const
      {
	return v_c;
      }
  private:
    reservation::key k_c_temp;
    const reservation::key *k_c;
    reservation::value v_c_temp;
    const reservation::value *v_c;
};

class frequent_flyer_callback : public abstract_ordered_index::scan_callback{
  public:
    frequent_flyer_callback() {}

    virtual bool invoke(
	const char *keyp, size_t keylen,
	const string &value)
    {
      frequent_flyer::key k_ff_temp;
      const frequent_flyer::key *k = Decode(keyp, k_ff_temp);
      k_ff.push_back(k);
      frequent_flyer::value v_ff_temp;
      const frequent_flyer::value *v = Decode(value, v_ff_temp);
      v_ff.push_back(v);
      return false;
    }

    vector<const frequent_flyer::key *>k_ff;
    vector<const frequent_flyer::value *>v_ff;

};

class customer_callback :public abstract_ordered_index::scan_callback{
  public:
    customer_callback() {}
    virtual bool invoke(const char *keyp, size_t keylen,
	const string &value)
    {
      seats_customer::key temp;
      //const seats_customer::key *k = Decode(keyp, temp);
      //printf("scan %ld\n" ,k->c_id);
      return true;
    }
};
  seats_worker::txn_result
seats_worker::txn_delete_reservation()
{
  //return txn_result(true, 0);
  //uint64_t st = rdtsc();
  scoped_str_arena s_arena(arena);
  void *txn = db->new_txn(txn_flags, arena, txn_buf(),abstract_db::HINT_SEATS_DEFAULT);
  db->init_txn(txn,cgraph,delete_reservation_type);
  bool txn_ret = false;
  if (delete_queue.size()<=0)
    return txn_result(true,0);
  try {
    reservation_entry *reserv = delete_queue.front();
    if (reserv == NULL) printf("Nothing to delete!\n");
    //printf("txn here?\n");
    int rand = RandomNumber(r, 1, 100);
    int64_t c_id = reserv->customer_id;
    int64_t f_id = reserv->flight_id;
    int64_t al_id = 0;
    if (rand <= PROB_DELETE_WITH_CUSTOMER_ID_STR) {
      char id_buf[64];
      sprintf(id_buf, "%ld", c_id);

      seats_customer_name_idx::key k_c;
      k_c.c_partition = partition_id;
      k_c.c_id_str.assign(id_buf);

      ALWAYS_ASSERT(tbl_seats_customer_name_idx(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v));
      seats_customer_name_idx::value v_c_temp;
      const seats_customer_name_idx::value* v_c = Decode(obj_v, v_c_temp);
      if(v_c->c_id != c_id){
      	printf("cid %ld %ld\n", v_c->c_id, c_id);
	      ALWAYS_ASSERT(false);
      }
    }
    else if (rand <= PROB_DELETE_WITH_CUSTOMER_ID_STR + PROB_DELETE_WITH_FREQUENTFLYER_ID_STR){
      char id_buf[64];
      sprintf(id_buf, "%ld", c_id);
      frequent_flyer_name_idx::key k_c_0;
      k_c_0.ff_partition = partition_id;
      k_c_0.ff_c_id_str.assign(id_buf);
      k_c_0.ff_c_id = 0;
      k_c_0.ff_al_id = 0;
      frequent_flyer_name_idx::key k_c_1;
      k_c_1.ff_partition = partition_id;
      k_c_1.ff_c_id_str.assign(id_buf);
      k_c_1.ff_c_id = numeric_limits<int64_t>::max();
      k_c_1.ff_al_id = numeric_limits<int64_t>::max();

      ff_name_idx_callback callback;
      tbl_frequent_flyer_name_idx(partition_id)->scan(txn, Encode(obj_key0, k_c_0), &Encode(obj_key1, k_c_1), callback, s_arena.get());
      const frequent_flyer_name_idx::key *k_c = callback.get_key();
      if (k_c) {
	       ALWAYS_ASSERT(k_c->ff_c_id == c_id);
	       al_id = k_c->ff_al_id;
      }
    }
    else {
    }
    float r_price=1;

delete_piece_1:
    try{
      db->one_op_begin(txn);
      const seats_customer::key k_c(partition_id, c_id);
      customerIndex++;
      if (customerIndex>=CONFLICT_WINDOW_1)
          customerIndex=0;
      int64_t fake_c_id = conflict_list[customerIndex];
      const seats_customer::key fake_k_c(partition_id,fake_c_id);
      bool found = tbl_seats_customer(partition_id)->get(txn, Encode(obj_key0, fake_k_c), obj_v);
      if(!found){
  //printf("partition_id %lx cid_id %lx\n", partition_id, (unsigned int)c_id);
       printf("not found error\n");
       ALWAYS_ASSERT(false);
      }
      seats_customer::value v_c_temp;
      const seats_customer::value *v_c = Decode(obj_v, v_c_temp);

      seats_customer::value v_c_new(*v_c);
      v_c_new.c_iattr00++;
      v_c_new.c_iattr10--;
      v_c_new.c_iattr11--;
      v_c_new.c_balance -= r_price;
      tbl_seats_customer(partition_id)->put(txn, Encode(str(), fake_k_c), Encode(obj_v, v_c_new));
      if(!db->one_op_end(txn))
        goto delete_piece_1;
    } catch(piece_abort_exception &ex){
      db->atomic_ops_abort(txn);
      goto delete_piece_1;
    }


delete_piece_2:

    try{
      db->one_op_begin(txn);
      const flight::key k_f(partition_id, f_id);
      flightIndex++;
      if (flightIndex>=CONFLICT_WINDOW_2)
          flightIndex=0;
      int64_t fake_f_id = conflict_list_flight[flightIndex];
      const flight::key fake_k_f(partition_id, fake_f_id);
      ALWAYS_ASSERT(tbl_flight(partition_id)->get(txn, Encode(obj_key0, fake_k_f), obj_v));
      flight::value v_f_temp;
      const flight::value *v_f = Decode(obj_v, v_f_temp);
      flight::value v_f_new(*v_f);
      v_f_new.f_seats_left++;
      tbl_flight(partition_id)->put(txn, Encode(str(), fake_k_f), Encode(obj_v, v_f_new));


      if(!db->one_op_end(txn))
	       goto delete_piece_2;
    } catch(piece_abort_exception &ex){
      db->atomic_ops_abort(txn);
      goto delete_piece_2;
    }

    bool delete_piece_3_success = false;
delete_piece_3:
     delete_piece_3_success = false;
     try{
      do {
         db->mul_ops_begin(txn);
         const reservation::key k_r_0(partition_id, f_id, c_id, 0);
         const reservation::key k_r_1(partition_id, f_id, c_id, numeric_limits<int64_t>::max());
         reservation_callback callback;
         tbl_reservation(partition_id)->scan(txn,Encode(obj_key0, k_r_0), &Encode(obj_key1, k_r_1), callback, s_arena.get());
         if(callback.get_value()==NULL||callback.get_key()==NULL){
             if(!db->mul_ops_end(txn))
                goto delete_piece_3;
             //txn_ret = db->commit_txn(txn);
             //if(txn_ret){
             //    delete_queue.pop();
             //}
             //return txn_result(txn_ret, 0);
              break;
         }
         const reservation::key *k_r = callback.get_key();
         const reservation::value *v_r = callback.get_value();
         r_price = v_r->r_price;
         tbl_reservation(partition_id)->remove(txn, Encode(str(), *k_r));
         reservation_seat_idx::key k_ri;
         k_ri.r_partition = partition_id;
         k_ri.r_f_id = f_id;
         k_ri.r_seat = v_r->r_seat;
         k_ri.r_id = k_r->r_id;
         tbl_reservation_seat_idx(partition_id)->remove(txn, Encode(str(), k_ri));
         if(!db->mul_ops_end(txn))
          goto delete_piece_3;
        delete_piece_3_success = true;
      } while (false);
     } catch(piece_abort_exception &ex){
        db->atomic_ops_abort(txn);
        goto delete_piece_3;
    }



delete_piece_4:

    try{
       db->one_op_begin(txn);
if (delete_piece_3_success) {
      if (al_id != 0) {
	     const frequent_flyer::key k_ff(partition_id, c_id, al_id);
	     ALWAYS_ASSERT(tbl_frequent_flyer(partition_id)->get(txn, Encode(obj_key0, k_ff), obj_v));
	     frequent_flyer::value v_ff_temp;
	     const frequent_flyer::value *v_ff = Decode(obj_v, v_ff_temp);
	     frequent_flyer::value v_ff_new(*v_ff);
	     v_ff_new.ff_iattr10--;
	     tbl_frequent_flyer(partition_id)->put(txn, Encode(str(), k_ff), Encode(obj_v, v_ff_new));
       }
}
       if(!db->one_op_end(txn))
	       goto delete_piece_4;
    }
    catch(piece_abort_exception &ex){

      db->atomic_ops_abort(txn);
      goto delete_piece_4;
    }

    txn_ret = db->commit_txn(txn);
    if(txn_ret){
      delete_queue.pop();
      //insert_queue.push(reserv);
    }
    //sdelt += rdtsc() - st;
    //sdelc++;
    return txn_result(txn_ret, 0);
  }catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    txn_ret = false;
  }
  //sdelt += rdtsc() - st;
  //sdelc++;
  return txn_result(txn_ret, 0);

}

  seats_worker::txn_result
seats_worker::txn_find_flights()
{
  //uint64_t st = rdtsc();
  int64_t depart_airport_id = 0;
  int64_t arrive_airport_id = 0;
  int64_t start_time = 0;
  int64_t stop_time = 0;
  float distance = -1;

  if(RandomNumber(r,1,100) <= PROB_FIND_FLIGHTS_RANDOM_AIRPORTS){
    depart_airport_id = RandomNumber(r, 1, airport_list.size());
    std::vector<int64_t> *temp = flights_per_airport[depart_airport_id-1];
    while(temp->size()==0){
      depart_airport_id = RandomNumber(r, 1, airport_list.size());
      temp = flights_per_airport[depart_airport_id-1];
    }
    int index = RandomNumber(r, 1, temp->size());
    arrive_airport_id = (*temp)[index-1];
    start_time = (PAST_DAY+RandomNumber(r, 0, FUTURE_DAY))*TIMES_PER_DAY;
    stop_time = start_time + TIMES_PER_DAY*2;
  }
  else{
    int index = RandomNumber(r, 1, flight_list[partition_id-1].size());
    flight::value &temp = (flight_list[partition_id-1])[index-1];
    depart_airport_id = temp.f_depart_ap_id;
    arrive_airport_id = temp.f_arrive_ap_id;
    start_time = temp.f_depart_time - TIMES_PER_DAY/2;
    stop_time = temp.f_depart_time + TIMES_PER_DAY/2;
  }

  if(RandomNumber(r, 1, 100) <= PROB_FIND_FLIGHTS_NEARBY_AIRPORT)
    distance = NEARBY_AIRPORT_DISTANCE;

  void *txn = db->new_txn(txn_flags | transaction_base::TXN_FLAG_READ_ONLY, arena, txn_buf(),  abstract_db::HINT_SEATS_READ_ONLY_DEFAULT);
  scoped_str_arena s_arena(arena);
  bool txn_ret = false;
  try {
    airport_distance::key ap_dist_0;
    ap_dist_0.d_ap_id0 = depart_airport_id;
    ap_dist_0.d_ap_id1 = 0;

    airport_distance::key ap_dist_1;
    ap_dist_1.d_ap_id0 = depart_airport_id;
    ap_dist_1.d_ap_id1 = numeric_limits<int64_t>::max();

    dstatic_limit_airport_distance_callback<512> c(distance, false); // probably a safe bet for now
    tbl_airport_distance(1)->scan(txn, Encode(obj_key0, ap_dist_0), &Encode(obj_key1, ap_dist_1), c, s_arena.get());
    std::vector<int64_t> list;
    list.push_back(arrive_airport_id);
    if(c.size()==1)
      list.push_back(c.values[0].first);
    else if(c.size() > 1){
      int64_t min2[2];
      float mind2[2];
      if(c.values[0].second > c.values[1].second){
	min2[0] = c.values[1].first;
	min2[1] = c.values[0].first;
	mind2[0] = c.values[1].second;
	mind2[1] = c.values[0].second;
      }
      else{
	min2[0] = c.values[0].first;
	min2[1] = c.values[1].first;
	mind2[0] = c.values[0].second;
	mind2[1] = c.values[1].second;
      }
      for(uint i = 2; i < c.size(); i++){
	if(c.values[i].second < mind2[0]){
	  min2[1] = min2[0];
	  mind2[1] = mind2[0];
	  min2[0] = c.values[i].first;
	  mind2[0] = c.values[i].second;
	}
	else if(c.values[i].second < mind2[1]){
	  min2[1] = c.values[i].first;
	  mind2[1] = c.values[i].second;
	}
      }
      list.push_back(min2[0]);
      list.push_back(min2[1]);
    }

    for(uint i = 0; i < list.size(); i++){
      dstatic_limit_flight_callback<512> fc(true, start_time, stop_time);

      flight_time_idx::key ft_idx0;
      ft_idx0.f_partition = partition_id;
      ft_idx0.f_depart_ap_id = depart_airport_id;
      ft_idx0.f_arrive_ap_id = list[i];
      ft_idx0.f_depart_time = 0;
      ft_idx0.f_id = 0;

      flight_time_idx::key ft_idx1;
      ft_idx1.f_partition = partition_id;
      ft_idx1.f_depart_ap_id = depart_airport_id;
      ft_idx1.f_arrive_ap_id = list[i];
      ft_idx1.f_depart_time = numeric_limits<int64_t>::max();
      ft_idx1.f_id = numeric_limits<int64_t>::max();

      tbl_flight_time_idx(partition_id)->scan(txn, Encode(obj_key0, ft_idx0), &Encode(obj_key1, ft_idx1), fc, s_arena.get());

      for(uint j = 0; j < fc.values.size(); j++){
	int64_t f_id = fc.values[j];
	flight::key k_f(partition_id, f_id);
	ALWAYS_ASSERT(tbl_flight(partition_id)->get(txn, Encode(obj_key0, k_f), obj_v));
	flight::value v_f_temp;
	const flight::value *v_f = Decode(obj_v, v_f_temp);
	//int64_t depart_time = v_f->f_depart_time, arrive_time = v_f->f_arrive_time;
	int64_t depart_ap_id = v_f->f_depart_ap_id, arrive_ap_id = v_f->f_arrive_ap_id;
	//FORUNUSED(depart_time);
	//FORUNUSED(arrive_time);

	airport::key k_de(depart_ap_id);
	ALWAYS_ASSERT(tbl_airport(1)->get(txn, Encode(obj_key0, k_de), obj_v));
	airport::value v_de_temp;
	const airport::value *v_de = Decode(obj_v, v_de_temp);

	country::key k_de_co(v_de->ap_co_id);
	ALWAYS_ASSERT(tbl_country(1)->get(txn, Encode(obj_key0, k_de_co), obj_v));
	country::value v_de_co_temp;
	//const country::value *v_de_co = Decode(obj_v, v_de_co_temp);
	//FORUNUSED(v_de_co);

	airport::key k_ar(arrive_ap_id);
	ALWAYS_ASSERT(tbl_airport(1)->get(txn, Encode(obj_key0, k_ar), obj_v));
	airport::value v_ar_temp;
	const airport::value *v_ar = Decode(obj_v, v_ar_temp);

	country::key k_ar_co(v_ar->ap_co_id);
	ALWAYS_ASSERT(tbl_country(1)->get(txn, Encode(obj_key0, k_ar_co), obj_v));
	country::value v_ar_co_temp;
	//const country::value *v_ar_co = Decode(obj_v, v_ar_co_temp);
	//FORUNUSED(v_ar_co);

	//printf("index[%d][%d] flight %ld depart %d from %s arrive %d at %s\n", i, j, f_id, depart_time, v_de->ap_city.data(), arrive_time, v_ar->ap_city.data());
      }
    }

    txn_ret = db->commit_txn(txn);
    //sfft += rdtsc() - st;
    //sffc++;
    return txn_result(txn_ret, 0);
  }catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    txn_ret = false;
  }
  //sfft += rdtsc() - st;
  //sffc++;
  return txn_result(txn_ret, 0);

}

  seats_worker::txn_result
seats_worker::txn_find_open_seats()
{
  //uint64_t st = rdtsc();
  int fsize = flight_list[partition_id-1].size();
  ALWAYS_ASSERT(fsize > 0);
  int findex = RandomNumber(r, 1, fsize) - 1;
  int f_id = findex + 1;
  //flight::value* f_v = (*flight_list[partition_id-1])[findex];
  //FORUNUSED(f_v);
  bool booked[FLIGHTS_NUM_SEATS];
  for(int i = 0; i < FLIGHTS_NUM_SEATS; i++)
    booked[i] = false;


  void *txn = db->new_txn(txn_flags | transaction_base::TXN_FLAG_READ_ONLY, arena, txn_buf(), abstract_db::HINT_SEATS_READ_ONLY_DEFAULT);
  scoped_str_arena s_arena(arena);
  bool txn_ret = false;
  try {
    flight::key k_f(partition_id, f_id);
    ALWAYS_ASSERT(tbl_flight(partition_id)->get(txn, Encode(obj_key0, k_f), obj_v));
    flight::value v_f_temp;
    //const flight::value *v_f = Decode(obj_v, v_f_temp);
    //float base_price = v_f->f_base_price;
    //int seats_total = v_f->f_seats_total;
    //int seats_left = v_f->f_seats_left;
    //float seat_price = base_price*(2 - seats_left/(seats_total+0.0));

    const reservation::key k_r0(partition_id, f_id, 0, 0);
    const reservation::key k_r1(partition_id, f_id, numeric_limits<int64_t>::max(), numeric_limits<int64_t>::max());
    dstatic_limit_reservation_seatnum_callback<150> c(false);
    tbl_reservation(partition_id)->scan(txn, Encode(obj_key0, k_r0), &Encode(obj_key1, k_r1), c, s_arena.get());
    for(int64_t seat : c.values)
      booked[seat] = true;
    txn_ret = db->commit_txn(txn);

    for(int i = 0; i < FLIGHTS_NUM_SEATS; i++){
      if(!booked[i]){
	//float price = seat_price * (i < FLIGHTS_FIRST_CLASS_OFFSET ? 2 : 1);
	//FORUNUSED(price);
	if(insert_queue.size() > CACHE_LIMIT_PENDING_INSERTS)
	  break;
	int64_t some_ap = RandomNumber(r, 1, 40);//airport_list.size());
	int size = (*customers_per_airport[partition_id-1])[some_ap-1];
	while(size==0){
	  some_ap = RandomNumber(r, 1, 40);//airport_list.size());
	  size = (*customers_per_airport[partition_id-1])[some_ap-1];
	}
	int64_t cid = (some_ap << 48) | RandomNumber(r, 1, size);
	int64_t rid = rid_base++;
	reservation_entry* toinsert = new reservation_entry();
	toinsert->customer_id = cid;
	toinsert->flight_id = f_id;
	toinsert->seat_num = i;
	toinsert->r_id = rid;
	//if(RandomNumber(r, 1, 100) < OPENSEATS_ADD_PERCENT){
	//if(insert_queue.size() < CACHE_LIMIT_PENDING_INSERTS)
	//insert_queue.push(toinsert);
	//}
      }
    }
    //sfot += rdtsc() - st;
    //sfoc++;
    return txn_result(txn_ret, 0);
  }catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    txn_ret = false;
  }
  //sfot += rdtsc() - st;
  //sfoc++;
  return txn_result(txn_ret, 0);

}


  seats_worker::txn_result
seats_worker::txn_new_reservation()
{
  //uint64_t st = rdtsc();
  scoped_str_arena s_arena(arena);
  //int fsize = flight_list[partition_id-1]->size();
  void *txn = db->new_txn(txn_flags , arena, txn_buf(),abstract_db::HINT_SEATS_DEFAULT);
  db->init_txn(txn,cgraph,new_reservation_type);
  bool txn_ret = false;
  try {
    /*reservation_entry *reserv = insert_queue.front();
      if (reserv == NULL||insert_queue.size()==0) printf("Nothing to insert!\n");*/
    //insert_queue.pop();

    //int64_t c_id = reserv->customer_id;
    //printf("c_id %ld \n",c_id);
    int64_t c_id = 0;
    if(RandomNumber(r, 1,100) < 90){
      int64_t some_ap = usenum; //RandomNumber(r, 1, 2);//airport_list.size());
      int size = usenum; //(*customers_per_airport[partition_id-1])[some_ap-1];

      c_id = (some_ap << 48) | RandomNumber(r, 1, size);
    }
    else{
      int64_t some_ap = RandomNumber(r, 1, airport_list.size());
      int size = (*customers_per_airport[partition_id-1])[some_ap-1];
      while(size==0){
	some_ap = RandomNumber(r, 1, airport_list.size());
	size = (*customers_per_airport[partition_id-1])[some_ap-1];
      }

      c_id = (some_ap << 48) | RandomNumber(r, 1, size);
    }

    //int findex = RandomNumber(r, 1, fsize) - 1;
    int64_t f_id = findex + 1;//reserv->flight_id;
    int64_t r_id = rid_base++;//reserv->r_id;

    int seatnum = RandomNumber(r, 1, FLIGHTS_NUM_SEATS);//reserv->seat_num;
    float price = 2.0 * RandomNumber(r, RESERVATION_PRICE_MIN, RESERVATION_PRICE_MAX);
    int64_t attributes[9];
    for (int i=0; i< 9; i++) {
      attributes[i] = RandomNumber(r, 1, 100000);
    }
    int64_t al_id;

new_piece_1:

    try{
      db->one_op_begin(txn);


      const seats_customer::key k_c(partition_id, c_id);
      customerIndex++;
      if (customerIndex>=CONFLICT_WINDOW_1)
          customerIndex=0;
      int64_t fake_c_id = conflict_list[customerIndex];
      const seats_customer::key fake_k_c(partition_id,fake_c_id);
      bool found = tbl_seats_customer(partition_id)->get(txn, Encode(obj_key0, fake_k_c), obj_v);
      if (!found) printf("cid %ld not found\n",c_id);
      seats_customer::value v_c_temp;
      const seats_customer::value *v_c = Decode(obj_v, v_c_temp);


      seats_customer::value v_c_new(*v_c);
      v_c_new.c_iattr12 = attributes[0];
      v_c_new.c_iattr13 = attributes[1];
      v_c_new.c_iattr14 = attributes[2];
      v_c_new.c_iattr15 = attributes[3];
      v_c_new.c_iattr10++;
      v_c_new.c_iattr11++;
      tbl_seats_customer(partition_id)->put(txn, Encode(str(), fake_k_c), Encode(obj_v, v_c_new));

      if(!db->one_op_end(txn))
          goto new_piece_1;
    }
    catch(piece_abort_exception &ex){
      db->atomic_ops_abort(txn);
      goto new_piece_1;
    }




new_piece_2:

    try{
      db->one_op_begin(txn);
      const flight::key k_f(partition_id, f_id);
      flightIndex++;
      if (flightIndex>=CONFLICT_WINDOW_2)
          flightIndex=0;
      int64_t fake_f_id = conflict_list_flight[flightIndex];
      const flight::key fake_k_f(partition_id, fake_f_id);

      ALWAYS_ASSERT(tbl_flight(partition_id)->get(txn, Encode(obj_key0, fake_k_f), obj_v));
      flight::value v_f_temp;
      const flight::value *v_f = Decode(obj_v, v_f_temp);
      al_id = v_f->f_al_id;
      flight::value v_f_new(*v_f);
      v_f_new.f_seats_left--;
      tbl_flight(partition_id)->put(txn, Encode(str(), fake_k_f), Encode(obj_v, v_f_new));

      if(!db->one_op_end(txn))
	     goto new_piece_2;
    } catch(piece_abort_exception &ex){
      db->atomic_ops_abort(txn);
      goto new_piece_2;
    }

    const airline::key k_a(al_id);
    ALWAYS_ASSERT(tbl_airline(partition_id)->get(txn, Encode(obj_key0, k_a), obj_v));
    airline::value v_a_temp;


 new_piece_3:
   bool new_piece_3_success = false;
   try{
    do{
      new_piece_3_success = false;
      db->mul_ops_begin(txn);


      const reservation_seat_idx::key k_ri_0(partition_id, f_id, seatnum, 0);
      const reservation_seat_idx::key k_ri_1(partition_id, f_id, seatnum, numeric_limits<int64_t>::max());
      reservation_seat_idx_callback callback;
      tbl_reservation_seat_idx(partition_id)->scan(txn,Encode(obj_key0, k_ri_0), &Encode(obj_key1, k_ri_1), callback, s_arena.get());
      const reservation_seat_idx::key *k_ri = callback.get_key();
      if(k_ri != NULL){
  //txn_ret = db->commit_txn(txn);
       //db->abort_txn(txn);
       //txn_ret = false;
        if(!db->mul_ops_end(txn))
            goto new_piece_3;
  //insert_queue.pop();
       newseatdrop++;
  //seats_retry = false;
       findex = (findex+partnum)%fsize;
       break;
  //snewt += rdtsc() - st;
  //snewc++;
       //return txn_result(txn_ret, 0);
    }

      const reservation::key k_r_0(partition_id, f_id, c_id, 0);
      const reservation::key k_r_1(partition_id, f_id, c_id, numeric_limits<int64_t>::max());
      reservation_callback r_callback;
      tbl_reservation(partition_id)->scan(txn,Encode(obj_key0, k_r_0), &Encode(obj_key1, k_r_1), r_callback, s_arena.get());
      const reservation::key *k_r = r_callback.get_key();
      if(k_r != NULL){
  //txn_ret = db->commit_txn(txn);
      //db->abort_txn(txn);
      //txn_ret = false;
  //insert_queue.pop();
      if(!db->mul_ops_end(txn))
         goto new_piece_3;
      newcustdrop++;
  //seats_retry = false;
      findex = (findex+partnum)%fsize;
  //snewt += rdtsc() - st;
  //snewc++;
       break;
      //return txn_result(txn_ret, 0);
     }


      const reservation::key key_r(partition_id, f_id, c_id, r_id);
      reservation::value val_r;
      val_r.r_seat = seatnum;
      val_r.r_price = price;
      val_r.r_iattr00 = attributes[0];
      val_r.r_iattr01 = attributes[1];
      val_r.r_iattr02 = attributes[2];
      val_r.r_iattr03 = attributes[3];
      val_r.r_iattr04 = attributes[4];
      val_r.r_iattr05 = attributes[5];
      val_r.r_iattr06 = attributes[6];
      val_r.r_iattr07 = attributes[7];
      val_r.r_iattr08 = attributes[8];
      tbl_reservation(partition_id)->insert(txn, Encode(str(), key_r), Encode(obj_v, val_r));

      reservation_seat_idx::key key_ri;
      key_ri.r_partition = partition_id;
      key_ri.r_f_id = f_id;
      key_ri.r_seat = seatnum;
      key_ri.r_id = r_id;
      reservation_seat_idx::value val_ri;
      tbl_reservation_seat_idx(partition_id)->insert(txn, Encode(str(), key_ri), Encode(obj_v, val_ri));

         if(!db->mul_ops_end(txn))
            goto new_piece_3;
          new_piece_3_success = true;
        } while(false);
       } catch (piece_abort_exception &ex){
          db->atomic_ops_abort(txn);
          goto new_piece_3;
       }




new_piece_4:
    try{
      db->one_op_begin(txn);
      if (new_piece_3_success){
      const frequent_flyer::key k_ff(partition_id, c_id, al_id);
      bool ffound = tbl_frequent_flyer(partition_id)->get(txn, Encode(obj_key0, k_ff), obj_v);
      if(ffound){
	     frequent_flyer::value v_ff_temp;
	     const frequent_flyer::value *v_ff = Decode(obj_v, v_ff_temp);
	     frequent_flyer::value v_ff_new(*v_ff);
	     v_ff_new.ff_iattr10++;
	     v_ff_new.ff_iattr11 = attributes[4];
	     v_ff_new.ff_iattr12 = attributes[5];
	     v_ff_new.ff_iattr13 = attributes[6];
	     v_ff_new.ff_iattr14 = attributes[7];
	     tbl_frequent_flyer(partition_id)->put(txn, Encode(str(), k_ff), Encode(obj_v, v_ff_new));
      }
    }

      if(!db->one_op_end(txn))
	       goto new_piece_4;
    }
    catch(piece_abort_exception &ex){
      db->atomic_ops_abort(txn);
      goto new_piece_4;
    }


    txn_ret = db->commit_txn(txn);
    if(txn_ret){
      //insert_queue.pop();
      findex = (findex+partnum)%fsize;
      reservation_entry* reserv = new reservation_entry(f_id, c_id, r_id, seatnum);
      delete_queue.push(reserv);
      count++;
      //printf("count is %d\n",count);
    }
    //snewt += rdtsc() - st;
    //snewc++;
    //seats_retry = true;
    return txn_result(txn_ret, 0);
  }catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    txn_ret = false;
  }
  //snewt += rdtsc() - st;
  //snewc++;
  //seats_retry = true;
  return txn_result(txn_ret, 0);

}


  seats_worker::txn_result
seats_worker::txn_update_customer()
{
  //uint64_t st = rdtsc();
  //return txn_result(true, 0);
  scoped_str_arena s_arena(arena);
  int64_t c_id = 0;
  if(RandomNumber(r, 1,100) < 90){

    int64_t some_ap = usenum; //RandomNumber(r, 1, 2);//airport_list.size());
    int size = usenum;//(*customers_per_airport[partition_id-1])[some_ap-1];

    c_id = (some_ap << 48) | RandomNumber(r, 1, size);
  }
  else{
    int64_t some_ap = RandomNumber(r, 1, airport_list.size());
    int size = (*customers_per_airport[partition_id-1])[some_ap-1];
    while(size==0){
      some_ap = RandomNumber(r, 1, airport_list.size());
      size = (*customers_per_airport[partition_id-1])[some_ap-1];
    }

    c_id = (some_ap << 48) | RandomNumber(r, 1, size);
  }

  uint64_t attr0 = RandomNumber(r, 0, 1000000);
  uint64_t attr1 = RandomNumber(r, 0, 1000000);

  void *txn = db->new_txn(txn_flags , arena, txn_buf(),abstract_db::HINT_SEATS_DEFAULT);
  db->init_txn(txn,cgraph,update_customer_type);
  bool txn_ret = false;
  try {


    if (RandomNumber(r, 1, 100) <= PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
      char id_buf[64];

      sprintf(id_buf, "%ld", c_id);
      seats_customer_name_idx::key k_c;
      k_c.c_partition = partition_id;
      k_c.c_id_str.assign(id_buf);

      tbl_seats_customer_name_idx(partition_id)->get(txn, Encode(obj_key0, k_c), obj_v);

      seats_customer_name_idx::value v_c_temp;
      const seats_customer_name_idx::value* v_c = Decode(obj_v, v_c_temp);
      ALWAYS_ASSERT(v_c->c_id == c_id);
      //	printf("custidx %d %ld %ld\n", partition_id, c_id,v_c->c_id);
    }


    uint64_t airport_id;

update_customer_piece_1:

    try{
      db->one_op_begin(txn);
      seats_customer::key k_c;
      k_c.c_partition = partition_id;
      k_c.c_id = c_id;
      customerIndex++;
      if (customerIndex>=CONFLICT_WINDOW_1)
          customerIndex=0;
      int64_t fake_c_id = conflict_list[customerIndex];
      const seats_customer::key fake_k_c(partition_id,fake_c_id);
      bool found = tbl_seats_customer(partition_id)->get(txn, Encode(obj_key0, fake_k_c), obj_v);
      if (!found){
	     printf("cust %d %ld\n", partition_id, c_id);
	     ALWAYS_ASSERT(false);
	     frequent_flyer_callback callback;
	     const frequent_flyer::key k_ff_0(partition_id, c_id, 0);
	     const frequent_flyer::key k_ff_1(partition_id, c_id, numeric_limits<int64_t>::max());
	     tbl_frequent_flyer(partition_id)->scan(txn, Encode(obj_key0, k_ff_0), &Encode(obj_key1, k_ff_1), callback, s_arena.get());
	     for (uint i=0; i<callback.k_ff.size(); i++) {
	       frequent_flyer::value v_ff_new(*(callback.v_ff[i]));
	       v_ff_new.ff_iattr00 = attr0;
	       v_ff_new.ff_iattr01 = attr1;
	       tbl_frequent_flyer(partition_id)->put(txn, Encode(str(), *(callback.k_ff[i])), Encode(obj_v, v_ff_new));
	      }
      }
      if(!db->mul_ops_end(txn))
	goto update_customer_piece_2;
    } catch(piece_abort_exception &ex){
      db->atomic_ops_abort(txn);
      goto update_customer_piece_2;
    }





    txn_ret = db->commit_txn(txn);
    //supct += rdtsc() - st;
    //supcc++;
    return txn_result(txn_ret, 0);


  }catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    txn_ret = false;
  }
  //supct += rdtsc() - st;
  //supcc++;
  return txn_result(txn_ret, 0);

}

  seats_worker::txn_result
seats_worker::txn_update_reservation()
{
  //uint64_t st = rdtsc();
  //return txn_result(true, 0);
  scoped_str_arena s_arena(arena);
  void *txn = db->new_txn(txn_flags , arena, txn_buf(),abstract_db::HINT_SEATS_DEFAULT);
  db->init_txn(txn,cgraph,update_reservation_type);
  bool txn_ret = false;
  if (delete_queue.size()<=0)
    return txn_result(true,0);
  try {

update_reservation_piece_1:
    try{

      do{

      db->mul_ops_begin(txn);
      reservation_entry *reserv = delete_queue.front();
      delete_queue.pop();
      delete_queue.push(reserv);
      if (reserv == NULL) printf("Nothing to update!\n");
      int64_t attr_value = RandomNumber(r, 1, (1 << 20));
      int attr_idx = RandomNumber(r , 1, 4);
      int64_t seatnum = RandomNumber(r, 0, FLIGHTS_NUM_SEATS-1);
      int64_t f_id = reserv->flight_id;
      int64_t c_id = reserv->customer_id;

      const reservation_seat_idx::key k_ri_0(partition_id, f_id, seatnum, 0);
      const reservation_seat_idx::key k_ri_1(partition_id, f_id, seatnum, numeric_limits<int64_t>::max());

      reservation_seat_idx_callback callback;
      tbl_reservation_seat_idx(partition_id)->scan(txn,Encode(obj_key0, k_ri_0), &Encode(obj_key1, k_ri_1), callback, s_arena.get());
      const reservation_seat_idx::key *k_ri = callback.get_key();
      if (k_ri){
	      //db->abort_txn(txn);
	      //txn_ret = false;
	      //urcon++;
        if(!db->mul_ops_end(txn))
          goto update_reservation_piece_1;
        break;
	//seats_retry = false;
	//txn_ret = db->commit_txn(txn);
	       //return txn_result(txn_ret, 0);
      }
      else{
	     const reservation::key k_r_0(partition_id, f_id, c_id, 0);
	     const reservation::key k_r_1(partition_id, f_id, c_id, numeric_limits<int64_t>::max());
	     reservation_callback r_callback;
	     tbl_reservation(partition_id)->scan(txn,Encode(obj_key0, k_r_0), &Encode(obj_key1, k_r_1), r_callback, s_arena.get());
	     const reservation::key *k_r = r_callback.get_key();
	     const reservation::value *v_r = r_callback.get_value();
	     if (!k_r){
	  // printf("null exception\n");
	      // db->abort_txn(txn);
	      // txn_ret=false;
	       //urcon++;
	  // seats_retry=false;
        if(!db->mul_ops_end(txn))
          goto update_reservation_piece_1;
        break;
	  //return txn_result(txn_ret,0);
	    }
	ALWAYS_ASSERT(k_r->r_id == reserv->r_id);

	reservation::value v_r_new(*v_r);
	v_r_new.r_seat = seatnum;
	if (attr_idx == 1) v_r_new.r_iattr00 = attr_value;
	else if (attr_idx == 2) v_r_new.r_iattr01 = attr_value;
	else if (attr_idx == 3) v_r_new.r_iattr02 = attr_value;
	else v_r_new.r_iattr03 = attr_value;
	tbl_reservation(partition_id)->put(txn, Encode(str(), *k_r), Encode(str(), v_r_new));


	reservation_seat_idx::key key_ri;
	key_ri.r_partition = partition_id;
	key_ri.r_f_id = f_id;
	key_ri.r_seat = seatnum;
	key_ri.r_id = k_r->r_id;
	reservation_seat_idx::value val_ri;
	tbl_reservation_seat_idx(partition_id)->insert(txn, Encode(str(), key_ri), Encode(str(), val_ri));

	reservation_seat_idx::key k_ri;
	k_ri.r_partition = partition_id;
	k_ri.r_f_id = f_id;
	k_ri.r_seat = v_r->r_seat;
	k_ri.r_id = k_r->r_id;
	reservation_seat_idx::value v_ri;
	tbl_reservation_seat_idx(partition_id)->remove(txn, Encode(str(), k_ri));
  }

      if(!db->mul_ops_end(txn))
      	goto update_reservation_piece_1;
    } while (false);
  }
    catch(piece_abort_exception &ex){
      db->atomic_ops_abort(txn);
      goto update_reservation_piece_1;
    }



    txn_ret = db->commit_txn(txn);
    //suprt += rdtsc() - st;
    //suprc++;
    //printf("succeed\n");
    //seats_retry = true;
    return txn_result(txn_ret, 0);

  }catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    txn_ret = false;
  }
  //suprt += rdtsc() - st;
  //suprc++;
  //seats_retry = true;
  return txn_result(txn_ret, 0);

}


class seats_bench_runner : public bench_runner {

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
    seats_bench_runner(abstract_db *db)
      :bench_runner(db)
    {
#define OPEN_TABLESPACE_X(x) \
      partitions[#x] = OpenTablesForTablespace(db, #x, sizeof(x));

      SEATS_TABLE_LIST(OPEN_TABLESPACE_X);
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
	ret.push_back(new seats_country_loader(9324, db, open_tables, partitions));
	ret.push_back(new seats_airport_loader(235443, db, open_tables, partitions));
	ret.push_back(new seats_airline_loader(89785943, db, open_tables, partitions));

	if (enable_parallel_loading) {
	  fast_random r(923587856425);
	  for (uint i = 1; i <= scale_factor; i++)
	    ret.push_back(new seats_customer_loader(r.next(), db, open_tables, partitions, i));
	}else{
	  ret.push_back(new seats_customer_loader(923587856425, db, open_tables, partitions, -1));
	}
#if 1
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
#endif

	return ret;
      }


    virtual vector<bench_worker *>
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
  int currentSeed = r.next();
	for (size_t i=0; i<nthreads; i++)
	  ret.push_back(
	      new seats_worker(
		blockstart + i,
		currentSeed, db, open_tables, partitions,
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
      }

  private:
    map<string, vector<abstract_ordered_index *>> partitions;

};

  void
seats_do_test(abstract_db *db, int argc, char **argv)
{
  optind = 1;
  loadCountry();
  loadAirport();
  loadAirline();
  loadFlightsPerAirport();
  cgraph = init_seats_cgraph();
  seats_bench_runner r(db);
  r.run();
}
#undef INVARIANT
#define INVARIANT(expr) ALWAYS_ASSERT(expr)
