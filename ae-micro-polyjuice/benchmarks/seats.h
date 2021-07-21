#ifndef _NDB_BENCH_SEATS_H_
#define _NDB_BENCH_SEATS_H_

#include "../record/encoder.h"
#include "../record/inline_str.h"
#include "../macros.h"
#include "../tuple.h"
#include "bench.h"

#define COUNTRY_KEY_FIELDS(x,y) \
	x(int64_t, co_id) 
#define COUNTRY_VALUE_FIELDS(x, y) \
	x(inline_str_16<64>, co_name) \
	y(inline_str_8<2>, co_code_2) \
	y(inline_str_8<3>, co_code_3)
DO_STRUCT(country,COUNTRY_KEY_FIELDS,COUNTRY_VALUE_FIELDS);

#define AIRPORT_KEY_FIELDS(x,y) \
	x(int64_t, ap_id)
#define AIRPORT_VALUE_FIELDS(x,y) \
	x(inline_str_8<3>, ap_code) \
	y(inline_str_16<128>, ap_name) \
	y(inline_str_16<64>, ap_city) \
	y(inline_str_8<12>, ap_postal_code) \
	y(int64_t, ap_co_id) \
	y(float, ap_longitude) \
	y(float, ap_latitude) \
	y(float, ap_gmt_offset) \
	y(int64_t, ap_wac) \
	y(int64_t, ap_iattr00) \
	y(int64_t, ap_iattr01) \
	y(int64_t, ap_iattr02) \
	y(int64_t, ap_iattr03) \
	y(int64_t, ap_iattr04) \
	y(int64_t, ap_iattr05) \
	y(int64_t, ap_iattr06) \
	y(int64_t, ap_iattr07) \
	y(int64_t, ap_iattr08) \
	y(int64_t, ap_iattr09) \
	y(int64_t, ap_iattr10) \
	y(int64_t, ap_iattr11) \
	y(int64_t, ap_iattr12) \
	y(int64_t, ap_iattr13) \
	y(int64_t, ap_iattr14) \
	y(int64_t, ap_iattr15)
DO_STRUCT(airport, AIRPORT_KEY_FIELDS,AIRPORT_VALUE_FIELDS);

#define AIRLINE_KEY_FIELDS(x,y) \
	x(int64_t, al_id)
#define AIRLINE_VALUE_FIELDS(x,y) \
	x(inline_str_8<3>, al_iata_code) \
	y(inline_str_8<3>, al_icao_code) \
	y(inline_str_16<32>, al_call_sign) \
	y(inline_str_16<128>, al_name) \
	y(int64_t, al_co_id) \
	y(int64_t, al_iattr00) \
	y(int64_t, al_iattr01) \
	y(int64_t, al_iattr02) \
	y(int64_t, al_iattr03) \
	y(int64_t, al_iattr04) \
	y(int64_t, al_iattr05) \
	y(int64_t, al_iattr06) \
	y(int64_t, al_iattr07) \
	y(int64_t, al_iattr08) \
	y(int64_t, al_iattr09) \
	y(int64_t, al_iattr10) \
	y(int64_t, al_iattr11) \
	y(int64_t, al_iattr12) \
	y(int64_t, al_iattr13) \
	y(int64_t, al_iattr14) \
	y(int64_t, al_iattr15)
DO_STRUCT(airline, AIRLINE_KEY_FIELDS, AIRLINE_VALUE_FIELDS);

#define CUSTOMER_KEY_FIELDS(x,y) \
	x(int32_t, c_partition) \
	y(int64_t, c_id)
#define CUSTOMER_VALUE_FIELDS(x, y) \
	x(inline_str_16<64>, c_id_str) \
	y(int64_t, c_base_ap_id) \
	y(float, c_balance) \
	y(inline_str_16<32>, c_sattr00) \
	y(inline_str_8<8>, c_sattr01) \
	y(inline_str_8<8>, c_sattr02) \
	y(inline_str_8<8>, c_sattr03) \
	y(inline_str_8<8>, c_sattr04) \
	y(inline_str_8<8>, c_sattr05) \
	y(inline_str_8<8>, c_sattr06) \
	y(inline_str_8<8>, c_sattr07) \
	y(inline_str_8<8>, c_sattr08) \
	y(inline_str_8<8>, c_sattr09) \
	y(inline_str_8<8>, c_sattr10) \
	y(inline_str_8<8>, c_sattr11) \
	y(inline_str_8<8>, c_sattr12) \
	y(inline_str_8<8>, c_sattr13) \
	y(inline_str_8<8>, c_sattr14) \
	y(inline_str_8<8>, c_sattr15) \
	y(inline_str_8<8>, c_sattr16) \
	y(inline_str_8<8>, c_sattr17) \
	y(inline_str_8<8>, c_sattr18) \
	y(inline_str_8<8>, c_sattr19) \
	y(int64_t, c_iattr00) \
	y(int64_t, c_iattr01) \
	y(int64_t, c_iattr02) \
	y(int64_t, c_iattr03) \
	y(int64_t, c_iattr04) \
	y(int64_t, c_iattr05) \
	y(int64_t, c_iattr06) \
	y(int64_t, c_iattr07) \
	y(int64_t, c_iattr08) \
	y(int64_t, c_iattr09) \
	y(int64_t, c_iattr10) \
	y(int64_t, c_iattr11) \
	y(int64_t, c_iattr12) \
	y(int64_t, c_iattr13) \
	y(int64_t, c_iattr14) \
	y(int64_t, c_iattr15) \
	y(int64_t, c_iattr16) \
	y(int64_t, c_iattr17) \
	y(int64_t, c_iattr18) \
	y(int64_t, c_iattr19)
DO_STRUCT(seats_customer, CUSTOMER_KEY_FIELDS,CUSTOMER_VALUE_FIELDS);

#define CUSTOMER_NAME_IDX_KEY_FIELDS(x, y) \
	x(int32_t, c_partition) \
	y(inline_str_16<64>, c_id_str)
#define CUSTOMER_NAME_IDX_VALUE_FIELDS(x, y) \
	x(int64_t, c_id)
DO_STRUCT(seats_customer_name_idx, CUSTOMER_NAME_IDX_KEY_FIELDS, CUSTOMER_NAME_IDX_VALUE_FIELDS);
	
#define FFLYER_KEY_FIELDS(x, y) \
	x(int32_t, ff_partition) \
	y(int64_t, ff_c_id) \
	y(int64_t, ff_al_id)
#define FFLYER_VALUE_FIELDS(x,y) \
	x(inline_str_16<64>, ff_c_id_str) \
	y(inline_str_16<32>, ff_sattr00) \
	y(inline_str_16<32>, ff_sattr01) \
	y(inline_str_16<32>, ff_sattr02) \
	y(inline_str_16<32>, ff_sattr03) \
	y(int64_t, ff_iattr00) \
	y(int64_t, ff_iattr01) \
	y(int64_t, ff_iattr02) \
	y(int64_t, ff_iattr03) \
	y(int64_t, ff_iattr04) \
	y(int64_t, ff_iattr05) \
	y(int64_t, ff_iattr06) \
	y(int64_t, ff_iattr07) \
	y(int64_t, ff_iattr08) \
	y(int64_t, ff_iattr09) \
	y(int64_t, ff_iattr10) \
	y(int64_t, ff_iattr11) \
	y(int64_t, ff_iattr12) \
	y(int64_t, ff_iattr13) \
	y(int64_t, ff_iattr14) \
	y(int64_t, ff_iattr15)
DO_STRUCT(frequent_flyer, FFLYER_KEY_FIELDS, FFLYER_VALUE_FIELDS);

#define FFLYER_NAME_IDX_KEY_FIELDS(x, y) \
	x(int32_t, ff_partition) \
	y(inline_str_16<64>, ff_c_id_str) \
	y(int64_t, ff_c_id) \
	y(int64_t, ff_al_id)
#define FFLYER_NAME_IDX_VALUE_FIELDS(x, y) \
	x(int32_t, ff_dummy)
DO_STRUCT(frequent_flyer_name_idx, FFLYER_NAME_IDX_KEY_FIELDS, FFLYER_NAME_IDX_VALUE_FIELDS);

#define FLIGHT_KEY_FIELDS(x, y) \
	x(int32_t, f_partition) \
	y(int64_t, f_id)
#define FLIGHT_VALUE_FIELDS(x, y) \
	x(int64_t, f_al_id) \
	y(int64_t, f_depart_ap_id) \
	y(int64_t, f_depart_time) \
	y(int64_t, f_arrive_ap_id) \
	y(int64_t, f_arrive_time) \
	y(int64_t, f_status) \
	y(float, f_base_price) \
	y(int64_t, f_seats_total) \
	y(int64_t, f_seats_left) \
	y(int64_t, f_iattr00) \
	y(int64_t, f_iattr01) \
	y(int64_t, f_iattr02) \
	y(int64_t, f_iattr03) \
	y(int64_t, f_iattr04) \
	y(int64_t, f_iattr05) \
	y(int64_t, f_iattr06) \
	y(int64_t, f_iattr07) \
	y(int64_t, f_iattr08) \
	y(int64_t, f_iattr09) \
	y(int64_t, f_iattr10) \
	y(int64_t, f_iattr11) \
	y(int64_t, f_iattr12) \
	y(int64_t, f_iattr13) \
	y(int64_t, f_iattr14) \
	y(int64_t, f_iattr15) \
	y(int64_t, f_iattr16) \
	y(int64_t, f_iattr17) \
	y(int64_t, f_iattr18) \
	y(int64_t, f_iattr19) \
	y(int64_t, f_iattr20) \
	y(int64_t, f_iattr21) \
	y(int64_t, f_iattr22) \
	y(int64_t, f_iattr23) \
	y(int64_t, f_iattr24) \
	y(int64_t, f_iattr25) \
	y(int64_t, f_iattr26) \
	y(int64_t, f_iattr27) \
	y(int64_t, f_iattr28) \
	y(int64_t, f_iattr29)
DO_STRUCT(flight, FLIGHT_KEY_FIELDS, FLIGHT_VALUE_FIELDS);

#define FLIGHT_TIME_IDX_KEY_FIELDS(x, y) \
	x(int32_t, f_partition) \
	y(int64_t, f_depart_ap_id) \
	y(int64_t, f_arrive_ap_id) \
	y(int64_t, f_depart_time) \
	y(int64_t, f_id)
#define FLIGHT_TIME_IDX_VALUE_FIELDS(x, y) \
	x(int32_t, f_dummy)
DO_STRUCT(flight_time_idx, FLIGHT_TIME_IDX_KEY_FIELDS, FLIGHT_TIME_IDX_VALUE_FIELDS);

#define RESERVATION_KEY_FIELDS(x, y) \
	x(int32_t, r_partition) \
	y(int64_t, r_f_id) \
	y(int64_t, r_c_id) \
	y(int64_t, r_id)
#define RESERVATION_VALUE_FIELDS(x, y) \
	x(int64_t, r_seat) \
	y(float, r_price) \
	y(int64_t, r_iattr00) \
	y(int64_t, r_iattr01) \
	y(int64_t, r_iattr02) \
	y(int64_t, r_iattr03) \
	y(int64_t, r_iattr04) \
	y(int64_t, r_iattr05) \
	y(int64_t, r_iattr06) \
	y(int64_t, r_iattr07) \
	y(int64_t, r_iattr08)
DO_STRUCT(reservation, RESERVATION_KEY_FIELDS, RESERVATION_VALUE_FIELDS);

#define RESERVATION_SEAT_IDX_KEY_FIELDS(x, y) \
	x(int32_t, r_partition) \
	y(int64_t, r_f_id) \
	y(int64_t, r_seat) \
	y(int64_t, r_id)
#define RESERVATION_SEAT_IDX_VALUE_FIELDS(x, y)\
	x(int32_t, r_dummy)
DO_STRUCT(reservation_seat_idx, RESERVATION_SEAT_IDX_KEY_FIELDS, RESERVATION_SEAT_IDX_VALUE_FIELDS);

#define AIRPORT_DISTANCE_KEY_FIELDS(x, y) \
	x(int64_t, d_ap_id0) \
	y(int64_t, d_ap_id1)
#define AIRPORT_DISTANCE_VALUE_FIELDS(x, y)\
	x(float, d_distance)
DO_STRUCT(airport_distance, AIRPORT_DISTANCE_KEY_FIELDS, AIRPORT_DISTANCE_VALUE_FIELDS);

//only for readonly txn
template <size_t N>
class dstatic_limit_airport_distance_callback: public abstract_ordered_index::scan_callback {
public:
	dstatic_limit_airport_distance_callback(float distance_, bool ignore_key_)
	: n(0), ignore_key(ignore_key_), distance(distance_)
	{
		static_assert(N > 0, "xx");
	}
		
	~dstatic_limit_airport_distance_callback(){
		values.clear();
	}
		
	virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
	{
		INVARIANT(n < N);
		airport_distance::key k_ad_temp;
		const airport_distance::key *k_ad = Decode(keyp, k_ad_temp);
		
		airport_distance::value v_ad_temp;
		const airport_distance::value *v_ad = Decode(value, v_ad_temp);
		if(distance > 0 && v_ad->d_distance < distance){
			values.emplace_back(k_ad->d_ap_id1, v_ad->d_distance);
			return ++n < N;
		}
		else
			return true;
	}
		
	inline size_t
	size() const
	{
		return values.size();
	}
	
	typedef std::pair<int64_t, float> kv_pair;
	typename util::vec<kv_pair, N>::type values;
	
private:
	size_t n;
	bool ignore_key;
	float distance;

};

//only for readonly txn
template <size_t N>
class dstatic_limit_flight_callback: public abstract_ordered_index::scan_callback {
public:
	dstatic_limit_flight_callback(bool ignore_key_, int64_t start_time_, int64_t stop_time_)
	: n(0), ignore_key(ignore_key_), start_time(start_time_), stop_time(stop_time_)
	{
		static_assert(N > 0, "xx");
	}
		
	~dstatic_limit_flight_callback(){
		values.clear();
	}
		
	virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
	{
		INVARIANT(n < N);
		flight_time_idx::key k_ad_temp;
		const flight_time_idx::key *k_ad = Decode(keyp, k_ad_temp);
		
		if(k_ad->f_depart_time >= start_time && k_ad->f_depart_time <= stop_time){
			values.push_back(k_ad->f_id);
			return ++n < N;
		}
		else
			return true;
	}
		
	inline size_t
	size() const
	{
		return values.size();
	}
	
	std::vector<int64_t> values;
	
private:
	size_t n;
	bool ignore_key;
	int64_t start_time;
	int64_t stop_time;
};

//only for readonly txn
template <size_t N>
class dstatic_limit_reservation_seatnum_callback: public abstract_ordered_index::scan_callback {
public:
	dstatic_limit_reservation_seatnum_callback(bool ignore_key_)
	: n(0), ignore_key(ignore_key_)
	{
		static_assert(N > 0, "xx");
	}
		
	~dstatic_limit_reservation_seatnum_callback(){
		values.clear();
	}
		
	virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
	{
		INVARIANT(n < N);
		reservation::value v_r_temp;
		const reservation::value *v_r = Decode(value, v_r_temp);
		
		values.push_back(v_r->r_seat);
		return ++n < N;

	}
		
	inline size_t
	size() const
	{
		return values.size();
	}
	
	std::vector<int64_t> values;
	
private:
	size_t n;
	bool ignore_key;
};



class dscan_callback {
  public:
    ~dscan_callback() {}
	//void on_resp_node(const concurrent_btree::node_opaque_t *n, uint64_t version){}
    virtual bool invoke(const std::string& key,
                        uint8_t *value) = 0;
	virtual bool operator()(const std::string& key,
                        uint8_t *value) = 0;
};

class dfrequent_flyer_callback : public dscan_callback{
public:
	dfrequent_flyer_callback(){}

	bool invoke(const std::string& key,
	uint8_t *value)
	{
		frequent_flyer::key k_ff_temp;
		const frequent_flyer::key *k = Decode(key, k_ff_temp);
		k_ff.push_back(k);
		//dbtuple* t_ff = reinterpret_cast<dbtuple *>(value);
		//t_ffs.push_back(t_ff);
		return true;
	}
	bool operator()(const std::string& key, uint8_t *value)
  	{
  		frequent_flyer::key k_ff_temp;
		const frequent_flyer::key *k = Decode(key, k_ff_temp);
		k_ff.push_back(k);
		//dbtuple* t_ff = reinterpret_cast<dbtuple *>(value);
		//t_ffs.push_back(t_ff);
		return true;
	}

	std::vector<const frequent_flyer::key *> k_ff;
	//std::vector<dbtuple*> t_ffs;
};

class dff_name_idx_callback : public dscan_callback {
public:
	dff_name_idx_callback() {
		getKey = false;
	}
	
	bool invoke(const std::string& key,
	uint8_t *value)
	{
			k_c = Decode(key, k_c_temp);
			getKey = true;
			return false;
	}
	
	bool operator()(const std::string& key, uint8_t *value)
  	{
		k_c = Decode(key, k_c_temp);
		getKey = true;
		return false;
	
	}
	inline const frequent_flyer_name_idx::key *
	get_key() const
	{
		if(getKey)
			return k_c;
		else
			return NULL;
	}
	
	frequent_flyer_name_idx::key k_c_temp;
	bool getKey;
	const frequent_flyer_name_idx::key *k_c;
};

class reservation_count_callback : public dscan_callback{
public:
	reservation_count_callback():reserv_count(0) {}

	bool invoke(const std::string& key, uint8_t *value){
		reservation::key k_r;
		//const reservation::key *k = Decode(key, k_r);
		//if(k->r_f_id==76218)
		//	printf("r key %d %d %d\n", k->r_f_id, k->r_c_id, k->r_id);
		dbtuple * tuple = reinterpret_cast<dbtuple *>(value);
		if(!tuple->is_deleting())
			reserv_count++;
		return true;
	}
	
	bool operator()(const std::string& key, uint8_t *value){
		reservation::key k_r;
		//const reservation::key *k = Decode(key, k_r);
		//if(k->r_f_id==76218)
		//	printf("r key %d %d %d\n", k->r_f_id, k->r_c_id, k->r_id);
		dbtuple * tuple = reinterpret_cast<dbtuple *>(value);
		if(!tuple->is_deleting())
			reserv_count++;
		return true;
	}

	int count(){
		return reserv_count;
	}

private:
	int reserv_count;
};


#endif
