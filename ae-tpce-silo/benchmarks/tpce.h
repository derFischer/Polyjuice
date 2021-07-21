#ifndef _NDB_BENCH_TPCE_H_
#define _NDB_BENCH_TPCE_H_

#include "../record/encoder.h"
#include "../record/inline_str.h"
#include "../macros.h"
#include "../egen/all.h"

#define int_date_time uint64_t
#define bool2char(b) ((b) ? 't' : 'f')
#define char2bool(c) (((c) == 't') ? true : false)

template <unsigned int N>
using inline_str_fixed_tpce = inline_str_fixed<N, '\0'>;

#define ACCOUNT_PERMISSION_KEY_FIELDS(x, y) \
    x(TIdent, AP_CA_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cTAX_ID_len>, AP_TAX_ID)
#define ACCOUNT_PERMISSION_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cACL_len>, AP_ACL) \
    y(inline_str_fixed_tpce<TPCE::sm_cL_NAME_len>, AP_L_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cF_NAME_len>, AP_F_NAME)
DO_STRUCT(ACCOUNT_PERMISSION, ACCOUNT_PERMISSION_KEY_FIELDS, ACCOUNT_PERMISSION_VALUE_FIELDS);

#define ADDRESS_KEY_FIELDS(x, y) \
    x(TIdent, AD_ID)
#define ADDRESS_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cAD_LINE_len>, AD_LINE1)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cAD_LINE_len>, AD_LINE2)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cAD_ZIP_len>, AD_ZC_CODE) \
    y(inline_str_fixed_tpce<TPCE::sm_cAD_CTRY_len>, AD_CTRY) /*D*/
DO_STRUCT(ADDRESS, ADDRESS_KEY_FIELDS, ADDRESS_VALUE_FIELDS);

#define BROKER_KEY_FIELDS(x, y) \
    x(TIdent, B_ID)
#define BROKER_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cST_ID_len>, B_ST_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cB_NAME_len>, B_NAME) \
    y(int32_t, B_NUM_TRADES) \
    y(float, B_COMM_TOTAL)
DO_STRUCT(BROKER, BROKER_KEY_FIELDS, BROKER_VALUE_FIELDS);

#define CASH_TRANSACTION_KEY_FIELDS(x, y) \
    x(TTrade, CT_T_ID)
#define CASH_TRANSACTION_VALUE_FIELDS(x, y) \
    x(int_date_time, CT_DTS)/*D*/ \
    y(float, CT_AMT)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cCT_NAME_len>, CT_NAME)
DO_STRUCT(CASH_TRANSACTION, CASH_TRANSACTION_KEY_FIELDS, CASH_TRANSACTION_VALUE_FIELDS);

#define CHARGE_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTT_ID_len>, CH_TT_ID) \
    y(int32_t, CH_C_TIER)
#define CHARGE_VALUE_FIELDS(x, y) \
    x(float, CH_CHRG)
DO_STRUCT(CHARGE, CHARGE_KEY_FIELDS, CHARGE_VALUE_FIELDS);

#define COMMISSION_RATE_KEY_FIELDS(x, y) \
    x(int32_t, CR_C_TIER) \
    y(inline_str_fixed_tpce<TPCE::sm_cTT_ID_len>, CR_TT_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cEX_ID_len>, CR_EX_ID) \
    y(int32_t, CR_FROM_QTY)
#define COMMISSION_RATE_VALUE_FIELDS(x, y) \
    x(int32_t, CR_TO_QTY) \
    y(float, CR_RATE)
DO_STRUCT(COMMISSION_RATE, COMMISSION_RATE_KEY_FIELDS, COMMISSION_RATE_VALUE_FIELDS);

#define COMPANY_KEY_FIELDS(x, y) \
    x(TIdent, CO_ID)
#define COMPANY_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cST_ID_len>, CO_ST_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cCO_NAME_len>, CO_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, CO_IN_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cSP_RATE_len>, CO_SP_RATE)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cCEO_NAME_len>, CO_CEO)/*D*/ \
    y(TIdent, CO_AD_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cCO_DESC_len>, CO_DESC)/*D*/ \
    y(int_date_time, CO_OPEN_DATE)
DO_STRUCT(COMPANY, COMPANY_KEY_FIELDS, COMPANY_VALUE_FIELDS);

#define COMPANY_COMPETITOR_KEY_FIELDS(x, y) \
    x(TIdent, CP_CO_ID) \
    y(TIdent, CP_COMP_CO_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, CP_IN_ID)
#define COMPANY_COMPETITOR_VALUE_FIELDS(x, y) \
    x(int32_t, dummy_value)
DO_STRUCT(COMPANY_COMPETITOR, COMPANY_COMPETITOR_KEY_FIELDS, COMPANY_COMPETITOR_VALUE_FIELDS);

#define CUSTOMER_KEY_FIELDS(x, y) \
    x(TIdent, C_ID)
#define CUSTOMER_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTAX_ID_len>, C_TAX_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cST_ID_len>, C_ST_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cL_NAME_len>, C_L_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cF_NAME_len>, C_F_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cM_NAME_len>, C_M_NAME)/*D*/ \
    y(char, C_GNDR)/*D*/ \
    y(char, C_TIER) \
    y(int_date_time, C_DOB)/*D*/ \
    y(TIdent, C_AD_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cCTRY_len>, C_CTRY_1)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cAREA_len>, C_AREA_1)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cLOCAL_len>, C_LOCAL_1)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cEXT_len>, C_EXT_1)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cCTRY_len>, C_CTRY_2)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cAREA_len>, C_AREA_2)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cLOCAL_len>, C_LOCAL_2)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cEXT_len>, C_EXT_2)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cCTRY_len>, C_CTRY_3)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cAREA_len>, C_AREA_3)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cLOCAL_len>, C_LOCAL_3)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cEXT_len>, C_EXT_3)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cEMAIL_len>, C_EMAIL_1)/*D*/ \
    y(inline_str_fixed_tpce<TPCE::sm_cEMAIL_len>, C_EMAIL_2)/*D*/
DO_STRUCT(CUSTOMER, CUSTOMER_KEY_FIELDS, CUSTOMER_VALUE_FIELDS);

#define CUSTOMER_ACCOUNT_KEY_FIELDS(x, y) \
    x(TIdent, CA_ID)
#define CUSTOMER_ACCOUNT_VALUE_FIELDS(x, y) \
    x(TIdent, CA_B_ID) \
    y(TIdent, CA_C_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cCA_NAME_len>, CA_NAME)/*D*/ \
    y(char, CA_TAX_ST) \
    y(float, CA_BAL)
DO_STRUCT(CUSTOMER_ACCOUNT, CUSTOMER_ACCOUNT_KEY_FIELDS, CUSTOMER_ACCOUNT_VALUE_FIELDS);

#define CUSTOMER_TAXRATE_KEY_FIELDS(x, y) \
    x(TIdent, CX_C_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cTX_ID_len>, CX_TX_ID)
#define CUSTOMER_TAXRATE_VALUE_FIELDS(x, y) \
    x(int32_t, dummy_value)
DO_STRUCT(CUSTOMER_TAXRATE, CUSTOMER_TAXRATE_KEY_FIELDS, CUSTOMER_TAXRATE_VALUE_FIELDS);

#define DAILY_MARKET_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, DM_S_SYMB) \
    y(int_date_time, DM_DATE)
#define DAILY_MARKET_VALUE_FIELDS(x, y) \
    x(float, DM_CLOSE) \
    y(float, DM_HIGH) \
    y(float, DM_LOW) \
    y(int64_t, DM_VOL)
DO_STRUCT(DAILY_MARKET, DAILY_MARKET_KEY_FIELDS, DAILY_MARKET_VALUE_FIELDS);

#define EXCHANGE_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cEX_ID_len>, EX_ID)
#define EXCHANGE_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cEX_NAME_len>, EX_NAME)/*D*/ \
    y(int32_t, EX_NUM_SYMB) \
    y(int32_t, EX_OPEN) \
    y(int32_t, EX_CLOSE) \
    y(inline_str_fixed_tpce<TPCE::sm_cEX_DESC_len>, EX_DESC)/*D*/ \
    y(TIdent, EX_AD_ID)
DO_STRUCT(EXCHANGE, EXCHANGE_KEY_FIELDS, EXCHANGE_VALUE_FIELDS);

#define FINANCIAL_KEY_FIELDS(x, y) \
    x(TIdent, FI_CO_ID) \
    y(int32_t, FI_YEAR) \
    y(int32_t, FI_QTR)
#define FINANCIAL_VALUE_FIELDS(x, y) \
    x(int_date_time, FI_QTR_START_DATE) \
    y(float, FI_REVENUE) \
    y(float, FI_NET_EARN) \
    y(float, FI_BASIC_EPS) \
    y(float, FI_DILUT_EPS) \
    y(float, FI_MARGIN) \
    y(float, FI_INVENTORY) \
    y(float, FI_ASSETS) \
    y(float, FI_LIABILITY) \
    y(int64_t, FI_OUT_BASIC) \
    y(int64_t, FI_OUT_DILUT)
DO_STRUCT(FINANCIAL, FINANCIAL_KEY_FIELDS, FINANCIAL_VALUE_FIELDS);

#define HOLDING_KEY_FIELDS(x, y) \
    x(TTrade, H_T_ID)
#define HOLDING_VALUE_FIELDS(x, y) \
    x(TIdent, H_CA_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, H_S_SYMB) \
    y(int_date_time, H_DTS) \
    y(float, H_PRICE) \
    y(int32_t, H_QTY)
DO_STRUCT(HOLDING, HOLDING_KEY_FIELDS, HOLDING_VALUE_FIELDS);

#define HOLDING_HISTORY_KEY_FIELDS(x, y) \
    x(TTrade, HH_T_ID) \
    y(TTrade, HH_H_T_ID)
#define HOLDING_HISTORY_VALUE_FIELDS(x, y) \
    x(int32_t, HH_BEFORE_QTY) \
    y(int32_t, HH_AFTER_QTY)
DO_STRUCT(HOLDING_HISTORY, HOLDING_HISTORY_KEY_FIELDS, HOLDING_HISTORY_VALUE_FIELDS);
/*D*/

#define HOLDING_SUMMARY_KEY_FIELDS(x, y) \
    x(TIdent, HS_CA_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, HS_S_SYMB)
#define HOLDING_SUMMARY_VALUE_FIELDS(x, y) \
    x(int32_t, HS_QTY)
DO_STRUCT(HOLDING_SUMMARY, HOLDING_SUMMARY_KEY_FIELDS, HOLDING_SUMMARY_VALUE_FIELDS);

#define INDUSTRY_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, IN_ID)
#define INDUSTRY_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cIN_NAME_len>, IN_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cSC_ID_len>, IN_SC_ID)
DO_STRUCT(INDUSTRY, INDUSTRY_KEY_FIELDS, INDUSTRY_VALUE_FIELDS);

#define LAST_TRADE_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, LT_S_SYMB)
#define LAST_TRADE_VALUE_FIELDS(x, y) \
    x(int_date_time, LT_DTS) \
    y(float, LT_PRICE) \
    y(float, LT_OPEN_PRICE) \
    y(int64_t, LT_VOL)
DO_STRUCT(LAST_TRADE, LAST_TRADE_KEY_FIELDS, LAST_TRADE_VALUE_FIELDS);

#define NEWS_ITEM_KEY_FIELDS(x, y) \
    x(TIdent, NI_ID)
#define NEWS_ITEM_VALUE_FIELDS(x, y) \
    x(int32_t, dummy_value)
DO_STRUCT(NEWS_ITEM, NEWS_ITEM_KEY_FIELDS, NEWS_ITEM_VALUE_FIELDS);
//D readonly

#define NEWS_XREF_KEY_FIELDS(x, y) \
    x(TIdent, NX_CO_ID) \
    y(TIdent, NX_NI_ID)
#define NEWS_XREF_VALUE_FIELDS(x, y) \
    x(int32_t, dummy_value)
DO_STRUCT(NEWS_XREF, NEWS_XREF_KEY_FIELDS, NEWS_XREF_VALUE_FIELDS);

#define SECTOR_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSC_ID_len>, SC_ID)
#define SECTOR_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSC_NAME_len>, SC_NAME)
DO_STRUCT(SECTOR, SECTOR_KEY_FIELDS, SECTOR_VALUE_FIELDS);

#define SECURITY_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, S_SYMB)
#define SECURITY_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cS_ISSUE_len>, S_ISSUE) \
    y(inline_str_fixed_tpce<TPCE::sm_cST_ID_len>, S_ST_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cS_NAME_len>, S_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cEX_ID_len>, S_EX_ID) \
    y(TIdent, S_CO_ID) \
    y(int64_t, S_NUM_OUT) \
    y(int_date_time, S_START_DATE) \
    y(int_date_time, S_EXCH_DATE) \
    y(float, S_PE) \
    y(float, S_52WK_HIGH) \
    y(int_date_time, S_52WK_HIGH_DATE) \
    y(float, S_52WK_LOW) \
    y(int_date_time, S_52WK_LOW_DATE) \
    y(float, S_DIVIDEND) \
    y(float, S_YIELD)
DO_STRUCT(SECURITY, SECURITY_KEY_FIELDS, SECURITY_VALUE_FIELDS);

#define SETTLEMENT_KEY_FIELDS(x, y) \
    x(TTrade, SE_T_ID)
#define SETTLEMENT_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSE_CASH_TYPE_len>, SE_CASH_TYPE) \
    y(int_date_time, SE_CASH_DUE_DATE)/*D*/ \
    y(float, SE_AMT) /*D*/
DO_STRUCT(SETTLEMENT, SETTLEMENT_KEY_FIELDS, SETTLEMENT_VALUE_FIELDS);

#define STATUS_TYPE_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cST_ID_len>, ST_ID)
#define STATUS_TYPE_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cST_NAME_len>, ST_NAME) /*D*/
DO_STRUCT(STATUS_TYPE, STATUS_TYPE_KEY_FIELDS, STATUS_TYPE_VALUE_FIELDS);

#define TAX_RATE_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTX_ID_len>, TX_ID)
#define TAX_RATE_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTX_NAME_len>, TX_NAME)/*D*/ \
    y(float, TX_RATE)
DO_STRUCT(TAX_RATE, TAX_RATE_KEY_FIELDS, TAX_RATE_VALUE_FIELDS);

#define TRADE_KEY_FIELDS(x, y) \
    x(TTrade, T_ID)
#define TRADE_VALUE_FIELDS(x, y) \
    x(int_date_time, T_DTS) \
    y(inline_str_fixed_tpce<TPCE::sm_cST_ID_len>, T_ST_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cTT_ID_len>, T_TT_ID) \
    y(char, T_IS_CASH) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, T_S_SYMB) \
    y(int32_t, T_QTY) \
    y(float, T_BID_PRICE) \
    y(TIdent, T_CA_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cEXEC_NAME_len>, T_EXEC_NAME) \
    y(float, T_TRADE_PRICE) \
    y(float, T_CHRG) \
    y(float, T_COMM) \
    y(float, T_TAX) \
    y(char, T_LIFO)
DO_STRUCT(TRADE, TRADE_KEY_FIELDS, TRADE_VALUE_FIELDS);

#define TRADE_HISTORY_KEY_FIELDS(x, y) \
    x(TTrade, TH_T_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cST_ID_len>, TH_ST_ID)
#define TRADE_HISTORY_VALUE_FIELDS(x, y) \
    x(int_date_time, TH_DTS)
DO_STRUCT(TRADE_HISTORY, TRADE_HISTORY_KEY_FIELDS, TRADE_HISTORY_VALUE_FIELDS);

#define TRADE_REQUEST_KEY_FIELDS(x, y) \
    x(TTrade, TR_T_ID)
#define TRADE_REQUEST_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTT_ID_len>, TR_TT_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, TR_S_SYMB) \
    y(int32_t, TR_QTY) \
    y(float, TR_BID_PRICE) \
    y(TIdent, TR_B_ID)
DO_STRUCT(TRADE_REQUEST, TRADE_REQUEST_KEY_FIELDS, TRADE_REQUEST_VALUE_FIELDS);

#define TRADE_TYPE_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTT_ID_len>, TT_ID)
#define TRADE_TYPE_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTT_NAME_len>, TT_NAME)/*D*/ \
    y(char, TT_IS_SELL) \
    y(char, TT_IS_MRKT)
DO_STRUCT(TRADE_TYPE, TRADE_TYPE_KEY_FIELDS, TRADE_TYPE_VALUE_FIELDS);

#define WATCH_ITEM_KEY_FIELDS(x, y) \
    x(TIdent, WI_WL_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, WI_S_SYMB)
#define WATCH_ITEM_VALUE_FIELDS(x, y) \
    x(int32_t, dummy_value)
DO_STRUCT(WATCH_ITEM, WATCH_ITEM_KEY_FIELDS, WATCH_ITEM_VALUE_FIELDS);

#define WATCH_LIST_KEY_FIELDS(x, y) \
    x(TIdent, WL_ID)
#define WATCH_LIST_VALUE_FIELDS(x, y) \
    x(TIdent, WL_C_ID)
DO_STRUCT(WATCH_LIST, WATCH_LIST_KEY_FIELDS, WATCH_LIST_VALUE_FIELDS);

#define ZIP_CODE_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cZC_CODE_len>, ZC_CODE)
#define ZIP_CODE_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cZC_TOWN_len>, ZC_TOWN) \
    y(inline_str_fixed_tpce<TPCE::sm_cZC_DIV_len>, ZC_DIV)
DO_STRUCT(ZIP_CODE, ZIP_CODE_KEY_FIELDS, ZIP_CODE_VALUE_FIELDS);

// secondary index tables
#define BROKER_B_NAME_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cB_NAME_len>, B1_B_NAME) \
    y(TIdent, B1_B_ID_K)
#define BROKER_B_NAME_VALUE_FIELDS(x, y) \
    x(TIdent, B1_B_ID_V)
DO_STRUCT(BROKER_B_NAME, BROKER_B_NAME_KEY_FIELDS, BROKER_B_NAME_VALUE_FIELDS);


#define COMPANY_CO_IN_ID_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, CO1_CO_IN_ID) \
    y(TIdent, CO1_CO_ID_K)
#define COMPANY_CO_IN_ID_VALUE_FIELDS(x, y) \
    x(TIdent, CO1_CO_ID_V)
DO_STRUCT(COMPANY_CO_IN_ID, COMPANY_CO_IN_ID_KEY_FIELDS, COMPANY_CO_IN_ID_VALUE_FIELDS);

#define COMPANY_CO_NAME_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cCO_NAME_len>, CO2_CO_NAME) \
    y(TIdent, CO2_CO_ID_K)
#define COMPANY_CO_NAME_VALUE_FIELDS(x, y) \
    x(TIdent, CO2_CO_ID_V)
DO_STRUCT(COMPANY_CO_NAME, COMPANY_CO_NAME_KEY_FIELDS, COMPANY_CO_NAME_VALUE_FIELDS);

#define CUSTOMER_C_TAX_ID_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cTAX_ID_len>, C1_C_TAX_ID) \
    y(TIdent, C1_C_ID_K)
#define CUSTOMER_C_TAX_ID_VALUE_FIELDS(x, y) \
    x(TIdent, C1_C_ID_V)
DO_STRUCT(CUSTOMER_C_TAX_ID, CUSTOMER_C_TAX_ID_KEY_FIELDS, CUSTOMER_C_TAX_ID_VALUE_FIELDS);

#define CUSTOMER_ACCOUNT_CA_C_ID_KEY_FIELDS(x, y) \
    x(TIdent, CA1_CA_C_ID) \
    y(TIdent, CA1_CA_ID_K)
#define CUSTOMER_ACCOUNT_CA_C_ID_VALUE_FIELDS(x, y) \
    x(TIdent, CA1_CA_ID_V)
DO_STRUCT(CUSTOMER_ACCOUNT_CA_C_ID, CUSTOMER_ACCOUNT_CA_C_ID_KEY_FIELDS, CUSTOMER_ACCOUNT_CA_C_ID_VALUE_FIELDS);

#define HOLDING_H_CA_ID_KEY_FIELDS(x, y) \
    x(TIdent, H1_H_CA_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, H1_H_S_SYMB) \
    y(TTrade, H1_H_T_ID_K)
#define HOLDING_H_CA_ID_VALUE_FIELDS(x, y) \
    x(TTrade, H1_H_T_ID_V)
DO_STRUCT(HOLDING_H_CA_ID, HOLDING_H_CA_ID_KEY_FIELDS, HOLDING_H_CA_ID_VALUE_FIELDS);

#define INDUSTRY_SC_ID_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSC_ID_len>, IN1_IN_SC_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, IN1_IN_ID_K)
#define INDUSTRY_SC_ID_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, IN1_IN_ID_V)
DO_STRUCT(INDUSTRY_SC_ID, INDUSTRY_SC_ID_KEY_FIELDS, INDUSTRY_SC_ID_VALUE_FIELDS);

#define INDUSTRY_IN_NAME_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cIN_NAME_len>, IN2_IN_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, IN2_IN_ID_K)
#define INDUSTRY_IN_NAME_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cIN_ID_len>, IN2_IN_ID_V)
DO_STRUCT(INDUSTRY_IN_NAME, INDUSTRY_IN_NAME_KEY_FIELDS, INDUSTRY_IN_NAME_VALUE_FIELDS);

#define SECTOR_SC_NAME_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSC_NAME_len>, SC1_SC_NAME) \
    y(inline_str_fixed_tpce<TPCE::sm_cSC_ID_len>, SC1_SC_ID_K)
#define SECTOR_SC_NAME_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSC_ID_len>, SC1_SC_ID_V)
DO_STRUCT(SECTOR_SC_NAME, SECTOR_SC_NAME_KEY_FIELDS, SECTOR_SC_NAME_VALUE_FIELDS);

#define SECURITY_S_CO_ID_KEY_FIELDS(x, y) \
    x(TIdent, S1_S_CO_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cS_ISSUE_len>, S1_S_ISSUE) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, S1_S_SYMB_K)
#define SECURITY_S_CO_ID_VALUE_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, S1_S_SYMB_V)
DO_STRUCT(SECURITY_S_CO_ID, SECURITY_S_CO_ID_KEY_FIELDS, SECURITY_S_CO_ID_VALUE_FIELDS);

#define TRADE_T_CA_ID_KEY_FIELDS(x, y) \
    x(TIdent, T1_T_CA_ID) \
    y(int_date_time, T1_T_DTS) \
    y(TTrade, T1_T_ID_K)
#define TRADE_T_CA_ID_VALUE_FIELDS(x, y) \
    x(TTrade, T1_T_ID_V)
DO_STRUCT(TRADE_T_CA_ID, TRADE_T_CA_ID_KEY_FIELDS, TRADE_T_CA_ID_VALUE_FIELDS);

#define TRADE_T_S_SYMB_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, T2_T_S_SYMB) \
    y(int_date_time, T2_T_DTS) \
    y(TTrade, T2_T_ID_K)
#define TRADE_T_S_SYMB_VALUE_FIELDS(x, y) \
    x(TTrade, T2_T_ID_V)
DO_STRUCT(TRADE_T_S_SYMB, TRADE_T_S_SYMB_KEY_FIELDS, TRADE_T_S_SYMB_VALUE_FIELDS);

#define TRADE_REQUEST_TR_B_ID_KEY_FIELDS(x, y) \
    x(TIdent, TR1_TR_B_ID) \
    y(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, TR1_TR_S_SYMB) \
    y(TTrade, TR1_TR_T_ID_K)
#define TRADE_REQUEST_TR_B_ID_VALUE_FIELDS(x, y) \
    x(TTrade, TR1_TR_T_ID_V)
DO_STRUCT(TRADE_REQUEST_TR_B_ID, TRADE_REQUEST_TR_B_ID_KEY_FIELDS, TRADE_REQUEST_TR_B_ID_VALUE_FIELDS);

//y(float, TR2_TR_BID_PRICE)
#define TRADE_REQUEST_TR_S_SYMB_KEY_FIELDS(x, y) \
    x(inline_str_fixed_tpce<TPCE::sm_cSYMBOL_len>, TR2_TR_S_SYMB) \
    y(inline_str_fixed_tpce<TPCE::sm_cTT_ID_len>, TR2_TR_TT_ID) \
    y(int64_t, TR2_TR_BID_PRICE) \
    y(TTrade, TR2_TR_T_ID_K)
#define TRADE_REQUEST_TR_S_SYMB_VALUE_FIELDS(x, y) \
    x(TTrade, TR2_TR_T_ID_V)
DO_STRUCT(TRADE_REQUEST_TR_S_SYMB, TRADE_REQUEST_TR_S_SYMB_KEY_FIELDS, TRADE_REQUEST_TR_S_SYMB_VALUE_FIELDS);

#define WATCH_LIST_WL_C_ID_KEY_FIELDS(x, y) \
    x(TIdent, WL1_WL_C_ID) \
    y(TIdent, WL1_WL_ID_K)
#define WATCH_LIST_WL_C_ID_VALUE_FIELDS(x, y) \
    x(TIdent, WL1_WL_ID_V)
DO_STRUCT(WATCH_LIST_WL_C_ID, WATCH_LIST_WL_C_ID_KEY_FIELDS, WATCH_LIST_WL_C_ID_VALUE_FIELDS);

#endif


