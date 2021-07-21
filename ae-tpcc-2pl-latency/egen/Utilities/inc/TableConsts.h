/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
 */

/*
*   Table column length constants used by the loader and
*   transactions.
*/
#ifndef TABLE_CONSTS_H
#define TABLE_CONSTS_H

#include "EGenStandardTypes.h"

namespace TPCE
{

// ADDRESS / ZIP_CODE tables
const int cTOWN_len = 80;
const int cDIV_len  = 80;
const int cCODE_len = 12;

//ACCOUNT_PERMISSION table
const int cACL_len = 4;

//ADDRESS table
const int cAD_NAME_len  = 80;
const int cAD_LINE_len = 80;
const int cAD_TOWN_len  = cTOWN_len;
const int cAD_DIV_len = cDIV_len;   //state/provice abreviation
const int cAD_ZIP_len = cCODE_len;
const int cAD_CTRY_len = 80;

//CASH_TRANSACTION table
const int cCT_NAME_len = 100;

//CUSTOMER table
const int cL_NAME_len       = 25;
const int cF_NAME_len       = 20;
const int cM_NAME_len       = 1;
const int cDOB_len      = 30;
const int cTAX_ID_len       = 20;
const int cGNDR_len     = 1;
const int cCTRY_len = 3;
const int cAREA_len = 3;
const int cLOCAL_len    = 10;
const int cEXT_len  = 5;
const int cEMAIL_len    = 50;

//BROKER table
const int cB_NAME_len = cF_NAME_len + cM_NAME_len + cL_NAME_len + 3;    // two spaces and one period

//COMPANY table
const int cCO_NAME_len = 60;
const int cSP_RATE_len = 4;
const int cCEO_NAME_len = cF_NAME_len + cL_NAME_len + 1;        // one space
const int cCO_DESC_len = 150;
const int cCO_SP_RATE_len = 4;

//CUSTOMER_ACCOUNT table
const int cCA_NAME_len      = 50;

//EXCHANGE table
const int cEX_ID_len = 6;
const int cEX_NAME_len = 100;
const int cEX_DESC_len = 150;
//const int cEX_OPEN_len = 8;
//const int cEX_CLOSE_len = 8;

//HOLDING table
const int cH_BUY_DTS_len = 30;  //date of purchase

//INDUSTRY table
const int cIN_ID_len = 2;
const int cIN_NAME_len = 50;

//NEWS_ITEM table
const int cNI_HEADLINE_len = 80;
const int cNI_SUMMARY_len = 255;
const int cNI_ITEM_len = 100 * 1000;
const int cNI_SOURCE_len = 30;
const int cNI_AUTHOR_len = 30;

//SECURITY table
const int cS_NAME_len = 70;
const int cSYMBOL_len = 7 + 1 + 7;  // base + separator + extended
const int cS_ISSUE_len = 6;

//SETTLEMENT table
const int cSE_CASH_TYPE_len = 40;

//SECTOR table
const int cSC_NAME_len = 30;
const int cSC_ID_len = 2;

//STATUS_TYPE table
const int cST_ID_len = 4;
const int cST_NAME_len = 10;

//TAX RATE table
const int cTX_ID_len = 4;
const int cTX_NAME_len = 50;

//TRADE table
const int cEXEC_NAME_len = cF_NAME_len + cM_NAME_len + cL_NAME_len + 3; // two spaces and one extra

//TRADE_HISTORY table
const int cTH_ST_ID_len = cST_ID_len;

//TRADE TYPE table
const int cTT_ID_len = 3;
const int cTT_NAME_len = 12;

//ZIP_CODE table
const int cZC_TOWN_len = cTOWN_len;
const int cZC_DIV_len = cDIV_len;
const int cZC_CODE_len = cCODE_len;

// ADDRESS / ZIP_CODE tables
const int sm_cTOWN_len = cTOWN_len + 1;
const int sm_cDIV_len  = cDIV_len + 1;
const int sm_cCODE_len = cCODE_len + 1;

//ACCOUNT_PERMISSION table
const int sm_cACL_len = cACL_len + 1;

//ADDRESS table
const int sm_cAD_NAME_len  = cAD_NAME_len + 1;
const int sm_cAD_LINE_len = cAD_LINE_len + 1;
const int sm_cAD_TOWN_len  = cAD_TOWN_len + 1;
const int sm_cAD_DIV_len = cAD_DIV_len + 1;   //state/provice abreviation
const int sm_cAD_ZIP_len = cAD_ZIP_len + 1;
const int sm_cAD_CTRY_len = cAD_CTRY_len + 1;

//CASH_TRANSACTION table
const int sm_cCT_NAME_len = cCT_NAME_len + 1;

//CUSTOMER table
const int sm_cL_NAME_len       = cL_NAME_len + 1;
const int sm_cF_NAME_len       = cF_NAME_len + 1;
const int sm_cM_NAME_len       = cM_NAME_len + 1;
const int sm_cDOB_len      = cDOB_len + 1;
const int sm_cTAX_ID_len       = cTAX_ID_len + 1;
const int sm_cGNDR_len     = cGNDR_len + 1;
const int sm_cCTRY_len = cCTRY_len + 1;
const int sm_cAREA_len = cAREA_len + 1;
const int sm_cLOCAL_len    = cLOCAL_len + 1;
const int sm_cEXT_len  = cEXT_len + 1;
const int sm_cEMAIL_len    = cEMAIL_len + 1;

//BROKER table
const int sm_cB_NAME_len = cB_NAME_len + 1;    // two spaces and one period

//COMPANY table
const int sm_cCO_NAME_len = cCO_NAME_len + 1;
const int sm_cSP_RATE_len = cSP_RATE_len + 1;
const int sm_cCEO_NAME_len = cCEO_NAME_len + 1;        // one space
const int sm_cCO_DESC_len = cCO_DESC_len + 1;
const int sm_cCO_SP_RATE_len = cCO_SP_RATE_len + 1;

//CUSTOMER_ACCOUNT table
const int sm_cCA_NAME_len      = cCA_NAME_len + 1;

//EXCHANGE table
const int sm_cEX_ID_len = cEX_ID_len + 1;
const int sm_cEX_NAME_len = cEX_NAME_len + 1;
const int sm_cEX_DESC_len = cEX_DESC_len + 1;

//HOLDING table
const int sm_cH_BUY_DTS_len = cH_BUY_DTS_len + 1;  //date of purchase

//INDUSTRY table
const int sm_cIN_ID_len = cIN_ID_len + 1;
const int sm_cIN_NAME_len = cIN_NAME_len + 1;

//NEWS_ITEM table
const int sm_cNI_HEADLINE_len = cNI_HEADLINE_len + 1;
const int sm_cNI_SUMMARY_len = cNI_SUMMARY_len + 1;
const int sm_cNI_ITEM_len = cNI_ITEM_len + 1;
const int sm_cNI_SOURCE_len = cNI_SOURCE_len + 1;
const int sm_cNI_AUTHOR_len = cNI_AUTHOR_len + 1;

//SECURITY table
const int sm_cS_NAME_len = cS_NAME_len + 1;
const int sm_cSYMBOL_len = cSYMBOL_len + 1;  // base + separator + extended
const int sm_cS_ISSUE_len = cS_ISSUE_len + 1;

//SETTLEMENT table
const int sm_cSE_CASH_TYPE_len = cSE_CASH_TYPE_len + 1;

//SECTOR table
const int sm_cSC_NAME_len = cSC_NAME_len + 1;
const int sm_cSC_ID_len = cSC_ID_len + 1;

//STATUS_TYPE table
const int sm_cST_ID_len = cST_ID_len + 1;
const int sm_cST_NAME_len = cST_NAME_len + 1;

//TAX RATE table
const int sm_cTX_ID_len = cTX_ID_len + 1;
const int sm_cTX_NAME_len = cTX_NAME_len + 1;

//TRADE table
const int sm_cEXEC_NAME_len = cEXEC_NAME_len + 1; // two spaces and one extra

//TRADE_HISTORY table
const int sm_cTH_ST_ID_len = cTH_ST_ID_len + 1;

//TRADE TYPE table
const int sm_cTT_ID_len = cTT_ID_len + 1;
const int sm_cTT_NAME_len = cTT_NAME_len + 1;

//ZIP_CODE table
const int sm_cZC_TOWN_len = cZC_TOWN_len + 1;
const int sm_cZC_DIV_len = cZC_DIV_len + 1;
const int sm_cZC_CODE_len = cZC_CODE_len + 1;

}   // namespace TPCE

#endif //TABLE_CONSTS_H
