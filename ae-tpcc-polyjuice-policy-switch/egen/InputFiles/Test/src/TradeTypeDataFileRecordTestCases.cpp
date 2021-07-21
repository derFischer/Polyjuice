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
 * - Doug Johnson
 */

//
// Include this module's header first to make sure it is self-contained
//
#include "../inc/TradeTypeDataFileRecordTestCases.h"

//
// Include system headers
//
#include <stdlib.h>

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"
#include "../inc/Utilities.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    TradeTypeDataFileRecordTestCases::TradeTypeDataFileRecordTestCases()
        : dfr1( 0 )
        , tt_id("TMB")
        , tt_name("Market-Buy")
        , tt_is_sell("0")
        , tt_is_mrkt("1")
    {
        fields.push_back(tt_id);
        fields.push_back(tt_name);
        fields.push_back(tt_is_sell);
        fields.push_back(tt_is_mrkt);
    }

    TradeTypeDataFileRecordTestCases::~TradeTypeDataFileRecordTestCases()
    {
        CleanUp( &dfr1 );
    }

    //
    // Add test cases to the test suite.
    //
    void TradeTypeDataFileRecordTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< TradeTypeDataFileRecordTestCases > tester ) const
    {
        AddTestCase( "TradeTypeDataFileRecord: TestCase_DFRConstructor", &TestCase_DFRConstructor<TPCE::TradeTypeDataFileRecord>, fields, testSuite );
        AddTestCaseField<TPCE::TradeTypeDataFileRecord, const std::string&>( "TradeTypeDataFileRecord: TestCase_ST_ID", &TestCase_DFRField<TPCE::TradeTypeDataFileRecord, const std::string&>, fields, &TPCE::TradeTypeDataFileRecord::TT_ID, tt_id, testSuite );
        AddTestCaseField<TPCE::TradeTypeDataFileRecord, const std::string&>( "TradeTypeDataFileRecord: TestCase_ST_NAME", &TestCase_DFRField<TPCE::TradeTypeDataFileRecord, const std::string&>, fields, &TPCE::TradeTypeDataFileRecord::TT_NAME, tt_name, testSuite );
        // These next two are a bit ugly. Convert the string to an int to get implicit conversion to bool.
        AddTestCaseField<TPCE::TradeTypeDataFileRecord, bool>( "TradeTypeDataFileRecord: TestCase_TT_IS_SELL", &TestCase_DFRField<TPCE::TradeTypeDataFileRecord, bool>, fields, &TPCE::TradeTypeDataFileRecord::TT_IS_SELL, atoi(tt_is_sell.c_str()), testSuite );
        AddTestCaseField<TPCE::TradeTypeDataFileRecord, bool>( "TradeTypeDataFileRecord: TestCase_TT_IS_MRKT", &TestCase_DFRField<TPCE::TradeTypeDataFileRecord, bool>, fields, &TPCE::TradeTypeDataFileRecord::TT_IS_MRKT, atoi(tt_is_mrkt.c_str()), testSuite );
        AddTestCase( "TradeTypeDataFileRecord: TestCase_DFRToString", &TestCase_DFRMultiFieldToString<TPCE::TradeTypeDataFileRecord>, fields, testSuite );
    }

} // namespace EGenUtilitiesTest
