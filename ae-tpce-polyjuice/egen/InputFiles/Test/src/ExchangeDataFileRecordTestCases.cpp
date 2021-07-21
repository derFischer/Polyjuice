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
#include "../inc/ExchangeDataFileRecordTestCases.h"

//
// Include system headers
//
#include <stdlib.h>

//
// Include application headers
//
#include "../../../Utilities/inc/EGenStandardTypes.h"
#include "../../../Test/inc/TestUtilities.h"
#include "../inc/Utilities.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    ExchangeDataFileRecordTestCases::ExchangeDataFileRecordTestCases()
        : dfr1( 0 )
        , ex_id("NYSE")
        , ex_name("New York Stock Exchange")
        , ex_open("0930")
        , ex_close("1600")
        , ex_desc("Simulates the New York Stock Exchange")
        , ex_ad_id("1")
    {
        fields.push_back(ex_id);
        fields.push_back(ex_name);
        fields.push_back(ex_open);
        fields.push_back(ex_close);
        fields.push_back(ex_desc);
        fields.push_back(ex_ad_id);
    }

    ExchangeDataFileRecordTestCases::~ExchangeDataFileRecordTestCases()
    {
        CleanUp( &dfr1 );
    }

    //
    // Add test cases to the test suite.
    //
    void ExchangeDataFileRecordTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< ExchangeDataFileRecordTestCases > tester ) const
    {
        AddTestCase( "ExchangeDataFileRecord: TestCase_DFRConstructor", &TestCase_DFRConstructor<TPCE::ExchangeDataFileRecord>, fields, testSuite );
        AddTestCaseField<TPCE::ExchangeDataFileRecord, const std::string&>( "ExchangeDataFileRecord: TestCase_EX_ID", &TestCase_DFRField<TPCE::ExchangeDataFileRecord, const std::string&>, fields, &TPCE::ExchangeDataFileRecord::EX_ID, ex_id, testSuite );
        AddTestCaseField<TPCE::ExchangeDataFileRecord, const std::string&>( "ExchangeDataFileRecord: TestCase_EX_NAME", &TestCase_DFRField<TPCE::ExchangeDataFileRecord, const std::string&>, fields, &TPCE::ExchangeDataFileRecord::EX_NAME, ex_name, testSuite );
        AddTestCaseField<TPCE::ExchangeDataFileRecord, int>( "ExchangeDataFileRecord: TestCase_EX_OPEN", &TestCase_DFRField<TPCE::ExchangeDataFileRecord, int>, fields, &TPCE::ExchangeDataFileRecord::EX_OPEN, atoi(ex_open.c_str()), testSuite );
        AddTestCaseField<TPCE::ExchangeDataFileRecord, int>( "ExchangeDataFileRecord: TestCase_EX_CLOSE", &TestCase_DFRField<TPCE::ExchangeDataFileRecord, int>, fields, &TPCE::ExchangeDataFileRecord::EX_CLOSE, atoi(ex_close.c_str()), testSuite );
        AddTestCaseField<TPCE::ExchangeDataFileRecord, const std::string&>( "ExchangeDataFileRecord: TestCase_EX_DESC", &TestCase_DFRField<TPCE::ExchangeDataFileRecord, const std::string&>, fields, &TPCE::ExchangeDataFileRecord::EX_DESC, ex_desc, testSuite );
        AddTestCaseField<TPCE::ExchangeDataFileRecord, TIdent>( "ExchangeDataFileRecord: TestCase_EX_AD_ID", &TestCase_DFRField<TPCE::ExchangeDataFileRecord, TIdent>, fields, &TPCE::ExchangeDataFileRecord::EX_AD_ID, atoi(ex_ad_id.c_str()), testSuite );
        AddTestCase( "ExchangeDataFileRecord: TestCase_DFRToString", &TestCase_DFRMultiFieldToString<TPCE::ExchangeDataFileRecord>, fields, testSuite );
    }

} // namespace EGenUtilitiesTest
