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
#include "../inc/CommissionRateDataFileRecordTestCases.h"

//
// Include system headers
//
#include <stdexcept>
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
    CommissionRateDataFileRecordTestCases::CommissionRateDataFileRecordTestCases()
        : dfr1( 0 )
        , cr_c_tier("1")
        , cr_tt_id("TMB")
        , cr_ex_id("NYSE")
        , cr_from_qty("1")
        , cr_to_qty("200")
        , cr_rate("0.4")
    {
        fields.push_back(cr_c_tier);
        fields.push_back(cr_tt_id);
        fields.push_back(cr_ex_id);
        fields.push_back(cr_from_qty);
        fields.push_back(cr_to_qty);
        fields.push_back(cr_rate);
    }

    CommissionRateDataFileRecordTestCases::~CommissionRateDataFileRecordTestCases()
    {
        CleanUp( &dfr1 );
    }

    //
    // Add test cases to the test suite.
    //
    void CommissionRateDataFileRecordTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< CommissionRateDataFileRecordTestCases > tester ) const
    {
        AddTestCase( "CommissionRateDataFileRecord: TestCase_DFRConstructor", &TestCase_DFRConstructor<TPCE::CommissionRateDataFileRecord>, fields, testSuite );
        AddTestCaseField<TPCE::CommissionRateDataFileRecord, int>( "CommissionRateDataFileRecord: TestCase_CR_C_TIER", &TestCase_DFRField<TPCE::CommissionRateDataFileRecord, int>, fields, &TPCE::CommissionRateDataFileRecord::CR_C_TIER, atoi(cr_c_tier.c_str()), testSuite );
        AddTestCaseField<TPCE::CommissionRateDataFileRecord, const std::string&>( "CommissionRateDataFileRecord: TestCase_CR_TT_ID", &TestCase_DFRField<TPCE::CommissionRateDataFileRecord, const std::string&>, fields, &TPCE::CommissionRateDataFileRecord::CR_TT_ID, cr_tt_id, testSuite );
        AddTestCaseField<TPCE::CommissionRateDataFileRecord, const std::string&>( "CommissionRateDataFileRecord: TestCase_CR_EX_ID", &TestCase_DFRField<TPCE::CommissionRateDataFileRecord, const std::string&>, fields, &TPCE::CommissionRateDataFileRecord::CR_EX_ID, cr_ex_id, testSuite );
        AddTestCaseField<TPCE::CommissionRateDataFileRecord, int>( "CommissionRateDataFileRecord: TestCase_CR_FROM_QTY", &TestCase_DFRField<TPCE::CommissionRateDataFileRecord, int>, fields, &TPCE::CommissionRateDataFileRecord::CR_FROM_QTY, atoi(cr_from_qty.c_str()), testSuite );
        AddTestCaseField<TPCE::CommissionRateDataFileRecord, int>( "CommissionRateDataFileRecord: TestCase_CR_TO_QTY", &TestCase_DFRField<TPCE::CommissionRateDataFileRecord, int>, fields, &TPCE::CommissionRateDataFileRecord::CR_TO_QTY, atoi(cr_to_qty.c_str()), testSuite );
        AddTestCaseField<TPCE::CommissionRateDataFileRecord, double>( "CommissionRateDataFileRecord: TestCase_CR_RATE", &TestCase_DFRField<TPCE::CommissionRateDataFileRecord, double>, fields, &TPCE::CommissionRateDataFileRecord::CR_RATE, atof(cr_rate.c_str()), testSuite );
        AddTestCase( "CommissionRateDataFileRecord: TestCase_DFRToString", &TestCase_DFRMultiFieldToString<TPCE::CommissionRateDataFileRecord>, fields, testSuite );
    }

} // namespace EGenUtilitiesTest
