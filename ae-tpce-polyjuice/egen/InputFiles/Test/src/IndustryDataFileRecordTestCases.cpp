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
#include "../inc/IndustryDataFileRecordTestCases.h"

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
    IndustryDataFileRecordTestCases::IndustryDataFileRecordTestCases()
        : dfr1( 0 )
        , in_id("TMB")
        , in_name("1")
        , in_sc_id("5")
    {
        fields.push_back(in_id);
        fields.push_back(in_name);
        fields.push_back(in_sc_id);
    }

    IndustryDataFileRecordTestCases::~IndustryDataFileRecordTestCases()
    {
        CleanUp( &dfr1 );
    }

    //
    // Add test cases to the test suite.
    //
    void IndustryDataFileRecordTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< IndustryDataFileRecordTestCases > tester ) const
    {
        AddTestCase( "IndustryDataFileRecord: TestCase_DFRConstructor", &TestCase_DFRConstructor<TPCE::IndustryDataFileRecord>, fields, testSuite );
        AddTestCaseField<TPCE::IndustryDataFileRecord, const std::string&>( "IndustryDataFileRecord: TestCase_IN_ID", &TestCase_DFRField<TPCE::IndustryDataFileRecord, const std::string&>, fields, &TPCE::IndustryDataFileRecord::IN_ID, in_id, testSuite );
        AddTestCaseField<TPCE::IndustryDataFileRecord, const std::string&>( "IndustryDataFileRecord: TestCase_IN_NAME", &TestCase_DFRField<TPCE::IndustryDataFileRecord, const std::string&>, fields, &TPCE::IndustryDataFileRecord::IN_NAME, in_name, testSuite );
        AddTestCaseField<TPCE::IndustryDataFileRecord, const std::string&>( "IndustryDataFileRecord: TestCase_IN_SC_ID", &TestCase_DFRField<TPCE::IndustryDataFileRecord, const std::string&>, fields, &TPCE::IndustryDataFileRecord::IN_SC_ID, in_sc_id, testSuite );
        AddTestCase( "IndustryDataFileRecord: TestCase_DFRToString", &TestCase_DFRMultiFieldToString<TPCE::IndustryDataFileRecord>, fields, testSuite );
    }

} // namespace EGenUtilitiesTest
