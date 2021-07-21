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
#include "../inc/StatusTypeDataFileRecordTestCases.h"

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
    StatusTypeDataFileRecordTestCases::StatusTypeDataFileRecordTestCases()
        : dfr1( 0 )
        , st_id("CMPT")
        , st_name("Completed")
    {
        fields.push_back(st_id);
        fields.push_back(st_name);
    }

    StatusTypeDataFileRecordTestCases::~StatusTypeDataFileRecordTestCases()
    {
        CleanUp( &dfr1 );
    }

    //
    // Add test cases to the test suite.
    //
    void StatusTypeDataFileRecordTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< StatusTypeDataFileRecordTestCases > tester ) const
    {
        AddTestCase( "StatusTypeDataFileRecord: TestCase_DFRConstructor", &TestCase_DFRConstructor<TPCE::StatusTypeDataFileRecord>, fields, testSuite );
        AddTestCaseField<TPCE::StatusTypeDataFileRecord, const std::string&>( "StatusTypeDataFileRecord: TestCase_ST_ID", &TestCase_DFRField<TPCE::StatusTypeDataFileRecord, const std::string&>, fields, &TPCE::StatusTypeDataFileRecord::ST_ID, st_id, testSuite );
        AddTestCaseField<TPCE::StatusTypeDataFileRecord, const std::string&>( "StatusTypeDataFileRecord: TestCase_ST_NAME", &TestCase_DFRField<TPCE::StatusTypeDataFileRecord, const std::string&>, fields, &TPCE::StatusTypeDataFileRecord::ST_NAME, st_name, testSuite );
        AddTestCase( "StatusTypeDataFileRecord: TestCase_DFRToString", &TestCase_DFRMultiFieldToString<TPCE::StatusTypeDataFileRecord>, fields, testSuite );
    }

} // namespace EGenUtilitiesTest
