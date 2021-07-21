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
#include "../inc/CompanyCompetitorDataFileRecordTestCases.h"

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
    CompanyCompetitorDataFileRecordTestCases::CompanyCompetitorDataFileRecordTestCases()
        : dfr1( 0 )
        , cp_co_id("1")
        , cp_comp_co_id("34")
        , cp_in_id("SP")
    {
        fields.push_back(cp_co_id);
        fields.push_back(cp_comp_co_id);
        fields.push_back(cp_in_id);
    }

    CompanyCompetitorDataFileRecordTestCases::~CompanyCompetitorDataFileRecordTestCases()
    {
        CleanUp( &dfr1 );
    }

    //
    // Add test cases to the test suite.
    //
    void CompanyCompetitorDataFileRecordTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< CompanyCompetitorDataFileRecordTestCases > tester ) const
    {
        AddTestCase( "CompanyCompetitorDataFileRecord: TestCase_DFRConstructor", &TestCase_DFRConstructor<TPCE::CompanyCompetitorDataFileRecord>, fields, testSuite );
        AddTestCaseField<TPCE::CompanyCompetitorDataFileRecord, TIdent>( "CompanyCompetitorDataFileRecord: TestCase_CP_CO_ID", &TestCase_DFRField<TPCE::CompanyCompetitorDataFileRecord, TIdent>, fields, &TPCE::CompanyCompetitorDataFileRecord::CP_CO_ID, atoi(cp_co_id.c_str()), testSuite );
        AddTestCaseField<TPCE::CompanyCompetitorDataFileRecord, TIdent>( "CompanyCompetitorDataFileRecord: TestCase_CP_COMP_CO_ID", &TestCase_DFRField<TPCE::CompanyCompetitorDataFileRecord, TIdent>, fields, &TPCE::CompanyCompetitorDataFileRecord::CP_COMP_CO_ID, atoi(cp_comp_co_id.c_str()), testSuite );
        AddTestCaseField<TPCE::CompanyCompetitorDataFileRecord, const std::string&>( "CompanyCompetitorDataFileRecord: TestCase_CP_IN_ID", &TestCase_DFRField<TPCE::CompanyCompetitorDataFileRecord, const std::string&>, fields, &TPCE::CompanyCompetitorDataFileRecord::CP_IN_ID, cp_in_id, testSuite );
        AddTestCase( "CompanyCompetitorDataFileRecord: TestCase_DFRToString", &TestCase_DFRMultiFieldToString<TPCE::CompanyCompetitorDataFileRecord>, fields, testSuite );
    }

} // namespace EGenUtilitiesTest
