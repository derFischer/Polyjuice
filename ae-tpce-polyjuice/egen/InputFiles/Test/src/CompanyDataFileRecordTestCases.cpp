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
#include "../inc/CompanyDataFileRecordTestCases.h"

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
    CompanyDataFileRecordTestCases::CompanyDataFileRecordTestCases()
        : dfr1( 0 )
        , co_id("1")
        , co_st_id("ACTV")
        , co_name("Zi Corporation")
        , co_in_id("SP")
        , co_desc("Holding company with subsidiaries which develop and povides software technology and educational products; Provides internet-based education services i")
    {
        fields.push_back(co_id);
        fields.push_back(co_st_id);
        fields.push_back(co_name);
        fields.push_back(co_in_id);
        fields.push_back(co_desc);
    }

    CompanyDataFileRecordTestCases::~CompanyDataFileRecordTestCases()
    {
        CleanUp( &dfr1 );
    }

    //
    // Add test cases to the test suite.
    //
    void CompanyDataFileRecordTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< CompanyDataFileRecordTestCases > tester ) const
    {
        AddTestCase( "CompanyDataFileRecord: TestCase_DFRConstructor", &TestCase_DFRConstructor<TPCE::CompanyDataFileRecord>, fields, testSuite );
        AddTestCaseField<TPCE::CompanyDataFileRecord, TIdent>( "CompanyDataFileRecord: TestCase_CO_ID", &TestCase_DFRField<TPCE::CompanyDataFileRecord, TIdent>, fields, &TPCE::CompanyDataFileRecord::CO_ID, atoi(co_id.c_str()), testSuite );
        AddTestCaseField<TPCE::CompanyDataFileRecord, const std::string&>( "CompanyDataFileRecord: TestCase_CO_ST_ID", &TestCase_DFRField<TPCE::CompanyDataFileRecord, const std::string&>, fields, &TPCE::CompanyDataFileRecord::CO_ST_ID, co_st_id, testSuite );
        AddTestCaseField<TPCE::CompanyDataFileRecord, const std::string&>( "CompanyDataFileRecord: TestCase_CO_NAME", &TestCase_DFRField<TPCE::CompanyDataFileRecord, const std::string&>, fields, &TPCE::CompanyDataFileRecord::CO_NAME, co_name, testSuite );
        AddTestCaseField<TPCE::CompanyDataFileRecord, const std::string&>( "CompanyDataFileRecord: TestCase_CO_IN_ID", &TestCase_DFRField<TPCE::CompanyDataFileRecord, const std::string&>, fields, &TPCE::CompanyDataFileRecord::CO_IN_ID, co_in_id, testSuite );
        AddTestCaseField<TPCE::CompanyDataFileRecord, const std::string&>( "CompanyDataFileRecord: TestCase_CO_DESC", &TestCase_DFRField<TPCE::CompanyDataFileRecord, const std::string&>, fields, &TPCE::CompanyDataFileRecord::CO_DESC, co_desc, testSuite );
        AddTestCase( "CompanyDataFileRecord: TestCase_DFRToString", &TestCase_DFRMultiFieldToString<TPCE::CompanyDataFileRecord>, fields, testSuite );
    }

} // namespace EGenUtilitiesTest
