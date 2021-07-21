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
#include "../inc/CompanyCompetitorFileTestCases.h"

//
// Include system headers
//

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"
#include "../inc/Utilities.h"
#include "../../inc/TextFileSplitter.h"
#include "../../inc/DataFile.h"
#include "../../inc/DataFileTypes.h"
#include "../../inc/CompanyCompetitorDataFileRecord.h"
#include "../../inc/CompanyCompetitorFile.h"
#include "../../../Utilities/inc/MiscConsts.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    CompanyCompetitorFileTestCases::CompanyCompetitorFileTestCases()
    {
    }

    CompanyCompetitorFileTestCases::~CompanyCompetitorFileTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void CompanyCompetitorFileTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< CompanyCompetitorFileTestCases > tester ) const
    {
        AddTestCase( "CompanyCompetitorFile: TestCase_Constructor", &CompanyCompetitorFileTestCases::TestCase_Constructor, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_CalculateCompanyCompetitorCount", &CompanyCompetitorFileTestCases::TestCase_CalculateCompanyCompetitorCount, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_CalculateStartFromCompanyCompetitor", &CompanyCompetitorFileTestCases::TestCase_CalculateStartFromCompanyCompetitor, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_GetCompanyId", &CompanyCompetitorFileTestCases::TestCase_GetCompanyId, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_GetCompanyCompetitorId", &CompanyCompetitorFileTestCases::TestCase_GetCompanyCompetitorId, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_GetIndustryId", &CompanyCompetitorFileTestCases::TestCase_GetIndustryId, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_GetConfiguredCompanyCompetitorCount", &CompanyCompetitorFileTestCases::TestCase_GetConfiguredCompanyCompetitorCount, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_GetActiveCompanyCompetitorCount", &CompanyCompetitorFileTestCases::TestCase_GetActiveCompanyCompetitorCount, tester, testSuite );
        AddTestCase( "CompanyCompetitorFile: TestCase_GetRecord", &CompanyCompetitorFileTestCases::TestCase_GetRecord, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void CompanyCompetitorFileTestCases::TestCase_Constructor()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);
        UINT baseCompanyCount(5000);

        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;

        // Construct the object.
        BOOST_CHECK_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);

        // Change the scale.
        configuredCustomers = 10000;
        activeCustomers = 10000;

        splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        df = new TPCE::CompanyCompetitorDataFile_t(*splitter);

        // Construct the object.
        BOOST_CHECK_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_CalculateCompanyCompetitorCount()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        // Validate calculation.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCompetitorCount, file->CalculateCompanyCompetitorCount(configuredCustomers));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_CalculateStartFromCompanyCompetitor()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        // Validate calculation.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCompetitorCount, file->CalculateStartFromCompanyCompetitor(configuredCustomers));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_GetCompanyId()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        TIdent index(0);
        TIdent offset(0*baseCompanyCount*3);

        // /3 because there are 3 competitors per company.

        BOOST_CHECK_EQUAL(file->GetCompanyId(index) + (offset/3), file->GetCompanyId(index+offset));

        index = 0;
        offset = 2*baseCompanyCount*3;
        // Validate wrapping.
        BOOST_CHECK_EQUAL(file->GetCompanyId(index) + (offset/3), file->GetCompanyId(index+offset));

        index = 1234;
        offset = 2*baseCompanyCount*3;
        // Validate wrapping.
        BOOST_CHECK_EQUAL(file->GetCompanyId(index) + (offset/3), file->GetCompanyId(index+offset));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_GetCompanyCompetitorId()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        TIdent index(0);
        TIdent offset(0*baseCompanyCount*3);

        // /3 because there are 3 competitors per company.

        BOOST_CHECK_EQUAL(file->GetCompanyCompetitorId(index) + (offset/3), file->GetCompanyCompetitorId(index+offset));

        index = 0;
        offset = 2*baseCompanyCount*3;
        // Validate wrapping.
        BOOST_CHECK_EQUAL(file->GetCompanyCompetitorId(index) + (offset/3), file->GetCompanyCompetitorId(index+offset));

        index = 1234;
        offset = 2*baseCompanyCount*3;
        // Validate wrapping.
        BOOST_CHECK_EQUAL(file->GetCompanyCompetitorId(index) + (offset/3), file->GetCompanyCompetitorId(index+offset));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_GetIndustryId()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        TIdent index(0);
        TIdent offset(0*baseCompanyCount*3);

        BOOST_CHECK_EQUAL(file->GetIndustryId(index), file->GetIndustryId(index+offset));

        index = 0;
        offset = 2*baseCompanyCount*3;
        // Validate wrapping.
        BOOST_CHECK_EQUAL(file->GetIndustryId(index), file->GetIndustryId(index+offset));

        index = 1234;
        offset = 2*baseCompanyCount*3;
        // Validate wrapping.
        BOOST_CHECK_EQUAL(file->GetIndustryId(index), file->GetIndustryId(index+offset));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_GetConfiguredCompanyCompetitorCount()
    {
        TIdent configuredCustomers(43000);
        TIdent activeCustomers(43000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        // Validate calculation/setting.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCompetitorCount, file->GetConfiguredCompanyCompetitorCount());

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_GetActiveCompanyCompetitorCount()
    {
        TIdent configuredCustomers(43000);
        TIdent activeCustomers(39000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        // Validate calculation/setting.
        BOOST_CHECK_EQUAL(activeCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCompetitorCount, file->GetActiveCompanyCompetitorCount());

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyCompetitorFileTestCases::TestCase_GetRecord()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);
        UINT baseCompanyCount(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/CompanyCompetitor.txt");
        TPCE::CompanyCompetitorDataFile_t* df = new TPCE::CompanyCompetitorDataFile_t(*splitter);
        TPCE::CCompanyCompetitorFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyCompetitorFile(*df, configuredCustomers, activeCustomers, baseCompanyCount) );

        TIdent index(0);
        TIdent offset(0*baseCompanyCount*3);

        const TPCE::CompanyCompetitorDataFileRecord& rec1a = file->GetRecord(index);
        const TPCE::CompanyCompetitorDataFileRecord& rec1b = file->GetRecord(index+offset);
        // Comparing the string representations is a way to validate all fields are identical.
        BOOST_CHECK_EQUAL(rec1a.ToString(), rec1b.ToString());

        index = 0;
        offset = 2*baseCompanyCount*3;
        const TPCE::CompanyCompetitorDataFileRecord& rec2a = file->GetRecord(index);
        const TPCE::CompanyCompetitorDataFileRecord& rec2b = file->GetRecord(index+offset);
        BOOST_CHECK_EQUAL(rec2a.ToString(), rec2b.ToString());

        index = 1234;
        offset = 2*baseCompanyCount*3;
        const TPCE::CompanyCompetitorDataFileRecord& rec3a = file->GetRecord(index);
        const TPCE::CompanyCompetitorDataFileRecord& rec3b = file->GetRecord(index+offset);
        BOOST_CHECK_EQUAL(rec3a.ToString(), rec3b.ToString());

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

} // namespace EGenUtilitiesTest
