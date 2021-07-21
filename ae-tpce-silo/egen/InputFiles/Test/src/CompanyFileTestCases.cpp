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
#include "../inc/CompanyFileTestCases.h"

//
// Include system headers
//
#include <cstdio> // for snprintf
#include <string>
#include <vector>

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"
#include "../inc/Utilities.h"
#include "../../inc/TextFileSplitter.h"
#include "../../inc/DataFile.h"
#include "../../inc/DataFileTypes.h"
#include "../../inc/CompanyDataFileRecord.h"
#include "../../inc/CompanyFile.h"
#include "../../../Utilities/inc/MiscConsts.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    CompanyFileTestCases::CompanyFileTestCases()
    {
    }

    CompanyFileTestCases::~CompanyFileTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void CompanyFileTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< CompanyFileTestCases > tester ) const
    {
        AddTestCase( "CompanyFile: TestCase_Constructor", &CompanyFileTestCases::TestCase_Constructor, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_CalculateCompanyCount", &CompanyFileTestCases::TestCase_CalculateCompanyCount, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_CalculateStartFromCompany", &CompanyFileTestCases::TestCase_CalculateStartFromCompany, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_CreateName", &CompanyFileTestCases::TestCase_CreateName, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_GetCompanyId", &CompanyFileTestCases::TestCase_GetCompanyId, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_GetSize", &CompanyFileTestCases::TestCase_GetSize, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_GetConfiguredCompanyCount", &CompanyFileTestCases::TestCase_GetConfiguredCompanyCount, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_GetActiveCompanyCount", &CompanyFileTestCases::TestCase_GetActiveCompanyCount, tester, testSuite );
        AddTestCase( "CompanyFile: TestCase_GetRecord", &CompanyFileTestCases::TestCase_GetRecord, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void CompanyFileTestCases::TestCase_Constructor()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);

        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;

        // Construct the object.
        BOOST_CHECK_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);

        // Change the scale.
        configuredCustomers = 10000;
        activeCustomers = 10000;

        splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        df = new TPCE::CompanyDataFile_t(*splitter);

        // Construct the object.
        BOOST_CHECK_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_CalculateCompanyCount()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        // Validate calculation.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCount, file->CalculateCompanyCount(configuredCustomers));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_CalculateStartFromCompany()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        // Validate calculation.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCount, file->CalculateStartFromCompany(configuredCustomers));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_CreateName()
    {
        TIdent configuredCustomers(55000);
        TIdent activeCustomers(55000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        int bufSize = 60+1; // 60 for CO_NAME +1 for null terminator
        char* buf = new char[bufSize];

        // Set up test values.
        std::vector<int> testValues;
        testValues.push_back(0);                        // simple base base
        testValues.push_back(df->size() - 1);           // last simple case before extension
        testValues.push_back(df->size());               // first extension
        testValues.push_back((2 * df->size()) - 1);     // end of first extension set
        testValues.push_back((2 * df->size()));         // start of second extension set

        // Run through the test values.
        for( std::vector<int>::iterator id = testValues.begin(); id != testValues.end(); ++id )
        {
            memset(buf, 0, bufSize);
            std::string answer = file->GetRecord(*id).CO_NAME();
            if( *id / df->size() )
            {
                const int extensionBufSize = 100;
                char extensionBuf[extensionBufSize];
                snprintf(extensionBuf, extensionBufSize, " #%d", (*id / df->size()));
                answer.append(extensionBuf);
                // C++11
                //answer += " #" + to_string(static_cast<long long>(*id / df->size()));
            }
            file->CreateName(*id, buf, bufSize);
            BOOST_CHECK_EQUAL(answer.c_str(), buf);
        }

        // Clean everything up.
        delete [] buf;
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_GetCompanyId()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        // Set up test values.
        std::vector<int> testValues;
        testValues.push_back(0);                        // simple base base
        testValues.push_back(df->size() - 1);           // last simple case before extension
        testValues.push_back(df->size());               // first extension
        testValues.push_back((2 * df->size()) - 1);     // end of first extension set
        testValues.push_back((2 * df->size()));         // start of second extension set

        // Run through the test values.
        for( std::vector<int>::iterator id = testValues.begin(); id < testValues.end(); ++id )
        {
            // File indexes are 0-based, IDs are 1-based and have a shift.
            TIdent answer = *id + 1 + TPCE::iTIdentShift;
            BOOST_CHECK_EQUAL(answer, file->GetCompanyId(*id));
        }

        // Clean everything up.
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_GetSize()
    {
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);

        // Set up test values.
        std::vector<int> testValues;
        testValues.push_back(0);                        // simple base base
        testValues.push_back(df->size() - 1);           // last simple case before extension
        testValues.push_back(df->size());               // first extension
        testValues.push_back((2 * df->size()) - 1);     // end of first extension set
        testValues.push_back((2 * df->size()));         // start of second extension set

        // Run through the test values.
        for( std::vector<int>::iterator configuredCustomers = testValues.begin(); configuredCustomers < testValues.end(); ++configuredCustomers )
        {
            // Construct the object.
            TPCE::CCompanyFile* file;
            BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, *configuredCustomers, *configuredCustomers) );

            BOOST_CHECK_EQUAL(*configuredCustomers / TPCE::iDefaultLoadUnitSize * TPCE::iOneLoadUnitCompanyCount, file->GetSize());

            CleanUp(&file);
        }

        // Clean everything up.
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_GetConfiguredCompanyCount()
    {
        TIdent configuredCustomers(43000);
        TIdent activeCustomers(43000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        // Validate calculation/setting.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCount, file->GetConfiguredCompanyCount());

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_GetActiveCompanyCount()
    {
        TIdent configuredCustomers(43000);
        TIdent activeCustomers(39000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        // Validate calculation/setting.
        BOOST_CHECK_EQUAL(activeCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitCompanyCount, file->GetActiveCompanyCount());

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void CompanyFileTestCases::TestCase_GetRecord()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Company.txt");
        TPCE::CompanyDataFile_t* df = new TPCE::CompanyDataFile_t(*splitter);
        TPCE::CCompanyFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CCompanyFile(*df, configuredCustomers, activeCustomers) );

        // Set up test values.
        std::vector<int> testValues;
        testValues.push_back(0);                        // simple base base
        testValues.push_back(df->size() - 1);           // last simple case before extension
        testValues.push_back(df->size());               // first extension
        testValues.push_back((2 * df->size()) - 1);     // end of first extension set
        testValues.push_back((2 * df->size()));         // start of second extension set

        // Run through the test values.
        for( std::vector<int>::iterator id = testValues.begin(); id < testValues.end(); ++id )
        {
            const TPCE::CompanyDataFileRecord& answer = (*df)[*id % df->size()];
            const TPCE::CompanyDataFileRecord& val = file->GetRecord(*id);
            // Comparing the string representations is a way to validate all fields are identical.
            BOOST_CHECK_EQUAL(val.ToString(), answer.ToString());
        }

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

} // namespace EGenUtilitiesTest
