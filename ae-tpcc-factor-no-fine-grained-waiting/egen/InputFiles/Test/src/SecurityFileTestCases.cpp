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
#include "../inc/SecurityFileTestCases.h"

//
// Include system headers
//
#include <assert.h>
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
#include "../../inc/SecurityDataFileRecord.h"
#include "../../inc/SecurityFile.h"
#include "../../../Utilities/inc/MiscConsts.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    SecurityFileTestCases::SecurityFileTestCases()
    {
    }

    SecurityFileTestCases::~SecurityFileTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void SecurityFileTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< SecurityFileTestCases > tester ) const
    {
        AddTestCase( "SecurityFile: TestCase_Constructor", &SecurityFileTestCases::TestCase_Constructor, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_CalculateSecurityCount", &SecurityFileTestCases::TestCase_CalculateSecurityCount, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_CalculateStartFromSecurity", &SecurityFileTestCases::TestCase_CalculateStartFromSecurity, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_CreateSymbol", &SecurityFileTestCases::TestCase_CreateSymbol, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetCompanyId", &SecurityFileTestCases::TestCase_GetCompanyId, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetCompanyIndex", &SecurityFileTestCases::TestCase_GetCompanyIndex, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetSize", &SecurityFileTestCases::TestCase_GetSize, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetConfiguredSecurityCount", &SecurityFileTestCases::TestCase_GetConfiguredSecurityCount, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetActiveSecurityCount", &SecurityFileTestCases::TestCase_GetActiveSecurityCount, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetRecord", &SecurityFileTestCases::TestCase_GetRecord, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_LoadSymbolToIdMap", &SecurityFileTestCases::TestCase_LoadSymbolToIdMap, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetId", &SecurityFileTestCases::TestCase_GetId, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetIndex", &SecurityFileTestCases::TestCase_GetIndex, tester, testSuite );
        AddTestCase( "SecurityFile: TestCase_GetExchangeIndex", &SecurityFileTestCases::TestCase_GetExchangeIndex, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void SecurityFileTestCases::TestCase_Constructor()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);

        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;

        // Construct the object.
        BOOST_CHECK_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);

        // Change the scale.
        configuredCustomers = 10000;
        activeCustomers = 10000;

        splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        df = new TPCE::SecurityDataFile_t(*splitter);

        // Construct the object.
        BOOST_CHECK_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_CalculateSecurityCount()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        // Validate calculation.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitSecurityCount, file->CalculateSecurityCount(configuredCustomers));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_CalculateStartFromSecurity()
    {
        TIdent configuredCustomers(5000);
        TIdent activeCustomers(5000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        // Validate calculation.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitSecurityCount, file->CalculateStartFromSecurity(configuredCustomers));

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_CreateSymbol()
    {
        TIdent configuredCustomers(55000);
        TIdent activeCustomers(55000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        int bufSize = 70+1; // 70 for S_NAME +1 for null terminator
        char* buf = new char[bufSize];

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
            memset(buf, 0, bufSize);
            std::string answer = file->GetRecord(*id).S_SYMB();

            // HACK: not re-inventing extension logic - using hardcoded assumptions.
            switch( *id / df->size() )
            {
            case 1:
                answer += "-a";
                break;
            case 2:
                answer += "-b";
                break;
            default:
                break;
            }

            file->CreateSymbol(*id, buf, bufSize);
            BOOST_CHECK_EQUAL(answer.c_str(), buf);
        }

        // Clean everything up.
        delete [] buf;
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetCompanyId()
    {
        TIdent configuredCustomers(55000);
        TIdent activeCustomers(55000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

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
            // Just re-inventing the formula...
            TIdent answer = df->operator[](*id % df->size()).S_CO_ID() + (*id/df->size()*TPCE::iBaseCompanyCount) + TPCE::iTIdentShift;
            BOOST_CHECK_EQUAL(answer, file->GetCompanyId(*id));
        }

        // Clean everything up.
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetCompanyIndex()
    {
        TIdent configuredCustomers(55000);
        TIdent activeCustomers(55000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

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
            // Just re-inventing the formula...
            TIdent answer = file->GetCompanyId(*id) - TPCE::iTIdentShift - 1;
            BOOST_CHECK_EQUAL(answer, file->GetCompanyIndex(*id));
        }

        // Clean everything up.
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetSize()
    {
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);

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
            TPCE::CSecurityFile* file;
            BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, *configuredCustomers, *configuredCustomers, TPCE::iBaseCompanyCount) );

            BOOST_CHECK_EQUAL(*configuredCustomers / TPCE::iDefaultLoadUnitSize * TPCE::iOneLoadUnitSecurityCount, file->GetSize());

            CleanUp(&file);
        }

        // Clean everything up.
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetConfiguredSecurityCount()
    {
        TIdent configuredCustomers(43000);
        TIdent activeCustomers(43000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        // Validate calculation/setting.
        BOOST_CHECK_EQUAL(configuredCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitSecurityCount, file->GetConfiguredSecurityCount());

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetActiveSecurityCount()
    {
        TIdent configuredCustomers(43000);
        TIdent activeCustomers(39000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        // Validate calculation/setting.
        BOOST_CHECK_EQUAL(activeCustomers/TPCE::iDefaultLoadUnitSize*TPCE::iOneLoadUnitSecurityCount, file->GetActiveSecurityCount());

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetRecord()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

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
            const TPCE::SecurityDataFileRecord& answer = (*df)[*id % df->size()];
            const TPCE::SecurityDataFileRecord& val = file->GetRecord(*id);
            // Comparing the string representations is a way to validate all fields are identical.
            BOOST_CHECK_EQUAL(val.ToString(), answer.ToString());
        }

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }


    void SecurityFileTestCases::TestCase_LoadSymbolToIdMap()
    {
        TIdent configuredCustomers(50000);
        TIdent activeCustomers(50000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        // At the time of this writing, LoadSymbolToIdMap is public, const and always returns 
        // true. So there really isn't much to validate other than the fact that we can 
        // invoke it.
        BOOST_CHECK_EQUAL( true, file->LoadSymbolToIdMap() );

        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetId()
    {
        TIdent configuredCustomers(55000);
        TIdent activeCustomers(55000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        int bufSize = 70+1; // 70 for S_NAME +1 for null terminator
        char* buf = new char[bufSize];

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
            memset(buf, 0, bufSize);
            file->CreateSymbol(*id, buf, bufSize);  // Assume CreateSymbol works correctly.

            // + 1 because indexes are 0-based and IDs are 1-based.
            BOOST_CHECK_EQUAL(*id + 1, file->GetId( buf ));
        }

        // Clean everything up.
        delete [] buf;
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetIndex()
    {
        TIdent configuredCustomers(55000);
        TIdent activeCustomers(55000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

        int bufSize = 70+1; // 70 for S_NAME +1 for null terminator
        char* buf = new char[bufSize];

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
            memset(buf, 0, bufSize);
            file->CreateSymbol(*id, buf, bufSize);  // Assume CreateSymbol works correctly.

            BOOST_CHECK_EQUAL(*id, file->GetIndex( buf ));
        }

        // Clean everything up.
        delete [] buf;
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

    void SecurityFileTestCases::TestCase_GetExchangeIndex()
    {
        TIdent configuredCustomers(55000);
        TIdent activeCustomers(55000);

        // Construct the object.
        TPCE::TextFileSplitter* splitter = new TPCE::TextFileSplitter("../../../flat_in/Security.txt");
        TPCE::SecurityDataFile_t* df = new TPCE::SecurityDataFile_t(*splitter);
        TPCE::CSecurityFile* file;
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CSecurityFile(*df, configuredCustomers, activeCustomers, TPCE::iBaseCompanyCount) );

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
            switch( file->GetExchangeIndex( *id ))
            {
            case TPCE::eNYSE:
                BOOST_CHECK_EQUAL("NYSE", (*df)[*id % df->size()].S_EX_ID());
                break;
            case TPCE::eNASDAQ:
                BOOST_CHECK_EQUAL("NASDAQ", (*df)[*id % df->size()].S_EX_ID());
                break;
            case TPCE::eAMEX:
                BOOST_CHECK_EQUAL("AMEX", (*df)[*id % df->size()].S_EX_ID());
                break;
            case TPCE::ePCX:
                BOOST_CHECK_EQUAL("PCX", (*df)[*id % df->size()].S_EX_ID());
                break;
            default:
                assert(!"Bad Exchange enum when testing TPCE::CSecurityFile::GetExchangeIndex()!");
                break;
            }

        }

        // Clean everything up.
        CleanUp(&file);
        CleanUp(&df);
        CleanUp(&splitter);
    }

} // namespace EGenUtilitiesTest
