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
#include "../inc/WeightedDataFileTestCases.h"

//
// Include system headers
//

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"
#include "../../inc/StringSplitter.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    WeightedDataFileTestCases::WeightedDataFileTestCases()
    {
    }

    WeightedDataFileTestCases::~WeightedDataFileTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void WeightedDataFileTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< WeightedDataFileTestCases > tester ) const
    {
        //
        // Constructor test cases.
        //
        AddTestCase( "WeightedDataFile: TestCase_Constructor_Default", &WeightedDataFileTestCases::TestCase_Constructor_Default, tester, testSuite );

        //
        // Member function test cases.
        //
        AddTestCase( "WeightedDataFile: TestCase_size", &WeightedDataFileTestCases::TestCase_size, tester, testSuite );
        AddTestCase( "WeightedDataFile: TestCase_operatorIndexer", &WeightedDataFileTestCases::TestCase_operatorIndexer, tester, testSuite );
        AddTestCase( "WeightedDataFile: TestCase_at", &WeightedDataFileTestCases::TestCase_at, tester, testSuite );
        AddTestCase( "WeightedDataFile: TestCase_getUniqueRecord", &WeightedDataFileTestCases::TestCase_getUniqueRecord, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void WeightedDataFileTestCases::TestCase_Constructor_Default()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("");
        BOOST_CHECK_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );
        CleanUp( &df );
        CleanUp( &splitter );

        // Test a container holding elements.
        splitter = new TPCE::StringSplitter("1\tone\n2\ttwo\n3\tthree\n");
        BOOST_CHECK_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );
        CleanUp( &df );
        CleanUp( &splitter );
    }

    void WeightedDataFileTestCases::TestCase_size()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );
        BOOST_CHECK_EQUAL(0, df->size());   // all records
        BOOST_CHECK_EQUAL(0, df->size( df->UniqueRecordsOnly ));    // unique records only
        CleanUp( &df );
        CleanUp( &splitter );


        // Test a container holding elements.
        splitter = new TPCE::StringSplitter("1\tone\n2\ttwo\n3\tthree\n");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );
        BOOST_CHECK_EQUAL(6, df->size());   // all records
        BOOST_CHECK_EQUAL(3, df->size( df->UniqueRecordsOnly ));    // unique records only
        CleanUp( &df );
        CleanUp( &splitter );
    }

    void WeightedDataFileTestCases::TestCase_operatorIndexer()
    {
        // Test a container holding elements.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("1\tone\n2\ttwo\n3\tthree\n");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_EQUAL( "one",   (*df)[0][0] );
        BOOST_CHECK_EQUAL( "two",   (*df)[1][0] );
        BOOST_CHECK_EQUAL( "two",   (*df)[2][0] );
        BOOST_CHECK_EQUAL( "three", (*df)[3][0] );
        BOOST_CHECK_EQUAL( "three", (*df)[4][0] );
        BOOST_CHECK_EQUAL( "three", (*df)[5][0] );

        CleanUp( &df );
        CleanUp( &splitter );
    }

    void WeightedDataFileTestCases::TestCase_at()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_THROW( (*df).at(0), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );

        // Test a container holding elements.
        splitter = new TPCE::StringSplitter("1\tone\n2\ttwo\n3\tthree\n");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_EQUAL( "one",   (*df).at(0).at(0) );
        BOOST_CHECK_EQUAL( "two",   (*df).at(1).at(0) );
        BOOST_CHECK_EQUAL( "two",   (*df).at(2).at(0) );
        BOOST_CHECK_EQUAL( "three", (*df).at(3).at(0) );
        BOOST_CHECK_EQUAL( "three", (*df).at(4).at(0) );
        BOOST_CHECK_EQUAL( "three", (*df).at(5).at(0) );
        BOOST_CHECK_THROW( (*df).at(6), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );
    }

    void WeightedDataFileTestCases::TestCase_getUniqueRecord()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_THROW( df->getUniqueRecord(0, true), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );



        // Test a container holding elements.
        splitter = new TPCE::StringSplitter("1\tone\n2\ttwo\n3\tthree\n");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::WeightedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_EQUAL( "one",   df->getUniqueRecord(0)[0] );
        BOOST_CHECK_EQUAL( "two",   df->getUniqueRecord(1)[0] );
        BOOST_CHECK_EQUAL( "three", df->getUniqueRecord(2)[0] );
        BOOST_CHECK_THROW( df->getUniqueRecord(3, true), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );
    }

} // namespace EGenUtilitiesTest
