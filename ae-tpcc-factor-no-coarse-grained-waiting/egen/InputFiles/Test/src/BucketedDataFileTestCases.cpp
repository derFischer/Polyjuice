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
#include "../inc/BucketedDataFileTestCases.h"

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
    BucketedDataFileTestCases::BucketedDataFileTestCases()
    {
    }

    BucketedDataFileTestCases::~BucketedDataFileTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void BucketedDataFileTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< BucketedDataFileTestCases > tester ) const
    {
        //
        // Constructor test cases.
        //
        AddTestCase( "BucketedDataFile: TestCase_Constructor_Default", &BucketedDataFileTestCases::TestCase_Constructor_Default, tester, testSuite );

        //
        // Member function test cases.
        //
        AddTestCase( "BucketedDataFile: TestCase_size", &BucketedDataFileTestCases::TestCase_size, tester, testSuite );
        AddTestCase( "BucketedDataFile: TestCase_operatorIndexer", &BucketedDataFileTestCases::TestCase_operatorIndexer, tester, testSuite );
        AddTestCase( "BucketedDataFile: TestCase_at", &BucketedDataFileTestCases::TestCase_at, tester, testSuite );
        AddTestCase( "BucketedDataFile: TestCase_getBucket", &BucketedDataFileTestCases::TestCase_getBucket, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void BucketedDataFileTestCases::TestCase_Constructor_Default()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("");
        BOOST_CHECK_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );
        CleanUp( &df );
        CleanUp( &splitter );

        // Test a container holding elements.
        splitter = new TPCE::StringSplitter("1\tone-one\n1\tone-two\n2\ttwo-one\n2\ttwo-two\n");
        BOOST_CHECK_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );
        CleanUp( &df );
        CleanUp( &splitter );
    }

    void BucketedDataFileTestCases::TestCase_size()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );
        BOOST_CHECK_EQUAL(0, df->size());       // size in records
        BOOST_CHECK_EQUAL(0, df->size(df->BucketsOnly));   // size in buckets
        CleanUp( &df );
        CleanUp( &splitter );


        // Test a container holding elements.
        splitter = new TPCE::StringSplitter("1\tone-one\n1\tone-two\n2\ttwo-one\n2\ttwo-two\n");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );
        BOOST_CHECK_EQUAL(4, df->size());       // size in records
        BOOST_CHECK_EQUAL(2, df->size(df->BucketsOnly));   // size in buckets
        CleanUp( &df );
        CleanUp( &splitter );
    }

    void BucketedDataFileTestCases::TestCase_operatorIndexer()
    {
        // Test a container holding elements.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("1\tone-one\n1\tone-two\n2\ttwo-one\n2\ttwo-two\n");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_EQUAL( "one-one", (*df)[0][0][0] );
        BOOST_CHECK_EQUAL( "one-two", (*df)[0][1][0] );
        BOOST_CHECK_EQUAL( "two-one", (*df)[1][0][0] );
        BOOST_CHECK_EQUAL( "two-two", (*df)[1][1][0] );

        CleanUp( &df );
        CleanUp( &splitter );
    }

    void BucketedDataFileTestCases::TestCase_at()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter("");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_THROW( (*df).at(0), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );

        // Test a container holding elements.
        splitter = new TPCE::StringSplitter("1\tone-one\n1\tone-two\n2\ttwo-one\n2\ttwo-two\n");
        BOOST_REQUIRE_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_EQUAL( "one-one", (*df).at(0)[0][0] );
        BOOST_CHECK_EQUAL( "one-two", (*df).at(0)[1][0] );
        BOOST_CHECK_EQUAL( "two-one", (*df).at(1)[0][0] );
        BOOST_CHECK_EQUAL( "two-two", (*df).at(1)[1][0] );

        BOOST_CHECK_THROW( (*df).at(2), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );
    }

    void BucketedDataFileTestCases::TestCase_getBucket()
    {
        // Test an empty container.
        TPCE::StringSplitter* splitter = new TPCE::StringSplitter( "" );
        BOOST_REQUIRE_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );

        BOOST_CHECK_THROW( df->getBucket(0, true), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );

        // Test a container holding elements.
        splitter = new TPCE::StringSplitter( "1\tone-one\n1\tone-two\n2\ttwo-one\n2\ttwo-two\n" );
        BOOST_REQUIRE_NO_THROW( df = new TPCE::BucketedDataFile< std::deque<std::string> >(*splitter) );

        //
        // Unchecked access.
        //
        BOOST_CHECK_EQUAL( "one-one", df->getBucket(1)[0][0] );
        BOOST_CHECK_EQUAL( "one-two", df->getBucket(1)[1][0] );
        BOOST_CHECK_EQUAL( "two-one", df->getBucket(2)[0][0] );
        BOOST_CHECK_EQUAL( "two-two", df->getBucket(2)[1][0] );

        //
        // Range-checked access
        //

        // Should throw because bucket IDs start at 1 not 0.
        BOOST_CHECK_THROW( df->getBucket(0, true), std::out_of_range );

        BOOST_CHECK_EQUAL( "one-one", df->getBucket(1, true)[0][0] );
        BOOST_CHECK_EQUAL( "one-two", df->getBucket(1, true)[1][0] );
        BOOST_CHECK_EQUAL( "two-one", df->getBucket(2, true)[0][0] );
        BOOST_CHECK_EQUAL( "two-two", df->getBucket(2, true)[1][0] );

        BOOST_CHECK_THROW( df->getBucket(3, true), std::out_of_range );

        CleanUp( &df );
        CleanUp( &splitter );
    }

} // namespace EGenUtilitiesTest
