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
#include "../inc/StringSplitterTestCases.h"

//
// Include system headers
//
#include <deque>
#include <sstream>

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    StringSplitterTestCases::StringSplitterTestCases()
        : defaultTestString("This\tstring\thas\nthe\tdefault\tdelimiters\n"),
        mixedTestString("This\tstring\thas_the\tmixed\tdelimiters_"),
        customTestString("This-string-has_the-custom-delimiters_")
    {
    }

    StringSplitterTestCases::~StringSplitterTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void StringSplitterTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< StringSplitterTestCases > tester ) const
    {
        //
        // Constructor test cases.
        //
        AddTestCase( "StringSplitter: TestCase_Constructor_Default", &StringSplitterTestCases::TestCase_Constructor_Default, tester, testSuite );

        //
        // Member function test cases.
        //
        AddTestCase( "StringSplitter: TestCase_eof", &StringSplitterTestCases::TestCase_eof, tester, testSuite );
        AddTestCase( "StringSplitter: TestCase_getNextRecord", &StringSplitterTestCases::TestCase_getNextRecord, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void StringSplitterTestCases::TestCase_Constructor_Default()
    {
        // Construct the object.
        BOOST_CHECK_NO_THROW( splitter1 = new TPCE::StringSplitter(defaultTestString) );
        CleanUp(&splitter1);

        // Construct the object.
        BOOST_CHECK_NO_THROW( splitter1 = new TPCE::StringSplitter(mixedTestString, '_') );
        CleanUp(&splitter1);

        // Construct the object.
        BOOST_CHECK_NO_THROW( splitter1 = new TPCE::StringSplitter(customTestString, '_', '-') );
        CleanUp(&splitter1);
    }

    void StringSplitterTestCases::TestCase_eof()
    {
        // Load the splitter with a null string.
        BOOST_REQUIRE_NO_THROW( splitter1 = new TPCE::StringSplitter("") );

        // Initially, eof should be false since we haven't done any reads yet.
        BOOST_CHECK_EQUAL(false, splitter1->eof());

        // Now do a read so EOF is detected.
        splitter1->getNextRecord();

        // Verify EOF.
        BOOST_CHECK_EQUAL(true, splitter1->eof());

        CleanUp(&splitter1);
    }

    void StringSplitterTestCases::TestCase_getNextRecord()
    {
        //
        // Default Delimiters
        //

        // Load the splitter with a string.
        BOOST_REQUIRE_NO_THROW( splitter1 = new TPCE::StringSplitter(defaultTestString) );

        std::deque<std::string> words;

        // First line.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 3, words.size() );
        BOOST_CHECK_EQUAL( "This", words[0] );
        BOOST_CHECK_EQUAL( "has", words[2] );

        // Second line.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 3, words.size() );
        BOOST_CHECK_EQUAL( "the", words[0] );
        BOOST_CHECK_EQUAL( "delimiters", words[2] );

        // Empty string at EOF.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 1, words.size() );
        BOOST_CHECK_EQUAL( "", words[0] );

        CleanUp(&splitter1);

        //
        // Mixed Delimiters
        //

        // Load the splitter with a string.
        BOOST_REQUIRE_NO_THROW( splitter1 = new TPCE::StringSplitter(mixedTestString, '_') );

        // First line.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 3, words.size() );
        BOOST_CHECK_EQUAL( "This", words[0] );
        BOOST_CHECK_EQUAL( "has", words[2] );

        // Second line.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 3, words.size() );
        BOOST_CHECK_EQUAL( "the", words[0] );
        BOOST_CHECK_EQUAL( "delimiters", words[2] );

        // Empty string at EOF.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 1, words.size() );
        BOOST_CHECK_EQUAL( "", words[0] );

        CleanUp(&splitter1);

        //
        // Custom Delimiters
        //

        // Load the splitter with a string.
        BOOST_REQUIRE_NO_THROW( splitter1 = new TPCE::StringSplitter(customTestString, '_', '-') );

        // First line.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 3, words.size() );
        BOOST_CHECK_EQUAL( "This", words[0] );
        BOOST_CHECK_EQUAL( "has", words[2] );

        // Second line.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 3, words.size() );
        BOOST_CHECK_EQUAL( "the", words[0] );
        BOOST_CHECK_EQUAL( "delimiters", words[2] );

        // Empty string at EOF.
        BOOST_CHECK_NO_THROW( words = splitter1->getNextRecord() );
        BOOST_CHECK_EQUAL( 1, words.size() );
        BOOST_CHECK_EQUAL( "", words[0] );

        CleanUp(&splitter1);
    }

} // namespace EGenUtilitiesTest
