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
#include "../inc/BufferFillerTestCases.h"

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"

using namespace EGenTestCommon;

namespace EGenUtilitiesTest
{
    //
    // Constructor / Destructor
    //
    BufferFillerTestCases::BufferFillerTestCases()
    {
    }

    BufferFillerTestCases::~BufferFillerTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void BufferFillerTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< BufferFillerTestCases > tester ) const
    {
        AddTestCase( "BufferFiller Test Case - FillArray", &BufferFillerTestCases::TestCase_FillArray, tester, testSuite );
        AddTestCase( "BufferFiller Test Case - ClearArray", &BufferFillerTestCases::TestCase_ClearArray, tester, testSuite );
    }

    //
    // Fill test case.
    //
    void BufferFillerTestCases::TestCase_FillArray() const
    {
        static const char newFiller='a';
        static const char initialFiller='\0';
        static const size_t bufferSize = 100;

        //
        // Check settings
        //
        if( bufferSize < 1 )
        {
            BOOST_MESSAGE( "Buffer size < 1; nothing to test." );
                return;
        }
        if( initialFiller == newFiller )
        {
            BOOST_MESSAGE( "Initial filler and new filler are the same; nothing to test." );
        }

        //
        // Pad an extra char on each side of the buffer so we can check for overruns.
        //
        static const size_t arraySize = bufferSize + 2;
        char array[arraySize];
        char *buffer = &array[1];
        size_t ii;  // loop counter

        //
        // Initialize the array
        //
        memset( array, initialFiller, arraySize );

        //
        // Fill the buffer
        //
        TPCE::BufferFiller::Fill( buffer, newFiller, bufferSize );

        //
        // Check that Fill filled the buffer
        //
        for( ii = 0; ii < bufferSize; ++ii)
        {
            BOOST_CHECK_EQUAL( *( buffer + ii ), newFiller );
        }

        //
        // Check that Fill didn't overrun the buffer at either end
        //
        BOOST_CHECK_EQUAL( array[0], initialFiller );
        BOOST_CHECK_EQUAL( array[arraySize-1], initialFiller );
    }

    //
    // Clear test case.
    //
    void BufferFillerTestCases::TestCase_ClearArray() const
    {
        static const char newFiller='\0';
        static const char initialFiller='a';
        static const size_t bufferSize = 100;

        //
        // Check settings
        //
        if( bufferSize < 1 )
        {
            BOOST_MESSAGE( "Buffer size < 1; nothing to test." );
                return;
        }
        if( initialFiller == newFiller )
        {
            BOOST_MESSAGE( "Initial filler and new filler are the same; nothing to test." );
        }

        //
        // Pad an extra char on each side of the buffer so we can check for overruns
        //
        static const size_t arraySize = bufferSize + 2;
        char array[arraySize];
        char *buffer = &array[1];
        size_t ii;  // loop counter

        //
        // Initialize the array
        //
        memset( array, initialFiller, arraySize );

        //
        // Fill the buffer
        //
        TPCE::BufferFiller::Clear( buffer, bufferSize );

        //
        // Check that Fill filled the buffer
        //
        for( ii = 0; ii < bufferSize; ++ii)
        {
            BOOST_CHECK_EQUAL( *( buffer + ii ), newFiller );
        }

        //
        // Check that Fill didn't overrun the buffer at either end
        //
        BOOST_CHECK_EQUAL( array[0], initialFiller );
        BOOST_CHECK_EQUAL( array[arraySize-1], initialFiller );
    }

} // namespace EGenUtilitiesTest
