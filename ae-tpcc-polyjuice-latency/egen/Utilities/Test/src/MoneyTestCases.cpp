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
#include "../inc/MoneyTestCases.h"

//
// Include system headers
//
#include <cmath>

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"

using namespace EGenTestCommon;

// Macros are evil. Perhaps there is a better way,
// but I'm not spending time right now looking for it.
#ifdef WIN32
#undef min
#undef max
#endif

namespace EGenUtilitiesTest
{
    //
    // Constructor / Destructor
    //
    MoneyTestCases::MoneyTestCases()
    : minCents( std::numeric_limits<Cents>::min() )
        , maxCents( std::numeric_limits<Cents>::max() )
        , minDollars( minCents / 100.0 )
        , maxDollars( maxCents / 100.0 )
        , m1( 0 )
        , m2( 0 )
    {
    }

    MoneyTestCases::~MoneyTestCases()
    {
        CleanUp( &m1 );
        CleanUp( &m2 );
    }

    //
    // Add test cases to the test suite.
    //
    void MoneyTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< MoneyTestCases > tester ) const
    {
        //
        // Constructor test cases.
        //
        AddTestCase( "CMoney: TestCase_Constructor_Default", &MoneyTestCases::TestCase_Constructor_Default, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Constructor_Copy", &MoneyTestCases::TestCase_Constructor_Copy, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Constructor_Double", &MoneyTestCases::TestCase_Constructor_Double, tester, testSuite );

        //
        // Access test cases.
        //
        AddTestCase( "CMoney: TestCase_DollarAmount", &MoneyTestCases::TestCase_DollarAmount, tester, testSuite );
        AddTestCase( "CMoney: TestCase_CentsAmount", &MoneyTestCases::TestCase_CentsAmount, tester, testSuite );

        //
        // Operator test cases - CMoney
        //
        AddTestCase( "CMoney: TestCase_Operator_Plus_CMoney", &MoneyTestCases::TestCase_Operator_Plus_CMoney, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_PlusEquals_CMoney", &MoneyTestCases::TestCase_Operator_PlusEquals_CMoney, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_Minus_CMoney", &MoneyTestCases::TestCase_Operator_Minus_CMoney, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_MinusEquals_CMoney", &MoneyTestCases::TestCase_Operator_MinusEquals_CMoney, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_Multiply_CMoney", &MoneyTestCases::TestCase_Operator_Multiply_CMoney, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_Divide_CMoney", &MoneyTestCases::TestCase_Operator_Divide_CMoney, tester, testSuite );

        //
        // Operator test cases - Int
        //
        AddTestCase( "CMoney: TestCase_Operator_Multiply_Int", &MoneyTestCases::TestCase_Operator_Multiply_Int, tester, testSuite );

        //
        // Operator test cases - Double
        //
        AddTestCase( "CMoney: TestCase_Operator_Plus_Double", &MoneyTestCases::TestCase_Operator_Plus_Double, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_Minus_Double", &MoneyTestCases::TestCase_Operator_Minus_Double, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_MinusEquals_Double", &MoneyTestCases::TestCase_Operator_MinusEquals_Double, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_Multiply_Double", &MoneyTestCases::TestCase_Operator_Multiply_Double, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_Divide_Double", &MoneyTestCases::TestCase_Operator_Divide_Double, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_Assignment_Double", &MoneyTestCases::TestCase_Operator_Assignment_Double, tester, testSuite );

        //
        // Operator test cases - Comparison
        //
        AddTestCase( "CMoney: TestCase_Operator_Equivalent", &MoneyTestCases::TestCase_Operator_Equivalent, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_GreaterThan", &MoneyTestCases::TestCase_Operator_GreaterThan, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_GreaterThanEqual", &MoneyTestCases::TestCase_Operator_GreaterThanEqual, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_LessThan", &MoneyTestCases::TestCase_Operator_LessThan, tester, testSuite );
        AddTestCase( "CMoney: TestCase_Operator_LessThanEqual", &MoneyTestCases::TestCase_Operator_LessThanEqual, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void MoneyTestCases::TestCase_Constructor_Default()
    {
        Cents   initialCents    = 0;
        Dollars initialDollars  = 0.00;

        // Construct the object.
        BOOST_CHECK_NO_THROW( m1 = new TPCE::CMoney() );

        // If construction was successful, validate the object was initialized as expected.
        if( m1 != 0 )
        {
            BOOST_CHECK_EQUAL( initialCents, m1->CentsAmount() );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, ( initialDollars ) ( m1->DollarAmount() ));
            CleanUp( &m1 );
        }
    }

    void MoneyTestCases::TestCase_Constructor_Copy()
    {
        size_t  ii;             // loop counter
        Dollars dollarAmount;   // holds cents converted to dollars

        Cents testCases[] = {
              minCents  // test lower bound
            ,     -100  // test a negative number
            ,        0  // test 0
            ,      100  // test a positive number
            , maxCents  // test upper bound
        };

        for( ii = 0; ii < sizeof( testCases ) / sizeof( testCases[0] ); ++ii )
        {
            dollarAmount = testCases[ii] / 100.0;

            // Use the REQUIRE level because there is no point in proceeding if
            // we can't successfully construct the source object.
            BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( dollarAmount ) );

            // Now try copy construction
            BOOST_CHECK_NO_THROW( m2 = new TPCE::CMoney( m1 ));
            if( m2 != 0 )
            {
                // Make sure everything checks out.
                BOOST_CHECK_EQUAL( testCases[ii], m2->CentsAmount() );
                BOOST_CHECK_EQUAL( m1->CentsAmount(), m2->CentsAmount() );
                BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, ( dollarAmount ) ( m2->DollarAmount() ));
                BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, ( m1->DollarAmount() ) ( m2->DollarAmount() ));

                CleanUp( &m2 );
            }

            CleanUp( &m1 );
        }
    }

    void MoneyTestCases::TestCase_Constructor_Double()
    {
        size_t ii;  // loop counter

        Dollars validTestCases[] = {
              minDollars    // test lower bound
            ,   -1942.00    // test a negative number
            ,       0.00    // test 0
            ,    2010.00    // test a positive number
            , maxDollars    // test upper bound
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_CHECK_NO_THROW( m1 = new TPCE::CMoney( validTestCases[ii] ));
            if( m1 != 0 )
            {
                BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, ( validTestCases[ii] ) ( m1->DollarAmount() ));
                CleanUp( &m1 );
            }
        }

        Dollars out_of_rangeTestCases[] = {
              minDollars - 100  // test below lower bound
            , maxDollars + 100  // test above upper bound
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_CHECK_THROW( m1 = new TPCE::CMoney( out_of_rangeTestCases[ii] ), std::out_of_range );
            if( m1 != 0 )
            {
                // Something went wrong if we got here because an exception was expected.
                // Do a comparison to get values printed out.
                BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, ( out_of_rangeTestCases[ii] ) ( m1->DollarAmount() ));
                CleanUp( &m1 );
            }
        }

        //
        //NOTE: CMoney does not define an epsilon for doubles. Implicitly, CMoney( 0.001 ) == CMoney( 0.0 ).
        //
    }

    //
    // Access test cases
    //
    void MoneyTestCases::TestCase_DollarAmount()
    {
        size_t ii;  // loop counter

        Dollars testCases[] = {
              minDollars    // test lower bound
            ,     -10.01    // test a negative number
            ,       0.00    // test 0
            ,       9.24    // test a positive number
            , maxDollars    // test upper bound
        };

        for( ii = 0; ii < sizeof( testCases ) / sizeof( testCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( testCases[ii] ));
            if( m1 != 0 )
            {
                BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, ( testCases[ii] ) ( m1->DollarAmount() ));
                CleanUp( &m1 );
            }
        }
    }

    void MoneyTestCases::TestCase_CentsAmount()
    {
        size_t ii;  // loop counter

        Cents testCases[] = {
              minCents  // test lower bound
            , -3124897  // test a negative number
            ,        0  // test 0
            , 98724334  // test a positive number
            , maxCents  // test upper bound
        };

        for( ii = 0; ii < sizeof( testCases ) / sizeof( testCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( testCases[ii] / 100.0 ));
            BOOST_CHECK_EQUAL( testCases[ii], m1->CentsAmount() );

            CleanUp( &m1 );
        }
    }

    //
    // Operator test cases - CMoney
    //
    void MoneyTestCases::TestCase_Operator_Plus_CMoney()
    {
        size_t          ii;     // loop counter
        TPCE::CMoney    result; // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  2.59,  9.15 }    // test positive addition
            ,{ -2.59,  9.15 }    // test positive addition crossing 0
            ,{  9.15, -2.59 }    // test negative addition
            ,{  2.59, -9.15 }    // test negative addition crossing 0
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( validTestCases[ii].dollarAmount2 ));

            BOOST_CHECK_NO_THROW( result = *m1 + *m2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 + validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
            CleanUp( &m2 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
            { ( minCents / 100.00 ),           ( minCents / 100.00 ) }             // test underflow
           ,{ (( maxCents - 5000 ) / 100.00 ), (( maxCents - 5000 ) / 100.00 ) }   // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount2 ));

            // The addition should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( result = *m1 + *m2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 + out_of_rangeTestCases[ii].dollarAmount2, result.DollarAmount() );

            CleanUp( &m1 );
            CleanUp( &m2 );
        }
    }

    void MoneyTestCases::TestCase_Operator_PlusEquals_CMoney()
    {
        size_t ii;  // loop counter

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  2.59,  9.15 }    // test positive addition
            ,{ -2.59,  9.15 }    // test positive addition crossing 0
            ,{  9.15, -2.59 }    // test negative addition
            ,{  2.59, -9.15 }    // test negative addition crossing 0
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( validTestCases[ii].dollarAmount2 ));

            BOOST_CHECK_NO_THROW( *m1 += *m2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 + validTestCases[ii].dollarAmount2) (m1->DollarAmount()));

            CleanUp( &m1 );
            CleanUp( &m2 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
            { ( minCents / 100.00 ),           ( minCents / 100.00 ) }             // test underflow
           ,{ (( maxCents - 5000 ) / 100.00 ), (( maxCents - 5000 ) / 100.00 ) }   // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount2 ));

            // The addition should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( *m1 += *m2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 + out_of_rangeTestCases[ii].dollarAmount2, m1->DollarAmount() );

            CleanUp( &m1 );
            CleanUp( &m2 );
        }
    }

    void MoneyTestCases::TestCase_Operator_Minus_CMoney()
    {
        size_t ii;              // loop counter
        TPCE::CMoney result;    // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  9.15,  2.59 }   // test positive subtraction
            ,{  2.59,  9.15 }   // test positive subtraction crossing 0
            ,{  9.15, -2.59 }   // test negative subtraction
            ,{ -2.59, -9.15 }   // test negative subtraction crossing 0
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( validTestCases[ii].dollarAmount2 ));

            BOOST_CHECK_NO_THROW( result = *m1 - *m2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 - validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
            CleanUp( &m2 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
            { (( minCents + 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test underflow
           ,{ (( maxCents - 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount2 ));

            // The subtraction should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( result = *m1 - *m2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 - out_of_rangeTestCases[ii].dollarAmount2, result.DollarAmount() );

            CleanUp( &m1 );
            CleanUp( &m2 );
        }
    }

    void MoneyTestCases::TestCase_Operator_MinusEquals_CMoney()
    {
        size_t ii;  // loop counter

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  9.15,  2.59 }    // test positive subtraction
            ,{  2.59,  9.15 }    // test positive subtraction crossing 0
            ,{  9.15, -2.59 }    // test negative subtraction
            ,{ -2.59, -9.15 }    // test negative subtraction crossing 0
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( validTestCases[ii].dollarAmount2 ));

            BOOST_CHECK_NO_THROW( *m1 -= *m2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 - validTestCases[ii].dollarAmount2) (m1->DollarAmount()));

            CleanUp( &m1 );
            CleanUp( &m2 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
            { (( minCents + 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test underflow
           ,{ (( maxCents - 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount2 ));

            // The subtraction should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( *m1 -= *m2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 - out_of_rangeTestCases[ii].dollarAmount2, m1->DollarAmount() );

            CleanUp( &m1 );
            CleanUp( &m2 );
        }
    }

    void MoneyTestCases::TestCase_Operator_Multiply_CMoney()
    {
        size_t          ii;     // loop counter
        TPCE::CMoney    result; // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  4.07,  2.00 }    // test positive-positive multiplication
            ,{  4.07, -2.00 }    // test positive-negative multiplication
            ,{ -4.07,  2.00 }    // test negative-positive multiplication
            ,{ -4.07, -2.00 }    // test negative-negative multiplication
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( validTestCases[ii].dollarAmount2 ));

            BOOST_CHECK_NO_THROW( result = *m1 * *m2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 * validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
            CleanUp( &m2 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
             { (( maxCents - 5000 ) / 100.00 ), (( maxCents - 5000 ) / 100.00 ) }    // test positive-positive overflow
            ,{ (( maxCents - 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test positive-negative underflow
            ,{ (( minCents + 5000 ) / 100.00 ), (( maxCents - 5000 ) / 100.00 ) }    // test negative-positive underflow
            ,{ (( minCents + 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test negative-negative overflow

           // { (( minCents - 5000 ) / 100.00 ), 10000.00 }   // test underflow
           //,{ (( maxCents - 5000 ) / 100.00 ), 10000.00 }   // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount2 ));

            // The addition should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( result = *m1 * *m2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 * out_of_rangeTestCases[ii].dollarAmount2, result.DollarAmount() );

            CleanUp( &m1 );
            CleanUp( &m2 );
        }
    }

    void MoneyTestCases::TestCase_Operator_Divide_CMoney()
    {
        size_t          ii;     // loop counter
        TPCE::CMoney    result; // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {       4.08,       2.04 } // test positive-positive division - even
            ,{       4.08,      -2.04 } // test positive-negative division - even
            ,{      -4.08,       2.04 } // test negative-positive division - even
            ,{      -4.08,      -2.04 } // test negative-negative division - even
            ,{ maxDollars, maxDollars } // test positive-positive division - unity
            ,{       4.08,      -4.08 } // test positive-negative division - unity
            ,{      -4.08,       4.08 } // test negative-positive division - unity
            ,{ minDollars, minDollars } // test negative-negative division - unity
            ,{       3.45,       2.47 } // test positive-positive division - odd
            ,{       3.45,      -2.47 } // test positive-negative division - odd
            ,{      -3.45,       2.47 } // test negative-positive division - odd
            ,{       3.45,       2.47 } // test negative-negative division - odd
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));
            BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( validTestCases[ii].dollarAmount2 ));

            BOOST_CHECK_NO_THROW( result = *m1 / *m2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 / validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
            CleanUp( &m2 );
        }
    }


    //
    // Operator test cases - Int
    //
    void MoneyTestCases::TestCase_Operator_Multiply_Int()
    {
        size_t          ii;     // loop counter
        TPCE::CMoney    result; // used to hold result

        typedef struct tagTestCaseMoneyInt
        {
            Dollars dollarAmount;
            int     multiplier;
        } TestCaseMoneyInt;

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        TestCaseMoneyInt validTestCases[] = {
             { minDollars,     0 }
            ,{ minDollars,     1 }
            ,{ maxDollars,    -1 }
            ,{ maxDollars,     0 }
            ,{ maxDollars,     1 }
            ,{       0.00, 32975 }
            ,{   32975.00,     0 }
            ,{      53.53,    33 }
            ,{      53.53,   -33 }
            ,{     -53.53,    33 }
            ,{     -53.53,   -33 }
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount ));

            BOOST_CHECK_NO_THROW( result = *m1 * validTestCases[ii].multiplier );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount * validTestCases[ii].multiplier) (result.DollarAmount()));

            CleanUp( &m1 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        TestCaseMoneyInt out_of_rangeTestCases[] = {
             { minDollars, 2 }
            ,{ maxDollars, 2 }
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount ));

            // The multiplication should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( result = *m1 * out_of_rangeTestCases[ii].multiplier, std::out_of_range );

            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (out_of_rangeTestCases[ii].dollarAmount * out_of_rangeTestCases[ii].multiplier) (result.DollarAmount()));

            CleanUp( &m1 );
        }
    }


    //
    // Operator test cases - Double
    //
    void MoneyTestCases::TestCase_Operator_Plus_Double()
    {
        size_t          ii;     // loop counter
        TPCE::CMoney    result; // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  2.59,  9.15 }    // test positive addition
            ,{ -2.59,  9.15 }    // test positive addition crossing 0
            ,{  9.15, -2.59 }    // test negative addition
            ,{  2.59, -9.15 }    // test negative addition crossing 0
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));

            BOOST_CHECK_NO_THROW( result = *m1 + validTestCases[ii].dollarAmount2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 + validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
            { ( minCents / 100.00 ),           ( minCents / 100.00 ) }             // test underflow
           ,{ (( maxCents - 5000 ) / 100.00 ), (( maxCents - 5000 ) / 100.00 ) }   // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));

            // The addition should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( result = *m1 + out_of_rangeTestCases[ii].dollarAmount2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 + out_of_rangeTestCases[ii].dollarAmount2, result.DollarAmount() );

            CleanUp( &m1 );
        }
    }

    void MoneyTestCases::TestCase_Operator_Minus_Double()
    {
        size_t ii;              // loop counter
        TPCE::CMoney result;    // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  9.15,  2.59 }   // test positive subtraction
            ,{  2.59,  9.15 }   // test positive subtraction crossing 0
            ,{  9.15, -2.59 }   // test negative subtraction
            ,{ -2.59, -9.15 }   // test negative subtraction crossing 0
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));

            BOOST_CHECK_NO_THROW( result = *m1 - validTestCases[ii].dollarAmount2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 - validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
            { (( minCents + 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test underflow
           ,{ (( maxCents - 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));

            // The subtraction should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( result = *m1 - out_of_rangeTestCases[ii].dollarAmount2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 - out_of_rangeTestCases[ii].dollarAmount2, result.DollarAmount() );

            CleanUp( &m1 );
        }
    }

    void MoneyTestCases::TestCase_Operator_MinusEquals_Double()
    {
        size_t ii;  // loop counter

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  9.15,  2.59 }    // test positive subtraction
            ,{  2.59,  9.15 }    // test positive subtraction crossing 0
            ,{  9.15, -2.59 }    // test negative subtraction
            ,{ -2.59, -9.15 }    // test negative subtraction crossing 0
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));

            BOOST_CHECK_NO_THROW( *m1 -= validTestCases[ii].dollarAmount2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 - validTestCases[ii].dollarAmount2) (m1->DollarAmount()));

            CleanUp( &m1 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
            { (( minCents + 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test underflow
           ,{ (( maxCents - 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));

            // The subtraction should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( *m1 -= out_of_rangeTestCases[ii].dollarAmount2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 - out_of_rangeTestCases[ii].dollarAmount2, m1->DollarAmount() );

            CleanUp( &m1 );
        }
    }

    void MoneyTestCases::TestCase_Operator_Multiply_Double()
    {
        size_t          ii;     // loop counter
        TPCE::CMoney    result; // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {  4.07,  2.00 }    // test positive-positive multiplication
            ,{  4.07, -2.00 }    // test positive-negative multiplication
            ,{ -4.07,  2.00 }    // test negative-positive multiplication
            ,{ -4.07, -2.00 }    // test negative-negative multiplication
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));

            BOOST_CHECK_NO_THROW( result = *m1 * validTestCases[ii].dollarAmount2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 * validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
        }

        //
        // Exception Test Cases - these are expected to produce exceptions.
        //
        const TestCaseDollarDollar out_of_rangeTestCases[] = {
             { (( maxCents - 5000 ) / 100.00 ), (( maxCents - 5000 ) / 100.00 ) }    // test positive-positive overflow
            ,{ (( maxCents - 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test positive-negative underflow
            ,{ (( minCents + 5000 ) / 100.00 ), (( maxCents - 5000 ) / 100.00 ) }    // test negative-positive underflow
            ,{ (( minCents + 5000 ) / 100.00 ), (( minCents + 5000 ) / 100.00 ) }    // test negative-negative overflow

           // { (( minCents - 5000 ) / 100.00 ), 10000.00 }   // test underflow
           //,{ (( maxCents - 5000 ) / 100.00 ), 10000.00 }   // test overflow
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( out_of_rangeTestCases[ii].dollarAmount1 ));

            // The addition should throw an out-of-range exception.
            // However, it doesn't...
            BOOST_CHECK_THROW( result = *m1 * out_of_rangeTestCases[ii].dollarAmount2, std::out_of_range );
            // These are not equal, but do this check just to
            // get the error message highlighting the overflow that was
            // not caught above.
            BOOST_CHECK_EQUAL( out_of_rangeTestCases[ii].dollarAmount1 * out_of_rangeTestCases[ii].dollarAmount2, result.DollarAmount() );

            CleanUp( &m1 );
        }
    }

    void MoneyTestCases::TestCase_Operator_Divide_Double()
    {
        size_t          ii;     // loop counter
        TPCE::CMoney    result; // used to hold result

        //
        // Valid Test Cases - these are expected to produce correct answers.
        //
        const TestCaseDollarDollar validTestCases[] = {
             {       4.08,       2.04 } // test positive-positive division - even
            ,{       4.08,      -2.04 } // test positive-negative division - even
            ,{      -4.08,       2.04 } // test negative-positive division - even
            ,{      -4.08,      -2.04 } // test negative-negative division - even
            ,{ maxDollars, maxDollars } // test positive-positive division - unity
            ,{       4.08,      -4.08 } // test positive-negative division - unity
            ,{      -4.08,       4.08 } // test negative-positive division - unity
            ,{ minDollars, minDollars } // test negative-negative division - unity
            ,{       3.45,       2.47 } // test positive-positive division - odd
            ,{       3.45,      -2.47 } // test positive-negative division - odd
            ,{      -3.45,       2.47 } // test negative-positive division - odd
            ,{       3.45,       2.47 } // test negative-negative division - odd
        };

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( m1  = new TPCE::CMoney( validTestCases[ii].dollarAmount1 ));

            BOOST_CHECK_NO_THROW( result = *m1 / validTestCases[ii].dollarAmount2 );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, (validTestCases[ii].dollarAmount1 / validTestCases[ii].dollarAmount2) (result.DollarAmount()));

            CleanUp( &m1 );
        }
    }

    void MoneyTestCases::TestCase_Operator_Assignment_Double()
    {
        size_t ii;  // loop counter

        Dollars validTestCases[] = {
              minDollars    // test lower bound
            ,   -1942.00    // test a negative number
            ,       0.00    // test 0
            ,    2010.00    // test a positive number
            , maxDollars    // test upper bound
        };

        BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney() );

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_CHECK_NO_THROW( *m1 = validTestCases[ii] );
            BOOST_CHECK_PREDICATE( DollarAmountsAreEqual, ( validTestCases[ii] ) ( m1->DollarAmount() ));
        }

        Dollars out_of_rangeTestCases[] = {
              minDollars - 100  // test below lower bound
            , maxDollars + 100  // test above upper bound
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            BOOST_CHECK_THROW( *m1 = out_of_rangeTestCases[ii], std::out_of_range );
        }

        //
        //NOTE: CMoney does not define an epsilon for doubles. Implicitly, CMoney( 0.001 ) == CMoney( 0.0 ).
        //

        CleanUp( &m1);
    }


    //
    // Operator test cases - Comparison
    //
    void MoneyTestCases::TestCase_Operator_Equivalent()
    {
        Dollars baseAmount = 1.17;

        BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( baseAmount ));
        BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( baseAmount ));

        BOOST_CHECK( *m1 == *m2 );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount - 0.01 );
        BOOST_CHECK( ! (*m1 == *m2) );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount + 0.01 );
        BOOST_CHECK( ! (*m1 == *m2) );

        CleanUp( &m1 );
        CleanUp( &m2 );
    }

    void MoneyTestCases::TestCase_Operator_GreaterThan()
    {
        Dollars baseAmount = 148.50;

        BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( baseAmount ));
        BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( baseAmount ));

        BOOST_CHECK( ! (*m1 > *m2) );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount - 0.01 );
        BOOST_CHECK( *m1 > *m2 );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount + 0.01 );
        BOOST_CHECK( ! (*m1 > *m2) );

        CleanUp( &m1 );
        CleanUp( &m2 );
    }

    void MoneyTestCases::TestCase_Operator_GreaterThanEqual()
    {
        Dollars baseAmount = 148.50;

        BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( baseAmount ));
        BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( baseAmount ));

        BOOST_CHECK( *m1 >= *m2 );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount - 0.01 );
        BOOST_CHECK( *m1 >= *m2 );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount + 0.01 );
        BOOST_CHECK( ! (*m1 >= *m2) );

        CleanUp( &m1 );
        CleanUp( &m2 );
    }

    void MoneyTestCases::TestCase_Operator_LessThan()
    {
        Dollars baseAmount = 148.50;

        BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( baseAmount ));
        BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( baseAmount ));

        BOOST_CHECK( ! (*m1 < *m2) );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount - 0.01 );
        BOOST_CHECK( ! (*m1 < *m2) );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount + 0.01 );
        BOOST_CHECK( *m1 < *m2 );

        CleanUp( &m1 );
        CleanUp( &m2 );
    }

    void MoneyTestCases::TestCase_Operator_LessThanEqual()
    {
        Dollars baseAmount = 148.50;

        BOOST_REQUIRE_NO_THROW( m1 = new TPCE::CMoney( baseAmount ));
        BOOST_REQUIRE_NO_THROW( m2 = new TPCE::CMoney( baseAmount ));

        BOOST_CHECK( *m1 <= *m2 );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount - 0.01 );
        BOOST_CHECK( ! (*m1 <= *m2) );

        BOOST_REQUIRE_NO_THROW( *m2 = baseAmount + 0.01 );
        BOOST_CHECK( *m1 <= *m2 );

        CleanUp( &m1 );
        CleanUp( &m2 );
    }

} // namespace EGenUtilitiesTest
