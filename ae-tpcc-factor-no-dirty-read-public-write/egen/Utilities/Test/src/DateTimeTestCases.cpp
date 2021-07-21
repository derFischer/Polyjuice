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
#include "../inc/DateTimeTestCases.h"

//
// Include system headers
//
#include <string>

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"

using namespace EGenTestCommon;

namespace EGenUtilitiesTest
{
    ///////////////////////////////////////////////////////////////////////
    //
    // This section contains various test case definitions.
    //

    //
    // Day number test cases
    //
    const INT32 DateTimeTestCases::validTestCasesDayNumber[] = {
         0          // Jan  1, 0001
        ,3652058    // Dec 31, 9999
    };

    const INT32 DateTimeTestCases::out_of_rangeTestCasesDayNumber[] = {
        -1          // Day before Jan  1, 0001
        ,3652059    // Day after  Dec 31, 9999
    };

    //
    // Year/Month/Day test cases
    //
    // By definition (in CDateTime) January 1, 0001 is day number 0.
    // Since it is 0-based, subtract 1 from expected day-numbers.
    //
    // Format: { yyyy, mm, dd, day# }
    //
    const DateTimeTestCases::TestCaseYMD DateTimeTestCases::validTestCasesYMD[] = {
        {     1,  1,  1, 0 }                    // First Day
        ,{    1,  2, 28, 31+28-1 }              // First non-leap year
        ,{    1, 12, 31, 365-1 }                // Last day of first year
        ,{    4,  2, 29, 365*3+31+29-1 }        // First leap day
        ,{ 2007,  3, 15, 732676+31+28+15-1 }
    };

    //
    // These test cases are expected to throw std::out_of_range exceptions.
    // Because we're testing exceptions, the expected day-number doesn't matter.
    //
    const DateTimeTestCases::TestCaseYMD DateTimeTestCases::out_of_rangeTestCasesYMD[] = {
         {     0,  1,  1, 0 }   // Year too low
        ,{     1,  0,  1, 0 }   // Month too low
        ,{     1,  1,  0, 0 }   // Day too low
        ,{ 10000,  1,  1, 0 }   // Year too high
        ,{     1, 13,  1, 0 }   // Month too high
        ,{     1,  1, 32, 0 }   // Day too high - Jan
        ,{     1,  2, 29, 0 }   // Day too high - Feb
        ,{     4,  2, 30, 0 }   // Day too high - Feb Leap Year
        ,{     1,  3, 32, 0 }   // Day too high - Mar
        ,{     1,  4, 31, 0 }   // Day too high - Apr
        ,{     1,  5, 32, 0 }   // Day too high - May
        ,{     1,  6, 31, 0 }   // Day too high - Jun
        ,{     1,  7, 32, 0 }   // Day too high - Jul
        ,{     1,  8, 32, 0 }   // Day too high - Aug
        ,{     1,  9, 31, 0 }   // Day too high - Sep
        ,{     1, 10, 32, 0 }   // Day too high - Oct
        ,{     1, 11, 31, 0 }   // Day too high - Nov
        ,{     1, 12, 32, 0 }   // Day too high - Dec
    };

    //
    // Year/Month/Day/Hour/Minute/Second/Millisecond test cases
    //
    // By definition (in CDateTime) January 1, 0001 is day number 0.
    // Since it is 0-based, subtract 1 from expected day-numbers.
    //
    // Format: { yyyy, mm, dd, hh, mm, ss, mmm, day#, msec }
    //
    const DateTimeTestCases::TestCaseYMDHMSM DateTimeTestCases::validTestCasesYMDHMSM[] = {
        {     1,  1,  1,  0,  0,  0,   0, 0, 0 }                        // Base day
        ,{    1,  2, 28,  0,  0,  0,   0, 31+28-1, 0 }                  // First non-leap year
        ,{    1, 12, 31,  0,  0,  0,   0, 365-1, 0 }                    // Last day of first year
        ,{    4,  2, 29,  0,  0,  0,   0, 365*3+31+29-1, 0 }            // First leap day
        ,{ 2007,  3, 15,  0,  0,  0,   0, 732676+31+28+15-1, 0 }
        ,{ 9999, 12, 31, 23, 59, 59, 999, 3652059-1, TPCE::MsPerDay-1 } // Max values
    };

    //
    // These test cases are expected to throw std::out_of_range exceptions.
    // Because we're testing exceptions, the expected day-number and msec don't matter.
    //
    const DateTimeTestCases::TestCaseYMDHMSM DateTimeTestCases::out_of_rangeTestCasesYMDHMSM[] = {
        {      0,  1,  1,  0,  0,  0,    0, 0, 0 }  // Year too low
        ,{     1,  0,  1,  0,  0,  0,    0, 0, 0 }  // Month too low
        ,{     1,  1,  0,  0,  0,  0,    0, 0, 0 }  // Day too low
        ,{     1,  1,  1, -1,  0,  0,    0, 0, 0 }  // Hour too low
        ,{     1,  1,  1,  0, -1,  0,    0, 0, 0 }  // Minute too low
        ,{     1,  1,  1,  0,  0, -1,    0, 0, 0 }  // Second too low
        ,{     1,  1,  1,  0,  0,  0,   -1, 0, 0 }  // MilliSecond too low
        ,{ 10000,  1,  1,  0,  0,  0,    0, 0, 0 }  // Year too high
        ,{     1, 13,  1,  0,  0,  0,    0, 0, 0 }  // Month too high
        ,{     1,  1, 32,  0,  0,  0,    0, 0, 0 }  // Day too high - Jan
        ,{     1,  2, 29,  0,  0,  0,    0, 0, 0 }  // Day too high - Feb
        ,{     4,  2, 30,  0,  0,  0,    0, 0, 0 }  // Day too high - Feb Leap Year
        ,{     1,  3, 32,  0,  0,  0,    0, 0, 0 }  // Day too high - Mar
        ,{     1,  4, 31,  0,  0,  0,    0, 0, 0 }  // Day too high - Apr
        ,{     1,  5, 32,  0,  0,  0,    0, 0, 0 }  // Day too high - May
        ,{     1,  6, 31,  0,  0,  0,    0, 0, 0 }  // Day too high - Jun
        ,{     1,  7, 32,  0,  0,  0,    0, 0, 0 }  // Day too high - Jul
        ,{     1,  8, 32,  0,  0,  0,    0, 0, 0 }  // Day too high - Aug
        ,{     1,  9, 31,  0,  0,  0,    0, 0, 0 }  // Day too high - Sep
        ,{     1, 10, 32,  0,  0,  0,    0, 0, 0 }  // Day too high - Oct
        ,{     1, 11, 31,  0,  0,  0,    0, 0, 0 }  // Day too high - Nov
        ,{     1, 12, 32,  0,  0,  0,    0, 0, 0 }  // Day too high - Dec
        ,{     1,  1,  1, 24,  0,  0,    0, 0, 0 }  // Hour too high
        ,{     1,  1,  1,  0, 60,  0,    0, 0, 0 }  // Minute too high
        ,{     1,  1,  1,  0,  0, 60,    0, 0, 0 }  // Second too high
        ,{     1,  1,  1,  0,  0,  0, 1000, 0, 0 }  // MilliSecond too high
    };

    //
    // Hour/Minute/Second/Millisecond test cases
    //
    // Format: { hh, mm, ss, mm, day#, msec }
    //
    const DateTimeTestCases::TestCaseHMSM DateTimeTestCases::validTestCasesHMSM[] = {
         {  0,  0,  0,   0, 0 }                     // Min valid values
        ,{ 23, 59, 59, 999, TPCE::MsPerDay - 1 }    // Max valid values
    };

    //
    // These test cases are expected to throw std::out_of_range exceptions.
    // Because we're testing exceptions, the expected milliseconds doesn't matter.
    //
    const DateTimeTestCases::TestCaseHMSM DateTimeTestCases::out_of_rangeTestCasesHMSM[] = {
         { -1,  0,  0,   0, 0 } // Hour too low
        ,{  0, -1,  0,   0, 0 } // Minute too low
        ,{  0,  0, -1,   0, 0 } // Second too low
        ,{  0,  0,  0,  -1, 0 } // MilliSecond too low
    };

    //
    // End section containing various test case definitions.
    //
    ///////////////////////////////////////////////////////////////////////

    
    
    //
    // Constructor / Destructor
    //
    DateTimeTestCases::DateTimeTestCases()
        : dt1( 0 )
        , dt2( 0 )
    {
    }

    DateTimeTestCases::~DateTimeTestCases()
    {
        CleanUp( &dt1 );
        CleanUp( &dt2 );
    }

    //
    // Add test cases to the test suite.
    //
    void DateTimeTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< DateTimeTestCases > tester ) const
    {
        //
        // Constructor test cases.
        //
        AddTestCase( "CDateTime: TestCase_Constructor_Default", &DateTimeTestCases::TestCase_Constructor_Default, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Constructor_DayNumber", &DateTimeTestCases::TestCase_Constructor_DayNumber, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Constructor_YMD", &DateTimeTestCases::TestCase_Constructor_YMD, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Constructor_YMDHMSM", &DateTimeTestCases::TestCase_Constructor_YMDHMSM, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Constructor_TimestampStruct", &DateTimeTestCases::TestCase_Constructor_TimestampStruct, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Constructor_Copy", &DateTimeTestCases::TestCase_Constructor_Copy, tester, testSuite );

        //
        // Set test cases.
        //
        AddTestCase( "CDateTime: TestCase_Set_Default", &DateTimeTestCases::TestCase_Set_Default, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Set_DayNumber", &DateTimeTestCases::TestCase_Set_DayNumber, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Set_YMD", &DateTimeTestCases::TestCase_Set_YMD, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Set_YMDHMSM", &DateTimeTestCases::TestCase_Set_YMDHMSM, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Set_HMSM", &DateTimeTestCases::TestCase_Set_HMSM, tester, testSuite );

        //
        // Get test cases.
        //
        AddTestCase( "CDateTime: TestCase_Get_YMD", &DateTimeTestCases::TestCase_Get_YMD, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Get_YMDHMS", &DateTimeTestCases::TestCase_Get_YMDHMS, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Get_HMS", &DateTimeTestCases::TestCase_Get_HMS, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Get_TIMESTAMP_STRUCT", &DateTimeTestCases::TestCase_Get_TIMESTAMP_STRUCT, tester, testSuite );
#ifdef COMPILE_ODBC_LOAD
        AddTestCase( "CDateTime: TestCase_Get_DBDATETIME", &DateTimeTestCases::TestCase_Get_DBDATETIME, tester, testSuite );
#endif //COMPILE_ODBC_LOAD

        //
        // Day Number Calculation test case
        //
        AddTestCase( "CDateTime: TestCase_YMDtoDayno", &DateTimeTestCases::TestCase_YMDtoDayno, tester, testSuite );

        //
        // Character array test case
        //
        AddTestCase( "CDateTime: TestCase_ToStr", &DateTimeTestCases::TestCase_ToStr, tester, testSuite );

        //
        // Add test cases.
        //
        AddTestCase( "CDateTime: TestCase_Add_DM", &DateTimeTestCases::TestCase_Add_DM, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Add_Minutes", &DateTimeTestCases::TestCase_Add_Minutes, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Add_WorkMs", &DateTimeTestCases::TestCase_Add_WorkMs, tester, testSuite );

        //
        // Operator test cases
        //
        AddTestCase( "CDateTime: TestCase_Operator_LessThan", &DateTimeTestCases::TestCase_Operator_LessThan, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_LessThanOrEqual", &DateTimeTestCases::TestCase_Operator_LessThanOrEqual, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_GreaterThanOrEqual", &DateTimeTestCases::TestCase_Operator_GreaterThanOrEqual, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_Equivalent", &DateTimeTestCases::TestCase_Operator_Equivalent, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_Minus", &DateTimeTestCases::TestCase_Operator_Minus, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_DiffInMilliSecondsReference", &DateTimeTestCases::TestCase_Operator_DiffInMilliSecondsReference, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_DiffInMilliSecondsPointer", &DateTimeTestCases::TestCase_Operator_DiffInMilliSecondsPointer, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_PlusEquals", &DateTimeTestCases::TestCase_Operator_PlusEquals, tester, testSuite );
        AddTestCase( "CDateTime: TestCase_Operator_Assignment", &DateTimeTestCases::TestCase_Operator_Assignment, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void DateTimeTestCases::TestCase_Constructor_Default() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::CDateTime() test cases not implemented yet.\n\n"
            "\tThis constructor uses platform specific time code. Validating this\n"
            "\tcreates a bit of a chicken-or-the-egg situation.\n"
            "\tThis needs to be resolved before meaningful testing can be done.\n\n"
            ;
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Constructor_DayNumber()
    {
        size_t ii;  // loop counter

        for( ii = 0; ii < sizeof(validTestCasesDayNumber)/sizeof(validTestCasesDayNumber[0]); ++ii )
        {
            BOOST_CHECK_NO_THROW( dt1 = new TPCE::CDateTime( validTestCasesDayNumber[ii] ));
            if( dt1 != 0 )
            {
                BOOST_CHECK_EQUAL( validTestCasesDayNumber[ii], dt1->DayNo() );
                CleanUp( &dt1 );
            }
        }

        for( ii = 0; ii < sizeof(out_of_rangeTestCasesDayNumber)/sizeof(out_of_rangeTestCasesDayNumber[0]); ++ii )
        {
            BOOST_CHECK_THROW( dt1 = new TPCE::CDateTime( out_of_rangeTestCasesDayNumber[ii] ), std::out_of_range );
            CleanUp( &dt1 );
        }
    }

    void DateTimeTestCases::TestCase_Constructor_YMD()
    {
        TestCaseYMD xCheck; // used for cross-checking
        size_t      ii;     // loop counter

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCasesYMD ) / sizeof( validTestCasesYMD[0] ); ++ii )
        {
            // Construct the object.
            BOOST_CHECK_NO_THROW( dt1 = new TPCE::CDateTime( validTestCasesYMD[ii].year, validTestCasesYMD[ii].month, validTestCasesYMD[ii].day ));

            // If construction was successful, validate the object was initialized as expected.
            if( dt1 != 0 )
            {
                // We can't really check if the construction happened correctly if we can get
                // these values safely - so use REQUIRE level.
                BOOST_REQUIRE_NO_THROW( xCheck.dayNumber = dt1->DayNo() );
                BOOST_REQUIRE_NO_THROW( dt1->GetYMD( &( xCheck.year ), &( xCheck.month ), &( xCheck.day )));

                BOOST_CHECK_EQUAL( validTestCasesYMD[ii].year, xCheck.year );
                BOOST_CHECK_EQUAL( validTestCasesYMD[ii].month, xCheck.month );
                BOOST_CHECK_EQUAL( validTestCasesYMD[ii].day, xCheck.day );
                BOOST_CHECK_EQUAL( validTestCasesYMD[ii].dayNumber, xCheck.dayNumber );
                CleanUp( &dt1 );
            }
        }

        // Try test cases we expect to fail.
        for( ii = 0; ii < sizeof (out_of_rangeTestCasesYMD ) / sizeof( out_of_rangeTestCasesYMD[0] ); ++ii)
        {
            // Try to construct the object.
            BOOST_CHECK_THROW( dt1 = new TPCE::CDateTime( out_of_rangeTestCasesYMD[ii].year, out_of_rangeTestCasesYMD[ii].month, out_of_rangeTestCasesYMD[ii].day ),
                std::out_of_range 
                );

            // Clean up just in case construction was unexpectedly successful.
            CleanUp( &dt1 );
        }
    }

    void DateTimeTestCases::TestCase_Constructor_YMDHMSM()
    {
        TestCaseYMDHMSM xCheck;
        size_t ii;  // loop counter

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCasesYMDHMSM ) / sizeof( validTestCasesYMDHMSM[0] ); ++ii )
        {
            BOOST_CHECK_NO_THROW( dt1 = new TPCE::CDateTime( validTestCasesYMDHMSM[ii].year,
                validTestCasesYMDHMSM[ii].month, 
                validTestCasesYMDHMSM[ii].day,
                validTestCasesYMDHMSM[ii].hour,
                validTestCasesYMDHMSM[ii].minute,
                validTestCasesYMDHMSM[ii].second,
                validTestCasesYMDHMSM[ii].millisecond
                ));

            // If construction was successful, validate the object was initialized as expected.
            if( dt1 != 0 )
            {
                // We can't really check if the construction happened correctly if we can get
                // these values safely - so use REQUIRE level.
                BOOST_REQUIRE_NO_THROW( xCheck.dayNumber = dt1->DayNo());
                BOOST_REQUIRE_NO_THROW( xCheck.msecSoFarToday = dt1->MSec());
                BOOST_REQUIRE_NO_THROW( dt1->GetYMDHMS( &(xCheck.year), &(xCheck.month), &(xCheck.day), 
                    &(xCheck.hour), &(xCheck.minute), &(xCheck.second), &(xCheck.millisecond) ));

                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].year, xCheck.year );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].month, xCheck.month );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].day, xCheck.day );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].hour, xCheck.hour );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].minute, xCheck.minute );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].second, xCheck.second );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].millisecond, xCheck.millisecond );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].dayNumber, xCheck.dayNumber );
                BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].msecSoFarToday, xCheck.msecSoFarToday );
                CleanUp( &dt1 );
            }
        }

        // Try test cases we expect to fail.
        for( ii = 0; ii < sizeof( out_of_rangeTestCasesYMDHMSM ) / sizeof( out_of_rangeTestCasesYMDHMSM[0] ); ++ii)
        {
            BOOST_CHECK_THROW( dt1 = new TPCE::CDateTime( out_of_rangeTestCasesYMDHMSM[ii].year,
                out_of_rangeTestCasesYMDHMSM[ii].month, 
                out_of_rangeTestCasesYMDHMSM[ii].day,
                out_of_rangeTestCasesYMDHMSM[ii].hour,
                out_of_rangeTestCasesYMDHMSM[ii].minute,
                out_of_rangeTestCasesYMDHMSM[ii].second,
                out_of_rangeTestCasesYMDHMSM[ii].millisecond
                ), std::out_of_range );

            // Clean up just in case construction was unexpectedly successful.
            CleanUp( &dt1 );
        }
    }

    void DateTimeTestCases::TestCase_Constructor_TimestampStruct()
    {
        // Used to capture year/month/day inputs and the expected day-number output.
        typedef struct tagTestCase
        {
            TPCE::TIMESTAMP_STRUCT ts;
            INT32 dayNumber;
            INT32 msecSoFarToday;
        } TestCase;

        TestCase xCheck;
        size_t ii;  // loop counter

        // By definition (in CDateTime) January 1, 0001 is day number 0.
        // Since it is 0-based, subtract 1 from (most) expected day-numbers.
        TestCase validTestCases[] = {
             {{    1,  1,  1,  0,  0,  0,         0}, 0,                                  0 } // Base day
            ,{{    1,  2, 28,  0,  0,  0,         0}, 31+28-1,                            0 } // First non-leap year
            ,{{    1, 12, 31,  0,  0,  0,         0}, 365-1,                              0 } // Last day of first year
            ,{{    4,  2, 29,  0,  0,  0,         0}, 365*3+31+29-1,                      0 } // First leap day
            ,{{ 2007,  3, 15,  0,  0,  0,         0}, 732676+31+28+15-1,                  0 }
            ,{{ 9999, 12, 31, 23, 59, 59, 999000000}, 3652059-1,         TPCE::MsPerDay - 1 } // Max values
        };

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            BOOST_CHECK_NO_THROW( dt1 = new TPCE::CDateTime( &( validTestCases[ii].ts )));

            // If construction was successful, validate the object was initialized as expected.
            if( dt1 != 0 )
            {
                // We can't really check if the construction happened correctly if we can get
                // these values safely - so use REQUIRE level.
                BOOST_REQUIRE_NO_THROW( xCheck.dayNumber = dt1->DayNo());
                BOOST_REQUIRE_NO_THROW( xCheck.msecSoFarToday = dt1->MSec());
                BOOST_REQUIRE_NO_THROW( dt1->GetTimeStamp( &(xCheck.ts) ));

                BOOST_CHECK_EQUAL( validTestCases[ii].ts.year, xCheck.ts.year );
                BOOST_CHECK_EQUAL( validTestCases[ii].ts.month, xCheck.ts.month );
                BOOST_CHECK_EQUAL( validTestCases[ii].ts.day, xCheck.ts.day );
                BOOST_CHECK_EQUAL( validTestCases[ii].ts.hour, xCheck.ts.hour );
                BOOST_CHECK_EQUAL( validTestCases[ii].ts.minute, xCheck.ts.minute );
                BOOST_CHECK_EQUAL( validTestCases[ii].ts.second, xCheck.ts.second );
                BOOST_CHECK_EQUAL( validTestCases[ii].ts.fraction, xCheck.ts.fraction );
                BOOST_CHECK_EQUAL( validTestCases[ii].dayNumber, xCheck.dayNumber );
                BOOST_CHECK_EQUAL( validTestCases[ii].msecSoFarToday, xCheck.msecSoFarToday );
                CleanUp( &dt1 );
            }
        }

        // These test cases are expected to throw std::out_of_range exceptions.
        // Because we're testing exceptions, the expected day-number doesn't matter.
        TestCase out_of_rangeTestCases[] = {
             {{     0,  1,  1,  0,  0,  0,          0}, 0, 0 }  // Year too low
            ,{{     1,  0,  1,  0,  0,  0,          0}, 0, 0 }  // Month too low
            ,{{     1,  1,  0,  0,  0,  0,          0}, 0, 0 }  // Day too low
            //,{{     1,  1,  1, -1,  0,  0,   0}, 0, 0 }                // Hour too low
            //,{{     1,  1,  1,  0, -1,  0,   0}, 0, 0 }                // Minute too low
            //,{{     1,  1,  1,  0,  0, -1,   0}, 0, 0 }                // Second too low
            //,{{     1,  1,  1,  0,  0,  0,  -1}, 0, 0 }                // MilliSecond too low
            ,{{ 10000,  1,  1,  0,  0,  0,          0}, 0, 0 }  // Year too high
            ,{{     1, 13,  1,  0,  0,  0,          0}, 0, 0 }  // Month too high
            ,{{     1,  1, 32,  0,  0,  0,          0}, 0, 0 }  // Day too high - Jan
            ,{{     1,  2, 29,  0,  0,  0,          0}, 0, 0 }  // Day too high - Feb
            ,{{     4,  2, 30,  0,  0,  0,          0}, 0, 0 }  // Day too high - Feb Leap Year
            ,{{     1,  3, 32,  0,  0,  0,          0}, 0, 0 }  // Day too high - Mar
            ,{{     1,  4, 31,  0,  0,  0,          0}, 0, 0 }  // Day too high - Apr
            ,{{     1,  5, 32,  0,  0,  0,          0}, 0, 0 }  // Day too high - May
            ,{{     1,  6, 31,  0,  0,  0,          0}, 0, 0 }  // Day too high - Jun
            ,{{     1,  7, 32,  0,  0,  0,          0}, 0, 0 }  // Day too high - Jul
            ,{{     1,  8, 32,  0,  0,  0,          0}, 0, 0 }  // Day too high - Aug
            ,{{     1,  9, 31,  0,  0,  0,          0}, 0, 0 }  // Day too high - Sep
            ,{{     1, 10, 32,  0,  0,  0,          0}, 0, 0 }  // Day too high - Oct
            ,{{     1, 11, 31,  0,  0,  0,          0}, 0, 0 }  // Day too high - Nov
            ,{{     1, 12, 32,  0,  0,  0,          0}, 0, 0 }  // Day too high - Dec
            ,{{     1,  1,  1, 24,  0,  0,          0}, 0, 0 }  // Hour too high
            ,{{     1,  1,  1,  0, 60,  0,          0}, 0, 0 }  // Minute too high
            ,{{     1,  1,  1,  0,  0, 60,          0}, 0, 0 }  // Second too high
            ,{{     1,  1,  1,  0,  0,  0, 1000000000}, 0, 0 }  // MilliSecond too high
        };

        // Try test cases we expect to fail.
        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii)
        {
            BOOST_CHECK_THROW( dt1 = new TPCE::CDateTime( &(out_of_rangeTestCases[ii].ts) ), std::out_of_range );

            // Clean up in case construction was unexpectedly successful.
            CleanUp( &dt1 );
        }
    }

    void DateTimeTestCases::TestCase_Constructor_Copy()
    {
        TPCE::CDateTime source( 2000, 10, 20, 12, 16, 8, 7 );

        BOOST_CHECK_NO_THROW( dt1 = new TPCE::CDateTime( source ));
        if( dt1 != 0 )
        {
            BOOST_CHECK_EQUAL( source.DayNo(), dt1->DayNo() );
            BOOST_CHECK_EQUAL( source.MSec(), dt1->MSec() );

            CleanUp( &dt1 );
        }
    }

    //
    // Set test cases
    //
    void DateTimeTestCases::TestCase_Set_Default() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::Set() test cases not implemented yet.\n\n"
            "\tThis method uses platform specific time code. Validating this\n"
            "\tcreates a bit of a chicken-or-the-egg situation.\n"
            "\tThis needs to be resolved before meaningful testing can be done.\n\n"
            ;
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Set_DayNumber()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        for( ii = 0; ii < sizeof( validTestCasesDayNumber ) / sizeof( validTestCasesDayNumber[0] ); ++ii )
        {
            BOOST_CHECK_NO_THROW( dt1->Set( validTestCasesDayNumber[ii] ));
            BOOST_CHECK_EQUAL( validTestCasesDayNumber[ii], dt1->DayNo() );
        }

        for( ii = 0; ii < sizeof( out_of_rangeTestCasesDayNumber ) / sizeof( out_of_rangeTestCasesDayNumber[0] ); ++ii )
        {
            BOOST_CHECK_THROW( dt1->Set( out_of_rangeTestCasesDayNumber[ii] ), std::out_of_range );
        }

        CleanUp( &dt1 );
    }

    void DateTimeTestCases::TestCase_Set_YMD()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        TestCaseYMD xCheck;

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCasesYMD ) / sizeof( validTestCasesYMD[0] ); ++ii )
        {
            // Set the object.
            BOOST_CHECK_NO_THROW( dt1->Set( validTestCasesYMD[ii].year, validTestCasesYMD[ii].month, validTestCasesYMD[ii].day ));

            // We can't really check if the construction happened correctly if we can get
            // these values safely - so use REQUIRE level.
            BOOST_REQUIRE_NO_THROW( xCheck.dayNumber = dt1->DayNo());
            BOOST_REQUIRE_NO_THROW( dt1->GetYMD( &(xCheck.year), &(xCheck.month), &(xCheck.day) ));

            BOOST_CHECK_EQUAL( validTestCasesYMD[ii].year, xCheck.year );
            BOOST_CHECK_EQUAL( validTestCasesYMD[ii].month, xCheck.month );
            BOOST_CHECK_EQUAL( validTestCasesYMD[ii].day, xCheck.day );
            BOOST_CHECK_EQUAL( validTestCasesYMD[ii].dayNumber, xCheck.dayNumber );
        }

        // Try test cases we expect to fail.
        for( ii = 0; ii < sizeof( out_of_rangeTestCasesYMD ) / sizeof( out_of_rangeTestCasesYMD[0] ); ++ii)
        {
            // Try to construct the object.
            BOOST_CHECK_THROW( dt1->Set( out_of_rangeTestCasesYMD[ii].year, out_of_rangeTestCasesYMD[ii].month, out_of_rangeTestCasesYMD[ii].day ),
                std::out_of_range 
                );
        }

        CleanUp( &dt1 );
    }

    void DateTimeTestCases::TestCase_Set_YMDHMSM()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        TestCaseYMDHMSM xCheck;

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof(validTestCasesYMDHMSM)/sizeof(validTestCasesYMDHMSM[0]); ++ii )
        {
            BOOST_CHECK_NO_THROW( dt1->Set( validTestCasesYMDHMSM[ii].year,
                validTestCasesYMDHMSM[ii].month, 
                validTestCasesYMDHMSM[ii].day,
                validTestCasesYMDHMSM[ii].hour,
                validTestCasesYMDHMSM[ii].minute,
                validTestCasesYMDHMSM[ii].second,
                validTestCasesYMDHMSM[ii].millisecond
                ));

            // We can't really check if the construction happened correctly if we can get
            // these values safely - so use REQUIRE level.
            BOOST_REQUIRE_NO_THROW( xCheck.dayNumber = dt1->DayNo() );
            BOOST_REQUIRE_NO_THROW( xCheck.msecSoFarToday = dt1->MSec() );
            BOOST_REQUIRE_NO_THROW( dt1->GetYMDHMS( &( xCheck.year ), &( xCheck.month ), &( xCheck.day ), 
                &( xCheck.hour ), &( xCheck.minute ), &( xCheck.second ), &( xCheck.millisecond )));

            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].year, xCheck.year );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].month, xCheck.month );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].day, xCheck.day );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].hour, xCheck.hour );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].minute, xCheck.minute );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].second, xCheck.second );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].millisecond, xCheck.millisecond );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].dayNumber, xCheck.dayNumber );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].msecSoFarToday, xCheck.msecSoFarToday );
        }

        // Try test cases we expect to fail.
        for( ii = 0; ii < sizeof( out_of_rangeTestCasesYMDHMSM ) / sizeof( out_of_rangeTestCasesYMDHMSM[0] ); ++ii)
        {
            BOOST_CHECK_THROW( dt1->Set( out_of_rangeTestCasesYMDHMSM[ii].year,
                out_of_rangeTestCasesYMDHMSM[ii].month, 
                out_of_rangeTestCasesYMDHMSM[ii].day,
                out_of_rangeTestCasesYMDHMSM[ii].hour,
                out_of_rangeTestCasesYMDHMSM[ii].minute,
                out_of_rangeTestCasesYMDHMSM[ii].second,
                out_of_rangeTestCasesYMDHMSM[ii].millisecond
                ), std::out_of_range );
        }

        CleanUp( &dt1 );
    }

    void DateTimeTestCases::TestCase_Set_HMSM()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        TestCaseHMSM xCheck;

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCasesHMSM ) / sizeof( validTestCasesHMSM[0] ); ++ii )
        {
            // Set the object.
            BOOST_CHECK_NO_THROW( dt1->Set( 
                validTestCasesHMSM[ii].hour, 
                validTestCasesHMSM[ii].minute, 
                validTestCasesHMSM[ii].second, 
                validTestCasesHMSM[ii].msec ));

            // We can't really check if the construction happened correctly if we can get
            // these values safely - so use REQUIRE level.
            BOOST_REQUIRE_NO_THROW( xCheck.msecSoFarToday = dt1->MSec());
            BOOST_REQUIRE_NO_THROW( dt1->GetHMS( &( xCheck.hour ), &( xCheck.minute ), &( xCheck.second ), &( xCheck.msec )));

            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].hour, xCheck.hour );
            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].minute, xCheck.minute );
            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].second, xCheck.second );
            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].msec, xCheck.msec );
            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].msecSoFarToday, xCheck.msecSoFarToday );
        }

        // Try test cases we expect to fail.
        for( ii = 0; ii < sizeof( out_of_rangeTestCasesHMSM ) / sizeof( out_of_rangeTestCasesHMSM[0] ); ++ii)
        {
            // Try to construct the object.
            BOOST_CHECK_THROW( dt1->Set( 
                out_of_rangeTestCasesHMSM[ii].hour, 
                out_of_rangeTestCasesHMSM[ii].minute, 
                out_of_rangeTestCasesHMSM[ii].second, 
                out_of_rangeTestCasesHMSM[ii].msec ),
                std::out_of_range 
                );
        }

        CleanUp( &dt1 );
    }

    //
    // Get test cases
    //
    void DateTimeTestCases::TestCase_Get_YMD()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        TestCaseYMD xCheck;

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCasesYMD ) / sizeof( validTestCasesYMD[0] ); ++ii )
        {
            // Set the object.
            BOOST_REQUIRE_NO_THROW( dt1->Set( validTestCasesYMD[ii].year, validTestCasesYMD[ii].month, validTestCasesYMD[ii].day ));

            BOOST_CHECK_NO_THROW( dt1->GetYMD( &( xCheck.year ), &( xCheck.month ), &( xCheck.day )));

            BOOST_CHECK_EQUAL( validTestCasesYMD[ii].year,  xCheck.year );
            BOOST_CHECK_EQUAL( validTestCasesYMD[ii].month, xCheck.month );
            BOOST_CHECK_EQUAL( validTestCasesYMD[ii].day,   xCheck.day );
        }

        CleanUp( &dt1 );
    }

    void DateTimeTestCases::TestCase_Get_YMDHMS()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        TestCaseYMDHMSM xCheck;

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCasesYMDHMSM ) / sizeof( validTestCasesYMDHMSM[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( dt1->Set( validTestCasesYMDHMSM[ii].year,
                validTestCasesYMDHMSM[ii].month, 
                validTestCasesYMDHMSM[ii].day,
                validTestCasesYMDHMSM[ii].hour,
                validTestCasesYMDHMSM[ii].minute,
                validTestCasesYMDHMSM[ii].second,
                validTestCasesYMDHMSM[ii].millisecond
                ));

            BOOST_CHECK_NO_THROW( dt1->GetYMDHMS( &( xCheck.year ), &( xCheck.month ), &( xCheck.day ), 
                &( xCheck.hour ), &( xCheck.minute ), &( xCheck.second ), &( xCheck.millisecond )));

            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].year,          xCheck.year );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].month,         xCheck.month );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].day,           xCheck.day );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].hour,          xCheck.hour );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].minute,        xCheck.minute );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].second,        xCheck.second );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].millisecond,   xCheck.millisecond );
        }

        CleanUp( &dt1 );
    }

    void DateTimeTestCases::TestCase_Get_HMS()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        TestCaseHMSM xCheck;

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof(validTestCasesHMSM)/sizeof(validTestCasesHMSM[0]); ++ii )
        {
            // Set the object.
            BOOST_CHECK_NO_THROW( dt1->Set( 
                validTestCasesHMSM[ii].hour, 
                validTestCasesHMSM[ii].minute, 
                validTestCasesHMSM[ii].second, 
                validTestCasesHMSM[ii].msec ));

            BOOST_CHECK_NO_THROW( dt1->GetHMS( &(xCheck.hour), &(xCheck.minute), &(xCheck.second), &(xCheck.msec) ));

            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].hour, xCheck.hour );
            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].minute, xCheck.minute );
            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].second, xCheck.second );
            BOOST_CHECK_EQUAL( validTestCasesHMSM[ii].msec, xCheck.msec );
        }

        CleanUp( &dt1 );
    }

    void DateTimeTestCases::TestCase_Get_TIMESTAMP_STRUCT()
    {
        size_t ii;  // loop counter

        // Use the REQUIRE level because there is no point in proceeding if
        // we can't successfully construct an object to work with.
        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( 0 ));

        TPCE::TIMESTAMP_STRUCT xCheck;

        // Try test cases we expect to succeed.
        for( ii = 0; ii < sizeof( validTestCasesYMDHMSM ) / sizeof( validTestCasesYMDHMSM[0] ); ++ii )
        {
            BOOST_REQUIRE_NO_THROW( dt1->Set( validTestCasesYMDHMSM[ii].year,
                validTestCasesYMDHMSM[ii].month, 
                validTestCasesYMDHMSM[ii].day,
                validTestCasesYMDHMSM[ii].hour,
                validTestCasesYMDHMSM[ii].minute,
                validTestCasesYMDHMSM[ii].second,
                validTestCasesYMDHMSM[ii].millisecond
                ));

            BOOST_CHECK_NO_THROW( dt1->GetTimeStamp( &xCheck ));

            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].year,                          xCheck.year );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].month,                         xCheck.month );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].day,                           xCheck.day );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].hour,                          xCheck.hour );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].minute,                        xCheck.minute );
            BOOST_CHECK_EQUAL( validTestCasesYMDHMSM[ii].second,                        xCheck.second );
            BOOST_CHECK_EQUAL((( UINT32 )( validTestCasesYMDHMSM[ii].millisecond )),    xCheck.fraction/1000000 );
        }

        CleanUp( &dt1 );
    }

#ifdef COMPILE_ODBC_LOAD
    void DateTimeTestCases::TestCase_Get_DBDATETIME() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::GetDBDATETIME() test cases not implemented yet.\n\n";
        BOOST_MESSAGE( msg );
    }
#endif //COMPILE_ODBC_LOAD

    //
    // Day Number Calculation test case
    //
    void DateTimeTestCases::TestCase_YMDtoDayno()
    {
        // Used to capture year/month/day inputs and the expected day-number output.
        typedef struct tagTestCase
        {
            INT32 year;
            INT32 month;
            INT32 day;
            INT32 dayNumber;
        } TestCase;

        // By definition (in CDateTime) January 1, 0001 is day number 0.
        // Since it is 0-based, subtract 1 from (most) expected day-numbers.
        TestCase validTestCases[] = {
             { 0001, 01, 01,             0 }    // Base day
            ,{ 0001, 02, 28,       31+28-1 }    // First non-leap year
            ,{ 0001, 12, 31,         365-1 }    // Last day of first year
            ,{ 0004, 02, 29, 365*3+31+29-1 }    // First leap day
        };

        size_t ii;  // loop counter

        for( ii = 0; ii < sizeof( validTestCases ) / sizeof( validTestCases[0] ); ++ii )
        {
            // YMDtoDayno is 0 based, so subtract 1 from the answer.
            BOOST_CHECK_EQUAL( 
                TPCE::CDateTime::YMDtoDayno( validTestCases[ii].year, validTestCases[ii].month, validTestCases[ii].day ), 
                validTestCases[ii].dayNumber
                );
        }

        // These test cases are expected to throw std::out_of_range exceptions.
        // Because we're testing exceptions, the expected day-number doesn't matter.
        TestCase out_of_rangeTestCases[] = {
             { 0000, 01, 01, 0 }                    // Year too low
            ,{ 0001, 00, 01, 0 }                // Month too low
            ,{ 0001, 01, 00, 0 }                // Day too low
            // range checking inside CDateTime is currently inconsistent.
            // IsValid checks are slightly different than asserts-converted-to-throws-out_of_range
            //,{ 10000, 01, 01, 0 }                // Year too high
            ,{ 0001, 13, 01, 0 }                // Month too high
            ,{ 0001, 01, 32, 0 }                // Day too high
        };

        for( ii = 0; ii < sizeof( out_of_rangeTestCases ) / sizeof( out_of_rangeTestCases[0] ); ++ii )
        {
            // YMDtoDayno is 0 based, so subtract 1 from the answer.
            BOOST_CHECK_THROW( 
                TPCE::CDateTime::YMDtoDayno( out_of_rangeTestCases[ii].year, out_of_rangeTestCases[ii].month, out_of_rangeTestCases[ii].day ), 
                std::out_of_range 
                );
        }

    }

    //
    // Character array test case
    //
    void DateTimeTestCases::TestCase_ToStr() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::ToStr() test cases not implemented yet.\n\n";
        BOOST_MESSAGE( msg );
    }

    //
    // Add test cases
    //
    void DateTimeTestCases::TestCase_Add_DM() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::Add( days, msec, weekend ) test cases not implemented yet.\n\n"
            "\tThis method does no input validation. This means that it is very easy to\n"
            "\tcompromise a CDateTime object. Input validation needs to be implemented - this\n"
            "\trequires a few design decisions to be made first.\n"
            "\tThis needs to be resolved before meaningful testing can be done.\n\n"
            ;
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Add_Minutes() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::AddMinutes( minutes ) test cases not implemented yet.\n\n"
            "\tThis method does no input validation. This means that it is very easy to\n"
            "\tcompromise a CDateTime object. Input validation needs to be implemented - this\n"
            "\trequires a few design decisions to be made first.\n"
            "\tThis needs to be resolved before meaningful testing can be done.\n\n"
            ;
        BOOST_MESSAGE( msg );
    }
    void DateTimeTestCases::TestCase_Add_WorkMs() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::AddWorkMs( workMs ) test cases not implemented yet.\n\n"
            "\tThis method does no input validation. This means that it is very easy to\n"
            "\tcompromise a CDateTime object. Input validation needs to be implemented - this\n"
            "\trequires a few design decisions to be made first.\n"
            "\tThis needs to be resolved before meaningful testing can be done.\n\n"
            ;
        BOOST_MESSAGE( msg );
    }

    //
    // Operator test cases
    //
    void DateTimeTestCases::TestCase_Operator_LessThan()
    {
        TestCaseYMDHMSM base = { 1976, 7, 4, 13, 14, 15, 167};

        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));
        BOOST_REQUIRE_NO_THROW( dt2 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));

        // dt1 == dt2
        BOOST_CHECK( ! ( *dt1 < *dt2 ));

        // dt1 < dt2 by one day.
        dt1->Set( base.year, base.month, base.day - 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( *dt1 < *dt2 );

        // dt1 < dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond - 1 );
        BOOST_CHECK( *dt1 < *dt2 );

        // dt1 > dt2 by one day.
        dt1->Set( base.year, base.month, base.day + 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( ! ( *dt1 < *dt2 ));

        // dt1 > dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond + 1 );
        BOOST_CHECK( ! ( *dt1 < *dt2 ));

        CleanUp( &dt1 );
        CleanUp( &dt2 );
    }

    void DateTimeTestCases::TestCase_Operator_LessThanOrEqual()
    {
        TestCaseYMDHMSM base = { 1976, 7, 4, 13, 14, 15, 167};

        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));
        BOOST_REQUIRE_NO_THROW( dt2 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));

        // dt1 == dt2
        BOOST_CHECK( *dt1 <= *dt2 );

        // dt1 < dt2 by one day.
        dt1->Set( base.year, base.month, base.day - 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( *dt1 <= *dt2 );

        // dt1 < dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond - 1 );
        BOOST_CHECK( *dt1 <= *dt2 );

        // dt1 > dt2 by one day.
        dt1->Set( base.year, base.month, base.day + 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( ! ( *dt1 <= *dt2 ));

        // dt1 > dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond + 1 );
        BOOST_CHECK( ! ( *dt1 <= *dt2 ));

        CleanUp( &dt1 );
        CleanUp( &dt2 );
    }

    void DateTimeTestCases::TestCase_Operator_GreaterThan() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::operator<= test cases not implemented yet.\n\n";
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Operator_GreaterThanOrEqual()
    {
        TestCaseYMDHMSM base = { 1976, 7, 4, 13, 14, 15, 167};

        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));
        BOOST_REQUIRE_NO_THROW( dt2 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));

        // dt1 == dt2
        BOOST_CHECK( *dt1 >= *dt2 );

        // dt1 < dt2 by one day.
        dt1->Set( base.year, base.month, base.day - 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( ! ( *dt1 >= *dt2 ));

        // dt1 < dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond - 1 );
        BOOST_CHECK( ! ( *dt1 >= *dt2 ));

        // dt1 > dt2 by one day.
        dt1->Set( base.year, base.month, base.day + 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( *dt1 >= *dt2 );

        // dt1 > dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond + 1 );
        BOOST_CHECK( *dt1 >= *dt2 );

        CleanUp( &dt1 );
        CleanUp( &dt2 );
    }

    void DateTimeTestCases::TestCase_Operator_Equivalent()
    {
        TestCaseYMDHMSM base = { 1976, 7, 4, 13, 14, 15, 167};

        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));
        BOOST_REQUIRE_NO_THROW( dt2 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));

        // dt1 == dt2
        BOOST_CHECK( *dt1 >= *dt2 );

        // dt1 < dt2 by one day.
        dt1->Set( base.year, base.month, base.day - 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( ! ( *dt1 == *dt2 ));

        // dt1 < dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond - 1 );
        BOOST_CHECK( ! ( *dt1 == *dt2 ));

        // dt1 > dt2 by one day.
        dt1->Set( base.year, base.month, base.day + 1, base.hour, base.minute, base.second, base.millisecond );
        BOOST_CHECK( ! ( *dt1 == *dt2 ));

        // dt1 > dt2 by one millisecond.
        dt1->Set( base.year, base.month, base.day, base.hour, base.minute, base.second, base.millisecond + 1 );
        BOOST_CHECK( ! ( *dt1 == *dt2 ));

        CleanUp( &dt1 );
        CleanUp( &dt2 );
    }

    void DateTimeTestCases::TestCase_Operator_Minus() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::operator- test cases not implemented yet.\n\n";
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Operator_DiffInMilliSecondsReference() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::DiffInMilliSeconds( CDateTime& ) test cases not implemented yet.\n\n";
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Operator_DiffInMilliSecondsPointer() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::DiffInMilliSeconds( CDateTime* ) test cases not implemented yet.\n\n";
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Operator_PlusEquals() const
    {
        std::string msg;
        msg = "\n\n\tCDateTime::Operator+= test cases not implemented yet.\n\n"
            "\tThis method does no input validation. This means that it is very easy to\n"
            "\tcompromise a CDateTime object. Input validation needs to be implemented - this\n"
            "\trequires a few design decisions to be made first.\n"
            "\tThis needs to be resolved before meaningful testing can be done.\n\n"
            ;
        BOOST_MESSAGE( msg );
    }

    void DateTimeTestCases::TestCase_Operator_Assignment()
    {
        TestCaseYMDHMSM base = { 1976, 7, 4, 13, 14, 15, 167};

        BOOST_REQUIRE_NO_THROW( dt1 = new TPCE::CDateTime( base.year - 1, base.month - 1, base.day - 1,
            base.hour - 1, base.minute - 1, base.second - 1, base.millisecond - 1 ));
        BOOST_REQUIRE_NO_THROW( dt2 = new TPCE::CDateTime( base.year, base.month, base.day,
            base.hour, base.minute, base.second, base.millisecond ));

        // dt1 != dt2
        BOOST_CHECK( ! (*dt1 == *dt2) );

        *dt1 = *dt2;

        // dt1 == dt2
        BOOST_CHECK( (*dt1 == *dt2) );

        CleanUp( &dt1 );
        CleanUp( &dt2 );
    }








    ///////////////////////////////////////////////////////////////////////
    //
    // These are not currently used.
    //
    //bool DateTimeTestCases::IsLeapYear( INT32 year ) const
    //{
    //    if( year % 400 == 0 )
    //    {
    //        return true;
    //    }
    //    else if( year % 100 == 0 )
    //    {
    //        return false;
    //    }
    //    else if( year % 4 == 0 )
    //    {
    //        return true;
    //    }
    //    else
    //    {
    //        return false;
    //    }
    //}

    //INT32 DateTimeTestCases::CountDaysThroughYearEnd( INT32 year ) const
    //{
    //    INT32 dayCount = year*365;

    //    for( int ii = 1; ii <= year; ++ii )
    //    {
    //        if( IsLeapYear(ii) )
    //        {
    //            ++dayCount;
    //        }
    //    }
    //    return dayCount;
    //}
    //
    ///////////////////////////////////////////////////////////////////////

} // namespace EGenUtilitiesTest
