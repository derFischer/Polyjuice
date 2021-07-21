#ifndef DATE_TIME_TEST_CASES_H
#define DATE_TIME_TEST_CASES_H

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

#include <boost/test/unit_test.hpp>

#include "../../inc/DateTime.h"

namespace EGenUtilitiesTest
{

    class DateTimeTestCases
    {
    private:
        ///////////////////////////////////////////////////////////////////////
        //
        // This section contains definitions of nested types and
        // member function declarations used for various test cases.
        //

        //
        // These members are used for tests involving just day numbers.
        //
        static const INT32 validTestCasesDayNumber[];
        static const INT32 out_of_rangeTestCasesDayNumber[];

        //
        // This type holds year/month/day inputs and the associated day-number.
        //
        typedef struct tagTestCaseYMD
        {
            INT32 year;
            INT32 month;
            INT32 day;
            INT32 dayNumber;
        } TestCaseYMD;

        //
        // These members are used for tests involving year/month/day inputs.
        //
        static const TestCaseYMD validTestCasesYMD[];
        static const TestCaseYMD out_of_rangeTestCasesYMD[];

        //
        // This type holds year/month/day/hour/minute/second/millisecond inputs
        // and the associated day-number/milliseconds.
        //
        typedef struct tagTestCaseYMDHMSM
        {
            INT32 year;
            INT32 month;
            INT32 day;
            INT32 hour;
            INT32 minute;
            INT32 second;
            INT32 millisecond;
            INT32 dayNumber;
            INT32 msecSoFarToday;
        } TestCaseYMDHMSM;

        //
        // These members are used for tests involving year/month/day/hour/minute/second/millisecond inputs.
        //
        static const TestCaseYMDHMSM validTestCasesYMDHMSM[];
        static const TestCaseYMDHMSM out_of_rangeTestCasesYMDHMSM[];

        //
        // This type holds hour/minute/second/millisecond inputs and the associated
        // total number of milliseconds.
        //
        typedef struct tagTestCaseHMSM
        {
            INT32 hour;
            INT32 minute;
            INT32 second;
            INT32 msec;
            INT32 msecSoFarToday;
        } TestCaseHMSM;

        //
        // These members are used for tests involving hour/minute/second/millisecond inputs.
        //
        static const TestCaseHMSM validTestCasesHMSM[];
        static const TestCaseHMSM out_of_rangeTestCasesHMSM[];

        //
        // End of section containing definitions of nested types and
        // member function declarations used for various test cases.
        //
        ///////////////////////////////////////////////////////////////////////



        ///////////////////////////////////////////////////////////////////////
        //
        // These are not currently used.
        //
        //bool IsLeapYear( INT32 year ) const;
        //INT32 CountDaysThroughYearEnd( INT32 year ) const;
        //
        ///////////////////////////////////////////////////////////////////////



        ///////////////////////////////////////////////////////////////////////
        //
        // Used for on-the-fly construction of the tested objects.
        //
        TPCE::CDateTime* dt1;
        TPCE::CDateTime* dt2;
        //
        ///////////////////////////////////////////////////////////////////////

    public:
        //
        // Constructor / Destructor
        //
        DateTimeTestCases();
        ~DateTimeTestCases();

        //
        // Add test cases to the test suite.
        //
        void AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr<DateTimeTestCases> tester ) const;


        ///////////////////////////////////////////////////////////////////////
        //
        // This section contains all test case declarations.
        //

        //
        // Constructor test cases
        //
        void TestCase_Constructor_Default() const;
        void TestCase_Constructor_DayNumber();
        void TestCase_Constructor_YMD();
        void TestCase_Constructor_YMDHMSM();
        void TestCase_Constructor_TimestampStruct();
        void TestCase_Constructor_Copy();

        //
        // Set test cases
        //
        void TestCase_Set_Default() const;
        void TestCase_Set_DayNumber();
        void TestCase_Set_YMD();
        void TestCase_Set_YMDHMSM();
        void TestCase_Set_HMSM();

        //
        // Get test cases
        //
        void TestCase_Get_YMD();
        void TestCase_Get_YMDHMS();
        void TestCase_Get_HMS();
        void TestCase_Get_TIMESTAMP_STRUCT();
#ifdef COMPILE_ODBC_LOAD
        void TestCase_Get_DBDATETIME() const;
#endif //COMPILE_ODBC_LOAD

        //
        // Day Number Calculation test case
        //
        void TestCase_YMDtoDayno();

        //
        // Character array test case
        //
        void TestCase_ToStr() const;

        //
        // Add test cases
        //
        void TestCase_Add_DM() const;
        void TestCase_Add_Minutes() const;
        void TestCase_Add_WorkMs()const ;

        //
        // Operator test cases
        //
        void TestCase_Operator_LessThan();
        void TestCase_Operator_LessThanOrEqual();
        void TestCase_Operator_GreaterThan() const;
        void TestCase_Operator_GreaterThanOrEqual();
        void TestCase_Operator_Equivalent();
        void TestCase_Operator_Minus() const;
        void TestCase_Operator_DiffInMilliSecondsReference() const;
        void TestCase_Operator_DiffInMilliSecondsPointer() const;
        void TestCase_Operator_PlusEquals() const;
        void TestCase_Operator_Assignment();

        //
        // End of section containing all test case declarations.
        //
        ///////////////////////////////////////////////////////////////////////
    };

} // namespace EGenUtilitiesTest

#endif // DATE_TIME_TEST_CASES_H