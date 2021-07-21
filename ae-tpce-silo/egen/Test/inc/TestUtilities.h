#ifndef TEST_UTILITIES_H
#define TEST_UTILITIES_H

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
#include <boost/bind.hpp>

#include <deque>
#include <string>

namespace EGenTestCommon
{

    //
    // Template helpder function to clean up a pointer pointing to an object allocated by new.
    //
    template< class T > void CleanUp( T **newPtr )
    {
        // Only clean up if there really is something to clean up.
        if( 0 != *newPtr )
        {
            delete *newPtr;
            *newPtr = 0;
        }
    }

    //
    // Template helper function for adding a const member function test case to a test suite.
    //
    template< class T > void AddTestCase( const char *const testCaseName, void ( T::* pTestCaseMethod)() const, boost::shared_ptr< T > tester, boost::unit_test::test_suite* testSuite )
    {
        boost::unit_test::test_case* testCase;

        testCase = BOOST_TEST_CASE( boost::bind( pTestCaseMethod, tester ));
        testCase->p_name.set( testCaseName );
        testSuite->add( testCase );
    }

    //
    // Template helper function for adding a non-const member function test case to a test suite.
    //
    template< class T > void AddTestCase( const char *const testCaseName, void ( T::* pTestCaseMethod )(), boost::shared_ptr< T > tester, boost::unit_test::test_suite* testSuite )
    {
        boost::unit_test::test_case* testCase;

        testCase = BOOST_TEST_CASE( boost::bind( pTestCaseMethod, tester ));
        testCase->p_name.set( testCaseName );
        testSuite->add( testCase );
    }

    //
    // Template helper function for adding a string field test case to a test suite.
    //
    template< class DataFileRecordType > void AddTestCaseField( 
        const char *const testCaseName, 
        void (pTestCaseMethod )(const std::deque<std::string>&, const std::string& (DataFileRecordType::* pFieldAccessor)() const, const std::string&), 
        const std::deque<std::string>& fields,
        const std::string& ( DataFileRecordType::* pFieldAccessor)() const,
        const std::string& answer,
        boost::unit_test::test_suite* testSuite
        )
    {
        boost::unit_test::test_case* testCase;

        testCase = BOOST_TEST_CASE( boost::bind( pTestCaseMethod, fields, pFieldAccessor, answer ));
        testCase->p_name.set( testCaseName );
        testSuite->add( testCase );
    }

    template< class DataFileRecordType, class AnswerType > void AddTestCaseField( 
        const char *const testCaseName, 
        void (pTestCaseMethod )(const std::deque<std::string>&, AnswerType (DataFileRecordType::* pFieldAccessor)() const, AnswerType), 
        const std::deque<std::string>& fields,
        AnswerType ( DataFileRecordType::* pFieldAccessor)() const,
        AnswerType answer,
        boost::unit_test::test_suite* testSuite
        )
    {
        boost::unit_test::test_case* testCase;

        testCase = BOOST_TEST_CASE( boost::bind( pTestCaseMethod, fields, pFieldAccessor, answer ));
        testCase->p_name.set( testCaseName );
        testSuite->add( testCase );
    }

    //template< class DataFileRecordType> void AddTestCaseFieldInt( 
    //    const char *const testCaseName, 
    //    void (pTestCaseMethod )(const std::deque<std::string>&, int (DataFileRecordType::* pFieldAccessor)() const, int), 
    //    const std::deque<std::string>& fields,
    //    int ( DataFileRecordType::* pFieldAccessor)() const,
    //    int answer,
    //    boost::unit_test::test_suite* testSuite
    //    )
    //{
    //    boost::unit_test::test_case* testCase;

    //    testCase = BOOST_TEST_CASE( boost::bind( pTestCaseMethod, fields, pFieldAccessor, answer ));
    //    testCase->p_name.set( testCaseName );
    //    testSuite->add( testCase );
    //}

    //
    // Helper function for adding a free function with one parameter test case to a test suite.
    //
    void AddTestCase( const char *const testCaseName, void (pTestCaseMethod )(const std::deque<std::string>&), const std::deque<std::string>& fields, boost::unit_test::test_suite* testSuite );

    //
    // Compare two values and determine if they are within a specified range.
    //
    bool CloseEnough( double amount1, double amount2, double threshold );

    //
    // Compare two values and determine if they are within a specified range.
    //
    bool DollarAmountsAreEqual( double amount1, double amount2 );

} // namespace EGenTestCommon

#endif // TEST_UTILITIES_H