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
#include "../inc/BigMathTestCases.h"

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
    BigMathTestCases::BigMathTestCases()
    {
    }

    BigMathTestCases::~BigMathTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void BigMathTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< BigMathTestCases > tester ) const
    {
        AddTestCase( "BigMath Test Case - 64x32", &BigMathTestCases::TestCase_64x32, tester, testSuite );
        AddTestCase( "BigMath Test Case - 64x64", &BigMathTestCases::TestCase_64x64, tester, testSuite );
    }

    //
    // 64 x 32 test case
    //
    void BigMathTestCases::TestCase_64x32() const
    {
        //
        // This type holds range/seed inputs and the associated answer.
        // This is used when testing that specified inputs produce the
        // expected answer.
        //
        typedef struct tagTestCaseRSA
        {
            UINT    range;
            UINT64    seed;
            UINT    answer;
        } TestCaseRSA;

        TestCaseRSA validTestCases1[] = {
            //
            // Range of 0 always produces 0 regardless of seed value.
            //
             { 0x00000000, 0x0000000000000000, 0x0 }    // seed = 0
            ,{ 0x00000000, 0x0000000000000001, 0x0 }    // seed = 1
            ,{ 0x00000000, 0x00000000ffffffff, 0x0 }    // seed = 2^32 - 1
            ,{ 0x00000000, 0x0000000100000000, 0x0 }    // seed = 2^32
            ,{ 0x00000000, 0x1000000000000000, 0x0 }    // seed = 2^63
            ,{ 0x00000000, 0xffffffffffffffff, 0x0 }    // seed = 2^64 - 1
            //
            // Seed of 0 always produces 0 regardless of range value.
            //
            ,{ 0x00000000, 0x0000000000000000, 0x0 }    // range = 0
            ,{ 0x00000001, 0x0000000000000000, 0x0 }    // range = 1
            ,{ 0x10000000, 0x0000000000000000, 0x0 }    // range = 2^31
            ,{ 0xffffffff, 0x0000000000000000, 0x0 }    // range = 2^32 - 1
        };

        size_t ii;  // loop counter

        for( ii = 0; ii < sizeof(validTestCases1)/sizeof(validTestCases1[0]); ++ii)
        {
            BOOST_CHECK_EQUAL( 
                TPCE::Mul6432WithShiftRight64( validTestCases1[ii].seed, validTestCases1[ii].range ), 
                validTestCases1[ii].answer );
        }

        //
        // This type holds range/seed inputs.
        //
        typedef struct tagTestCaseRS
        {
            UINT    range;
            UINT64    seed;
        } TestCaseRS;

        TestCaseRS validTestCases2[] = {
            //
            // Using the max value for seed should produce one less than the value of range.
            //
             { 0xabcdabcd, 0xffffffffffffffff }
            ,{ 0xdcbadcba, 0xffffffffffffffff }
            ,{ 0xffffffff, 0xffffffffffffffff }
        };

        for( ii = 0; ii < sizeof( validTestCases2 ) / sizeof( validTestCases2[0]); ++ii )
        {
            BOOST_CHECK_EQUAL( 
                TPCE::Mul6432WithShiftRight64( validTestCases2[ii].seed, validTestCases2[ii].range ), 
                validTestCases2[ii].range - 1 );
        }

        //
        // This type holds a range and two seeds.
        //
        typedef struct tagTestCaseRSS
        {
            UINT    range;
            UINT64  seed1;
            UINT64  seed2;
        } TestCaseRSS;

        TestCaseRSS validTestCases3[] = {
            //
            // If ( SU * RL ) is an integral multiple of 2^32,
            // then SL won't change the value because P0 can't cause a carry.
            //
             { 0x80000000, 0x0000000200000000, 0x00000002ffffffff }
            ,{ 0x80000000, 0xaaaaaaa200000000, 0xaaaaaaa2ffffffff }
            ,{ 0x80000000, 0xabcdabc400000000, 0xabcdabc4ffffffff }
        };

        for( ii = 0; ii < sizeof( validTestCases3 ) / sizeof( validTestCases3[0] ); ++ii)
        {
            BOOST_CHECK_EQUAL( 
                TPCE::Mul6432WithShiftRight64( validTestCases3[ii].seed1, validTestCases3[ii].range ), 
                TPCE::Mul6432WithShiftRight64( validTestCases3[ii].seed2, validTestCases3[ii].range ));
        }

        TestCaseRSS validTestCases4[] = {
            //
            // If (( SU * range ) % 2^32 ) + P0 >= 2^32 then a carry should occur.
            // This results in a value that is 1 greater than when (( SU * range ) % 2^32 ) + P0 < 2^32.
            //
            {  0xffffffff, 0x0000000300000000, 0x00000003ffffffff }
        };

        for( ii = 0; ii < sizeof(validTestCases4)/sizeof(validTestCases4[0]); ++ii)
        {
            BOOST_CHECK_EQUAL( 
                TPCE::Mul6432WithShiftRight64( validTestCases4[ii].seed1, validTestCases4[ii].range ) + 1, 
                TPCE::Mul6432WithShiftRight64( validTestCases4[ii].seed2, validTestCases4[ii].range ));
        }
    }

    //
    // 64 x 64 test case
    //
    void BigMathTestCases::TestCase_64x64() const
    {
        //
        // This type holds range/seed inputs and the associated answer.
        // This is used when testing that specified inputs produce the
        // expected answer.
        //
        typedef struct tagTestCase1
        {
            UINT64    range;
            UINT64    seed;
            UINT64    answer;
        } TestCase1;

        TestCase1 validTestCases1[] = {
            //
            // Range of 0 always produces 0 regardless of seed value.
            //
             { 0x0000000000000000, 0x0000000000000000, 0x0 }    // seed = 0
            ,{ 0x0000000000000000, 0x0000000000000001, 0x0 }    // seed = 1
            ,{ 0x0000000000000000, 0x00000000ffffffff, 0x0 }    // seed = 2^32 - 1
            ,{ 0x0000000000000000, 0x0000000100000000, 0x0 }    // seed = 2^32
            ,{ 0x0000000000000000, 0x1000000000000000, 0x0 }    // seed = 2^63
            ,{ 0x0000000000000000, 0xffffffffffffffff, 0x0 }    // seed = 2^64 - 1
            //
            // Seed of 0 always produces 0 regardless of range value.
            //
            ,{ 0x0000000000000000, 0x0000000000000000, 0x0 }    // range = 0
            ,{ 0x0000000000000001, 0x0000000000000000, 0x0 }    // range = 1
            ,{ 0x0000000010000000, 0x0000000000000000, 0x0 }    // range = 2^31
            ,{ 0x00000000ffffffff, 0x0000000000000000, 0x0 }    // range = 2^32 - 1
            ,{ 0x0000000100000000, 0x0000000000000000, 0x0 }    // range = 2^32
            ,{ 0x1000000000000000, 0x0000000000000000, 0x0 }    // range = 2^63
            ,{ 0xffffffffffffffff, 0x0000000000000000, 0x0 }    // range = 2^64 - 1
        };

        size_t ii;  // loop counter

        for( ii = 0; ii < sizeof( validTestCases1 ) / sizeof( validTestCases1[0] ); ++ii)
        {
            BOOST_CHECK_EQUAL( 
                TPCE::Mul6464WithShiftRight64( validTestCases1[ii].seed, validTestCases1[ii].range ), 
                validTestCases1[ii].answer );
        }

        //
        // This type holds range/seed inputs.
        //
        typedef struct tagTestCase2
        {
            UINT64    range;
            UINT64    seed;
        } TestCase2;

        TestCase2 validTestCases2[] = {
            //
            // Using the max value for seed should produce one less than the value of range.
            //
             { 0xabcdabcdabcdabcd, 0xffffffffffffffff }
            ,{ 0xdcbadcbadcbadcba, 0xffffffffffffffff }
            ,{ 0xffffffffffffffff, 0xffffffffffffffff }
        };

        for( ii = 0; ii < sizeof(validTestCases2)/sizeof(validTestCases2[0]); ++ii)
        {
            BOOST_CHECK_EQUAL( 
                TPCE::Mul6464WithShiftRight64( validTestCases2[ii].seed, validTestCases2[ii].range), 
                validTestCases2[ii].range - 1 );
        }

        //
        // The following values demonstrate various aspects of p12_carry.
        // But I'm not sure there is a clever check that can be done on
        // the return value.
        //
        //UINT64 range    = 0x1234123412341234;    // High-order bit set: None
        //UINT64 seed        = 0x4321432143214321;

        //UINT64 range    = 0xf000000000000000;    // High-order bit set: P2 only
        //UINT64 seed        = 0x00000000f0000000;

        //UINT64 range    = 0xf0000000ffffffff;    // High-order bit set: P2 & S
        //UINT64 seed        = 0x80000000f0000000;

        //UINT64 range    = 0xf0000000ffffffff;    // High-order bit set: P2 & P1 & S
        //UINT64 seed        = 0xfffffffff0000000;

        //UINT64 range    = 0x00000000f0000000;    // High-order bit set: P1 & S but not P2
        //UINT64 seed        = 0xf000000000000000;
    }

} // namespace EGenUtilitiesTest
