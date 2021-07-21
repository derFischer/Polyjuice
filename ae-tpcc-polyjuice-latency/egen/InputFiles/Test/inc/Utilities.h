#ifndef INPUT_FILES_TEST_UTILITIES_H
#define INPUT_FILES_TEST_UTILITIES_H

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
#include <deque>
#include <stdexcept>
#include <string>

#include "../../../Test/inc/TestUtilities.h"

namespace EGenInputFilesTest
{
    template< class DataFileRecordType > void TestCase_DFRConstructor(const std::deque<std::string>& fields)
    {
        DataFileRecordType* dfr=NULL;

        std::deque<std::string> tmpFields;

        // Should fail with too few fields
        BOOST_CHECK_THROW( dfr = new DataFileRecordType(tmpFields), std::runtime_error );
        EGenTestCommon::CleanUp(&dfr); // Just in case it erroneously succeeds.

        tmpFields = fields;

        // Should work.
        BOOST_CHECK_NO_THROW( dfr = new DataFileRecordType(tmpFields) );
        EGenTestCommon::CleanUp(&dfr);

        // Should fail with too many fields
        tmpFields.push_back("junk");
        BOOST_CHECK_THROW( dfr = new DataFileRecordType(tmpFields), std::runtime_error );
        EGenTestCommon::CleanUp(&dfr); // Just in case it erroneously succeeds.

        // Should fail but isn't handled - field format errors (e.g. "foo" instead of "1")
    }

    template<class DataFileRecordType> void TestCase_DFRFieldString(const std::deque<std::string>& fields, const std::string& (DataFileRecordType::* pFieldAccessor)() const, const std::string& answer)
    {
        DataFileRecordType* dfr=NULL;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfr = new DataFileRecordType(fields) );

        BOOST_CHECK_EQUAL(answer, (dfr->*pFieldAccessor)());
        EGenTestCommon::CleanUp(&dfr);
    }

    template<class DataFileRecordType, class AnswerType> void TestCase_DFRField(const std::deque<std::string>& fields, AnswerType (DataFileRecordType::* pFieldAccessor)() const, AnswerType answer)
    {
        DataFileRecordType* dfr=NULL;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfr = new DataFileRecordType(fields) );

        BOOST_CHECK_EQUAL(answer, (dfr->*pFieldAccessor)());
        EGenTestCommon::CleanUp(&dfr);
    }

    template<class DataFileRecordType> void TestCase_DFRToString(const std::deque<std::string>& fields)
    {
        DataFileRecordType* dfr=NULL;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfr = new DataFileRecordType(fields) );

        BOOST_CHECK_EQUAL(fields[0], dfr->ToString());

        EGenTestCommon::CleanUp(&dfr);
    }

    template<class DataFileRecordType> void TestCase_DFRMultiFieldToString(const std::deque<std::string>& fields)
    {
        DataFileRecordType* dfr=NULL;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfr = new DataFileRecordType(fields) );

        // Default seperator
        // This works for single field and multi-field DFRs.
        char fieldSeparator = '\t';
        std::string answer = fields[0];
        for( int i = 1; i<fields.size(); ++i)
        {
            answer += fieldSeparator + fields[i];
        }
        BOOST_CHECK_EQUAL(answer, dfr->ToString());

        // Custom seperator
        // This is only relevant for multi-field DFRs.
        if( 1 < fields.size() )
        {
            fieldSeparator = '|';
            answer = fields[0];
            for( int i = 1; i<fields.size(); ++i)
            {
                answer += fieldSeparator + fields[i];
            }
            BOOST_CHECK_EQUAL(answer, dfr->ToString(fieldSeparator));
        }

        EGenTestCommon::CleanUp(&dfr);
    }
} // EGenInputFilesTest
#endif // TEST_UTILITIES_H