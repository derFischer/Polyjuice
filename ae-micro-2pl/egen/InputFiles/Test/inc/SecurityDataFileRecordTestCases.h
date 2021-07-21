#ifndef SECURITY_DATA_FILE_RECORD_TEST_CASES_H
#define SECURITY_DATA_FILE_RECORD_TEST_CASES_H

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

#include <deque>
#include <string>

#include "../../inc/SecurityDataFileRecord.h"

namespace EGenInputFilesTest
{

    class SecurityDataFileRecordTestCases
    {
    private:
        std::string s_id;
        std::string s_st_id;
        std::string s_symb;
        std::string s_issue;
        std::string s_ex_id;
        std::string s_co_id;
        std::deque<std::string> fields;

        ///////////////////////////////////////////////////////////////////////
        //
        // Used for on-the-fly construction of the tested objects
        //
        TPCE::SecurityDataFileRecord* dfr1;

    public:
        //
        // Constructor / Destructor
        //
        SecurityDataFileRecordTestCases();
        ~SecurityDataFileRecordTestCases();

        //
        // Add test cases to the test suite.
        //
        void AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< SecurityDataFileRecordTestCases > tester ) const;

        ///////////////////////////////////////////////////////////////////////
        //
        // This section contains all test case declarations.
        //

        //
        // Currently, all test cases are handled via utility templates.
        //
    };

} // namespace EGenUtilitiesTest

#endif // SECURITY_DATA_FILE_RECORD_TEST_CASES_H