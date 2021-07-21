#ifndef DATA_FILE_MANAGER_TEST_CASES_H
#define DATA_FILE_MANAGER_TEST_CASES_H

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

#include "../../inc/DataFileManager.h"

namespace EGenInputFilesTest
{

    class DataFileManagerTestCases
    {
    private:
        // For on-the-fly contruction of the test object.
        TPCE::DataFileManager* dfm;

        // For the flat_in directory location.
        std::string dir;

    public:

        //
        // Constructor / Destructor
        //
        DataFileManagerTestCases();
        ~DataFileManagerTestCases();

        //
        // Add test cases to the test suite.
        //
        void AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr<DataFileManagerTestCases> tester ) const;


        ///////////////////////////////////////////////////////////////////////
        //
        // This section contains all test case declarations.
        //
        void TestCase_Constructor();
        void TestCase_loadFileIStream();
        void TestCase_loadFileFileType();

        void TestCase_AreaCodeDataFile();
        void TestCase_ChargeDataFile();
        void TestCase_CommissionRateDataFile();
        void TestCase_CompanyCompetitorDataFile();
        void TestCase_CompanyDataFile();
        void TestCase_CompanySPRateDataFile();
        void TestCase_ExchangeDataFile();
        void TestCase_FemaleFirstNameDataFile();
        void TestCase_IndustryDataFile();
        void TestCase_LastNameDataFile();
        void TestCase_MaleFirstNameDataFile();
        void TestCase_NewsDataFile();
        void TestCase_NonTaxableAccountNameDataFile();
        void TestCase_SectorDataFile();
        void TestCase_SecurityDataFile();
        void TestCase_StatusTypeDataFile();
        void TestCase_StreetNameDataFile();
        void TestCase_StreetSuffixDataFile();
        void TestCase_TaxableAccountNameDataFile();
        void TestCase_TaxRateCountryDataFile();
        void TestCase_TaxRateDivisionDataFile();
        void TestCase_TradeTypeDataFile();
        void TestCase_ZipCodeDataFile();

        void TestCase_CompanyCompetitorFile();
        void TestCase_CompanyFile();
        void TestCase_SecurityFile();
        void TestCase_TaxRateFile();
    };

} // namespace EGenInputFilesTest

#endif // DATA_FILE_MANAGER_TEST_CASES_H
