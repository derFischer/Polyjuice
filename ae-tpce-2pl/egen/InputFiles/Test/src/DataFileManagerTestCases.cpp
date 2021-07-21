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
#include "../inc/DataFileManagerTestCases.h"

//
// Include system headers
//
#include <fstream>

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"
#include "../../inc/DataFileTypes.h"
#include "../../inc/StringSplitter.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    DataFileManagerTestCases::DataFileManagerTestCases()
        : dfm( 0 )
        , dir("../../../flat_in/")
    {
    }

    DataFileManagerTestCases::~DataFileManagerTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void DataFileManagerTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< DataFileManagerTestCases > tester ) const
    {
        AddTestCase( "DataFileManager: TestCase_Constructor", &DataFileManagerTestCases::TestCase_Constructor, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_loadFileIStream", &DataFileManagerTestCases::TestCase_loadFileIStream, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_loadFileFileType", &DataFileManagerTestCases::TestCase_loadFileFileType, tester, testSuite );

        AddTestCase( "DataFileManager: TestCase_AreaCodeDataFile", &DataFileManagerTestCases::TestCase_AreaCodeDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_ChargeDataFile", &DataFileManagerTestCases::TestCase_ChargeDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_CommissionRateDataFile", &DataFileManagerTestCases::TestCase_CommissionRateDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_CompanyCompetitorDataFile", &DataFileManagerTestCases::TestCase_CompanyCompetitorDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_CompanyDataFile", &DataFileManagerTestCases::TestCase_CompanyDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_CompanySPRateDataFile", &DataFileManagerTestCases::TestCase_CompanySPRateDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_ExchangeDataFile", &DataFileManagerTestCases::TestCase_ExchangeDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_FemaleFirstNameDataFile", &DataFileManagerTestCases::TestCase_FemaleFirstNameDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_IndustryDataFile", &DataFileManagerTestCases::TestCase_IndustryDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_LastNameDataFile", &DataFileManagerTestCases::TestCase_LastNameDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_MaleFirstNameDataFile", &DataFileManagerTestCases::TestCase_MaleFirstNameDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_NewsDataFile", &DataFileManagerTestCases::TestCase_NewsDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_NonTaxableAccountNameDataFile", &DataFileManagerTestCases::TestCase_NonTaxableAccountNameDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_SectorDataFile", &DataFileManagerTestCases::TestCase_SectorDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_SecurityDataFile", &DataFileManagerTestCases::TestCase_SecurityDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_StatusTypeDataFile", &DataFileManagerTestCases::TestCase_StatusTypeDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_StreetNameDataFile", &DataFileManagerTestCases::TestCase_StreetNameDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_StreetSuffixDataFile", &DataFileManagerTestCases::TestCase_StreetSuffixDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_TaxableAccountNameDataFile", &DataFileManagerTestCases::TestCase_TaxableAccountNameDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_TaxRateCountryDataFile", &DataFileManagerTestCases::TestCase_TaxRateCountryDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_TaxRateDivisionDataFile", &DataFileManagerTestCases::TestCase_TaxRateDivisionDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_TradeTypeDataFile", &DataFileManagerTestCases::TestCase_TradeTypeDataFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_ZipCodeDataFile", &DataFileManagerTestCases::TestCase_ZipCodeDataFile, tester, testSuite );

        AddTestCase( "DataFileManager: TestCase_CompanyCompetitorFile", &DataFileManagerTestCases::TestCase_CompanyCompetitorFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_CompanyFile", &DataFileManagerTestCases::TestCase_CompanyFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_SecurityFile", &DataFileManagerTestCases::TestCase_SecurityFile, tester, testSuite );
        AddTestCase( "DataFileManager: TestCase_TaxRateFile", &DataFileManagerTestCases::TestCase_TaxRateFile, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void DataFileManagerTestCases::TestCase_Constructor()
    {
        // Construct the object.
        // - use all defaults
        BOOST_CHECK_NO_THROW( dfm = new TPCE::DataFileManager( ) );
        CleanUp( &dfm );

        // Construct the object.
        // - explicit dir
        BOOST_CHECK_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );
        CleanUp( &dfm );

        // Construct the object.
        // - explicit configured customer count
        BOOST_CHECK_NO_THROW( dfm = new TPCE::DataFileManager( dir, 25000 ) );
        CleanUp( &dfm );

        // Construct the object.
        // - explicit configured customer count
        // - explicit active customer count
        BOOST_CHECK_NO_THROW( dfm = new TPCE::DataFileManager( dir, 25000, 20000 ) );
        CleanUp( &dfm );

        // Construct the object.
        // - explicit configured customer count
        // - explicit active customer count
        // - explicit LAZY load
        BOOST_CHECK_NO_THROW( dfm = new TPCE::DataFileManager( dir, 25000, 20000, TPCE::DataFileManager::LAZY_LOAD) );
        CleanUp( &dfm );

        // Construct the object.
        // - explicit configured customer count
        // - explicit active customer count
        // - explicit IMMEDIATE load
        BOOST_CHECK_NO_THROW( dfm = new TPCE::DataFileManager( dir, 25000, 20000, TPCE::DataFileManager::IMMEDIATE_LOAD) );
        CleanUp( &dfm );

        // Construct the object.
        // - bad directory, immediate load
        BOOST_CHECK_THROW( dfm = new TPCE::DataFileManager( "foo", 25000, 20000, TPCE::DataFileManager::IMMEDIATE_LOAD), std::runtime_error );
        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_loadFileIStream()
    {
        // Construct the object without providing a directory
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( ) );
        std::ifstream file("../../../flat_in/AreaCode.txt");

        // This case should work fine.
        BOOST_CHECK_NO_THROW( dfm->loadFile( file, TPCE::AREA_CODE_DATA_FILE ));

        // Reset the file to the beginning.
        file.close();
        file.open("../../../flat_in/AreaCode.txt");

        // This case should NOT work because we're using the area code file instead of the 
        // company file. This is really more a test of the underlying objects rather than the
        // DataFileManager. But it does show that underlying errors are not lost.
        BOOST_CHECK_THROW( dfm->loadFile( file, TPCE::COMPANY_DATA_FILE ), std::runtime_error );

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_loadFileFileType()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // WARNING: This code is "brittle" since it is highly dependant on the enum definition.
        for( int fileType = TPCE::AREA_CODE_DATA_FILE; fileType <= TPCE::ZIPCODE_DATA_FILE; ++fileType )
        {
            BOOST_CHECK_NO_THROW( dfm->loadFile( (TPCE::DataFileType)fileType ));
        }
        CleanUp( &dfm );

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( "foo" ) );

        // WARNING: This code is "brittle" since it is highly dependant on the enum definition.
        for( int fileType = TPCE::AREA_CODE_DATA_FILE; fileType <= TPCE::ZIPCODE_DATA_FILE; ++fileType )
        {
            BOOST_CHECK_THROW( dfm->loadFile( (TPCE::DataFileType)fileType ), std::runtime_error);
        }

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_AreaCodeDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->AreaCodeDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_ChargeDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->ChargeDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_CommissionRateDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->CommissionRateDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_CompanyCompetitorDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->CompanyCompetitorDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_CompanyDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->CompanyDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_CompanySPRateDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->CompanySPRateDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_ExchangeDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->ExchangeDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_FemaleFirstNameDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->FemaleFirstNameDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_IndustryDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->IndustryDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_LastNameDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->LastNameDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_MaleFirstNameDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->MaleFirstNameDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_NewsDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->NewsDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_NonTaxableAccountNameDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->NonTaxableAccountNameDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_SectorDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->SectorDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_SecurityDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->SecurityDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_StatusTypeDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->StatusTypeDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_StreetNameDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->StreetNameDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_StreetSuffixDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->StreetSuffixDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_TaxableAccountNameDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->TaxableAccountNameDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_TaxRateCountryDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->TaxRateCountryDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_TaxRateDivisionDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->TaxRateDivisionDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_TradeTypeDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->TradeTypeDataFile().size() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_ZipCodeDataFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->ZipCodeDataFile().size() > 0);

        CleanUp( &dfm );
    }


    void DataFileManagerTestCases::TestCase_CompanyCompetitorFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->CompanyCompetitorFile().GetConfiguredCompanyCompetitorCount() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_CompanyFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->CompanyFile().GetSize() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_SecurityFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->SecurityFile().GetSize() > 0);

        CleanUp( &dfm );
    }

    void DataFileManagerTestCases::TestCase_TaxRateFile()
    {
        // Construct the object.
        BOOST_REQUIRE_NO_THROW( dfm = new TPCE::DataFileManager( dir ) );

        // Verify accessor (lazy load).
        BOOST_CHECK( dfm->TaxRateFile().size() > 0);

        CleanUp( &dfm );
    }

} // namespace EGenUtilitiesTest
