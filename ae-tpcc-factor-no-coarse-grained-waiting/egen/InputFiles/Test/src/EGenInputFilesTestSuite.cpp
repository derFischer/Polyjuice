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
#include "../inc/EGenInputFilesTestSuite.h"

//
// Include application headers
//
#include "../inc/StreamSplitterTestCases.h"
#include "../inc/StringSplitterTestCases.h"
#include "../inc/TextFileSplitterTestCases.h"

#include "../inc/AreaCodeDataFileRecordTestCases.h"
#include "../inc/ChargeDataFileRecordTestCases.h"
#include "../inc/CommissionRateDataFileRecordTestCases.h"
#include "../inc/CompanyCompetitorDataFileRecordTestCases.h"
#include "../inc/CompanyDataFileRecordTestCases.h"
#include "../inc/CompanySPRateDataFileRecordTestCases.h"
#include "../inc/ExchangeDataFileRecordTestCases.h"
#include "../inc/FemaleFirstNameDataFileRecordTestCases.h"
#include "../inc/IndustryDataFileRecordTestCases.h"
#include "../inc/LastNameDataFileRecordTestCases.h"
#include "../inc/MaleFirstNameDataFileRecordTestCases.h"
#include "../inc/NewsDataFileRecordTestCases.h"
#include "../inc/NonTaxableAccountNameDataFileRecordTestCases.h"
#include "../inc/SectorDataFileRecordTestCases.h"
#include "../inc/SecurityDataFileRecordTestCases.h"
#include "../inc/StatusTypeDataFileRecordTestCases.h"
#include "../inc/StreetNameDataFileRecordTestCases.h"
#include "../inc/StreetSuffixDataFileRecordTestCases.h"
#include "../inc/TaxableAccountNameDataFileRecordTestCases.h"
#include "../inc/TaxRateCountryDataFileRecordTestCases.h"
#include "../inc/TaxRateDivisionDataFileRecordTestCases.h"
#include "../inc/TradeTypeDataFileRecordTestCases.h"
#include "../inc/ZipCodeDataFileRecordTestCases.h"

#include "../inc/DataFileTestCases.h"
#include "../inc/WeightedDataFileTestCases.h"
#include "../inc/BucketedDataFileTestCases.h"

#include "../inc/CompanyCompetitorFileTestCases.h"
#include "../inc/CompanyFileTestCases.h"
#include "../inc/SecurityFileTestCases.h"
#include "../inc/TaxRateFileTestCases.h"

#include "../inc/DataFileManagerTestCases.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{

    //
    // Constructor / Destructor
    //
    EGenInputFilesTestSuite::EGenInputFilesTestSuite()
    {
    }

    EGenInputFilesTestSuite::~EGenInputFilesTestSuite()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void EGenInputFilesTestSuite::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< EGenInputFilesTestSuite > tester )
    {
        //
        // Load StreamSplitter Test Suite
        //
        TestSuiteBuilder< StreamSplitterTestCases > streamSplitterTestSuite( "StreamSplitter Test Suite" );
        testSuite->add( streamSplitterTestSuite.TestSuite() );

        //
        // Load StringSplitter Test Suite
        //
        TestSuiteBuilder< StringSplitterTestCases > stringSplitterTestSuite( "StringSplitter Test Suite" );
        testSuite->add( stringSplitterTestSuite.TestSuite() );

        //
        // Load TextFileSplitter Test Suite
        //
        TestSuiteBuilder< TextFileSplitterTestCases > textFileSplitterTestSuite( "TextFileSplitter Test Suite" );
        testSuite->add( textFileSplitterTestSuite.TestSuite() );

        //
        // Load AreaCodeDataFileRecord Test Suite
        //
        TestSuiteBuilder< AreaCodeDataFileRecordTestCases > areaCodeDataFileRecordTestSuite( "AreaCodeDataFileRecord Test Suite" );
        //testSuite->add( areaCodeDataFileRecordTestSuite.TestSuite() );

        //
        // Load ChargeDataFileRecord Test Suite
        //
        TestSuiteBuilder< ChargeDataFileRecordTestCases > chargeDataFileRecordTestSuite( "ChargeDataFileRecord Test Suite" );
        testSuite->add( chargeDataFileRecordTestSuite.TestSuite() );

        //
        // Load CommissionRateDataFileRecord Test Suite
        //
        TestSuiteBuilder< CommissionRateDataFileRecordTestCases > commissionRateDataFileRecordTestSuite( "CommissionRateDataFileRecord Test Suite" );
        testSuite->add( commissionRateDataFileRecordTestSuite.TestSuite() );

        //
        // Load CompanyCompetitorDataFileRecord Test Suite
        //
        TestSuiteBuilder< CompanyCompetitorDataFileRecordTestCases > companyCompetitorDataFileRecordTestSuite( "CompanyCompetitorDataFileRecord Test Suite" );
        testSuite->add( companyCompetitorDataFileRecordTestSuite.TestSuite() );

        //
        // Load CompanyDataFileRecord Test Suite
        //
        TestSuiteBuilder< CompanyDataFileRecordTestCases > companyDataFileRecordTestSuite( "CompanyDataFileRecord Test Suite" );
        testSuite->add( companyDataFileRecordTestSuite.TestSuite() );

        //
        // Load CompanySPRateDataFileRecord Test Suite
        //
        TestSuiteBuilder< CompanySPRateDataFileRecordTestCases > companySPRateDataFileRecordTestSuite( "CompanySPRateDataFileRecord Test Suite" );
        testSuite->add( companySPRateDataFileRecordTestSuite.TestSuite() );

        //
        // Load ExchangeDataFileRecord Test Suite
        //
        TestSuiteBuilder< ExchangeDataFileRecordTestCases > exchangeDataFileRecordTestSuite( "ExchangeDataFileRecord Test Suite" );
        testSuite->add( exchangeDataFileRecordTestSuite.TestSuite() );

        //
        // Load FemaleFirstNameDataFileRecord Test Suite
        //
        TestSuiteBuilder< FemaleFirstNameDataFileRecordTestCases > femaleFirstNameDataFileRecordTestSuite( "FemaleFirstNameDataFileRecord Test Suite" );
        testSuite->add( femaleFirstNameDataFileRecordTestSuite.TestSuite() );

        //
        // Load IndustryDataFileRecord Test Suite
        //
        TestSuiteBuilder< IndustryDataFileRecordTestCases > industryDataFileRecordTestSuite( "IndustryDataFileRecord Test Suite" );
        testSuite->add( industryDataFileRecordTestSuite.TestSuite() );

        //
        // Load LastNameDataFileRecord Test Suite
        //
        TestSuiteBuilder< LastNameDataFileRecordTestCases > lastNameDataFileRecordTestSuite( "LastNameDataFileRecord Test Suite" );
        testSuite->add( lastNameDataFileRecordTestSuite.TestSuite() );

        //
        // Load MaleFirstNameDataFileRecord Test Suite
        //
        TestSuiteBuilder< MaleFirstNameDataFileRecordTestCases > maleFirstNameDataFileRecordTestSuite( "MaleFirstNameDataFileRecord Test Suite" );
        testSuite->add( maleFirstNameDataFileRecordTestSuite.TestSuite() );

        //
        // Load NewsDataFileRecord Test Suite
        //
        TestSuiteBuilder< NewsDataFileRecordTestCases > newsDataFileRecordTestSuite( "NewsDataFileRecord Test Suite" );
        testSuite->add( newsDataFileRecordTestSuite.TestSuite() );

        //
        // Load NonTaxableAccountNameDataFileRecord Test Suite
        //
        TestSuiteBuilder< NonTaxableAccountNameDataFileRecordTestCases > nonTaxableAccountNameDataFileRecordTestSuite( "NonTaxableAccountNameDataFileRecord Test Suite" );
        testSuite->add( nonTaxableAccountNameDataFileRecordTestSuite.TestSuite() );

        //
        // Load SectorDataFileRecord Test Suite
        //
        TestSuiteBuilder< SectorDataFileRecordTestCases > sectorDataFileRecordTestSuite( "SectorDataFileRecord Test Suite" );
        testSuite->add( sectorDataFileRecordTestSuite.TestSuite() );

        //
        // Load SecurityDataFileRecord Test Suite
        //
        TestSuiteBuilder< SecurityDataFileRecordTestCases > securityDataFileRecordTestSuite( "SecurityDataFileRecord Test Suite" );
        testSuite->add( securityDataFileRecordTestSuite.TestSuite() );

        //
        // Load StatusTypeDataFileRecord Test Suite
        //
        TestSuiteBuilder< StatusTypeDataFileRecordTestCases > statusTypeDataFileRecordTestSuite( "StatusTypeDataFileRecord Test Suite" );
        testSuite->add( statusTypeDataFileRecordTestSuite.TestSuite() );

        //
        // Load StreetNameDataFileRecord Test Suite
        //
        TestSuiteBuilder< StreetNameDataFileRecordTestCases > streetNameDataFileRecordTestSuite( "StreetNameDataFileRecord Test Suite" );
        testSuite->add( streetNameDataFileRecordTestSuite.TestSuite() );

        //
        // Load StreetSuffixDataFileRecord Test Suite
        //
        TestSuiteBuilder< StreetSuffixDataFileRecordTestCases > streetSuffixDataFileRecordTestSuite( "StreetSuffixDataFileRecord Test Suite" );
        testSuite->add( streetSuffixDataFileRecordTestSuite.TestSuite() );

        //
        // Load TaxableAccountNameDataFileRecord Test Suite
        //
        TestSuiteBuilder< TaxableAccountNameDataFileRecordTestCases > taxableAccountNameDataFileRecordTestSuite( "TaxableAccountNameDataFileRecord Test Suite" );
        testSuite->add( taxableAccountNameDataFileRecordTestSuite.TestSuite() );

        //
        // Load TaxRateCountryDataFileRecord Test Suite
        //
        TestSuiteBuilder< TaxRateCountryDataFileRecordTestCases > taxRateCountryDataFileRecordTestSuite( "TaxRateCountryDataFileRecord Test Suite" );
        testSuite->add( taxRateCountryDataFileRecordTestSuite.TestSuite() );

        //
        // Load TaxRateDivisionDataFileRecord Test Suite
        //
        TestSuiteBuilder< TaxRateDivisionDataFileRecordTestCases > taxRateDivisionDataFileRecordTestSuite( "TaxRateDivisionDataFileRecord Test Suite" );
        testSuite->add( taxRateDivisionDataFileRecordTestSuite.TestSuite() );

        //
        // Load TradeTypeDataFileRecord Test Suite
        //
        TestSuiteBuilder< TradeTypeDataFileRecordTestCases > tradeTypeDataFileRecordTestSuite( "TradeTypeDataFileRecord Test Suite" );
        testSuite->add( tradeTypeDataFileRecordTestSuite.TestSuite() );

        //
        // Load ZipCodeDataFileRecord Test Suite
        //
        TestSuiteBuilder< ZipCodeDataFileRecordTestCases > zipCodeDataFileRecordTestSuite( "ZipCodeDataFileRecord Test Suite" );
        testSuite->add( zipCodeDataFileRecordTestSuite.TestSuite() );

        //
        // Load DataFile Test Suite
        //
        TestSuiteBuilder< DataFileTestCases > dataFileTestSuite( "DataFile Test Suite" );
        testSuite->add( dataFileTestSuite.TestSuite() );

        //
        // Load WeightedDataFile Test Suite
        //
        TestSuiteBuilder< WeightedDataFileTestCases > weightedDataFileTestSuite( "WeightedDataFile Test Suite" );
        testSuite->add( weightedDataFileTestSuite.TestSuite() );

        //
        // Load BucketedDataFile Test Suite
        //
        TestSuiteBuilder< BucketedDataFileTestCases > bucketedDataFileTestSuite( "BucketedDataFile Test Suite" );
        testSuite->add( bucketedDataFileTestSuite.TestSuite() );

        //
        // Load CompanyCompetitorFile Test Suite
        //
        TestSuiteBuilder< CompanyCompetitorFileTestCases > companyCompetitorFileTestSuite( "CompanyCompetitorFile Test Suite" );
        testSuite->add( companyCompetitorFileTestSuite.TestSuite() );

        //
        // Load CompanyFile Test Suite
        //
        TestSuiteBuilder< CompanyFileTestCases > companyFileTestSuite( "CompanyFile Test Suite" );
        testSuite->add( companyFileTestSuite.TestSuite() );

        //
        // Load SecurityFile Test Suite
        //
        TestSuiteBuilder< SecurityFileTestCases > securityFileTestSuite( "SecurityFile Test Suite" );
        testSuite->add( securityFileTestSuite.TestSuite() );

        //
        // Load TaxRateFile Test Suite
        //
        TestSuiteBuilder< TaxRateFileTestCases > taxRateFileTestSuite( "TaxRateFile Test Suite" );
        testSuite->add( taxRateFileTestSuite.TestSuite() );

        //
        // Load DataFileManager Test Suite
        //
        TestSuiteBuilder< DataFileManagerTestCases > dataManagerFileTestSuite( "DataFileManager Test Suite" );
        testSuite->add( dataManagerFileTestSuite.TestSuite() );
    }

}