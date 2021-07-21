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
#include "../inc/TaxRateFileTestCases.h"

//
// Include system headers
//

//
// Include application headers
//
#include "../../../Test/inc/TestUtilities.h"
#include "../inc/Utilities.h"
#include "../../inc/StringSplitter.h"
#include "../../inc/TextFileSplitter.h"
#include "../../inc/DataFile.h"
#include "../../inc/DataFileTypes.h"
#include "../../inc/TaxRateFile.h"
#include "../../../Utilities/inc/MiscConsts.h"

using namespace EGenTestCommon;

namespace EGenInputFilesTest
{
    //
    // Constructor / Destructor
    //
    TaxRateFileTestCases::TaxRateFileTestCases()
    {
    }

    TaxRateFileTestCases::~TaxRateFileTestCases()
    {
    }

    //
    // Add test cases to the test suite.
    //
    void TaxRateFileTestCases::AddTestCases( boost::unit_test::test_suite* testSuite, boost::shared_ptr< TaxRateFileTestCases > tester ) const
    {
        AddTestCase( "TaxRateFile: TestCase_Constructor", &TaxRateFileTestCases::TestCase_Constructor, tester, testSuite );
        AddTestCase( "TaxRateFile: TestCase_operatorIndexer", &TaxRateFileTestCases::TestCase_operatorIndexer, tester, testSuite );
        AddTestCase( "TaxRateFile: TestCase_size", &TaxRateFileTestCases::TestCase_size, tester, testSuite );
    }

    //
    // Constructor test cases
    //
    void TaxRateFileTestCases::TestCase_Constructor()
    {
        TPCE::TextFileSplitter* countrySplitter = new TPCE::TextFileSplitter("../../../flat_in/TaxRatesCountry.txt");
        TPCE::TaxRateCountryDataFile_t* cdf = new TPCE::TaxRateCountryDataFile_t(*countrySplitter);
        TPCE::TextFileSplitter* divisionSplitter = new TPCE::TextFileSplitter("../../../flat_in/TaxRatesDivision.txt");
        TPCE::TaxRateDivisionDataFile_t* ddf = new TPCE::TaxRateDivisionDataFile_t(*divisionSplitter);
        TPCE::CTaxRateFile* file;

        // Construct the object.
        BOOST_CHECK_NO_THROW( file = new TPCE::CTaxRateFile(*cdf, *ddf) );

        CleanUp(&file);
        CleanUp(&ddf);
        CleanUp(&divisionSplitter);
        CleanUp(&cdf);
        CleanUp(&countrySplitter);
    }

    void TaxRateFileTestCases::TestCase_operatorIndexer()
    {
        // Test an empty file.
        TPCE::StringSplitter* empty_countrySplitter = new TPCE::StringSplitter("");
        TPCE::TaxRateCountryDataFile_t* empty_cdf = new TPCE::TaxRateCountryDataFile_t(*empty_countrySplitter);
        TPCE::StringSplitter* empty_divisionSplitter = new TPCE::StringSplitter("");
        TPCE::TaxRateDivisionDataFile_t* empty_ddf = new TPCE::TaxRateDivisionDataFile_t(*empty_divisionSplitter);
        TPCE::CTaxRateFile* empty_file;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( empty_file = new TPCE::CTaxRateFile(*empty_cdf, *empty_ddf) );

        // Now test the empty object.
        BOOST_CHECK_THROW( (*empty_file)[0], std::out_of_range );

        CleanUp(&empty_file);
        CleanUp(&empty_ddf);
        CleanUp(&empty_divisionSplitter);
        CleanUp(&empty_cdf);
        CleanUp(&empty_countrySplitter);


        // Test a real file.
        TPCE::TextFileSplitter* countrySplitter = new TPCE::TextFileSplitter("../../../flat_in/TaxRatesCountry.txt");
        TPCE::TaxRateCountryDataFile_t* cdf = new TPCE::TaxRateCountryDataFile_t(*countrySplitter);
        TPCE::TextFileSplitter* divisionSplitter = new TPCE::TextFileSplitter("../../../flat_in/TaxRatesDivision.txt");
        TPCE::TaxRateDivisionDataFile_t* ddf = new TPCE::TaxRateDivisionDataFile_t(*divisionSplitter);
        TPCE::CTaxRateFile* file;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CTaxRateFile(*cdf, *ddf) );

        // Now test the object.
        BOOST_CHECK_EQUAL((*cdf)[0][0].ToString(), (*file)[0].ToString());

        int lastCdfBucketIndex = cdf->size(cdf->BucketsOnly) - 1;
        int lastCdfRecordIndex = (*cdf)[lastCdfBucketIndex].size() - 1;
        int lastCountryRecordIndex = cdf->size() - 1;
        BOOST_CHECK_EQUAL((*cdf)[lastCdfBucketIndex][lastCdfRecordIndex].ToString(), (*file)[lastCountryRecordIndex].ToString());

        int firstDivisionRecordIndex = lastCountryRecordIndex + 1;
        BOOST_CHECK_EQUAL((*ddf)[0][0].ToString(), (*file)[firstDivisionRecordIndex].ToString());

        int lastDdfBucketIndex = ddf->size(ddf->BucketsOnly) - 1;
        int lastDdfRecordIndex = (*ddf)[lastDdfBucketIndex].size() - 1;
        int lastDivisionRecordIndex = cdf->size() + ddf->size() - 1;
        BOOST_CHECK_EQUAL((*ddf)[lastDdfBucketIndex][lastDdfRecordIndex].ToString(), (*file)[lastDivisionRecordIndex].ToString());

        int tooBigIndex = lastDivisionRecordIndex + 1;
        BOOST_CHECK_THROW( (*file)[tooBigIndex], std::out_of_range );
        BOOST_CHECK_THROW( (*file)[file->size()], std::out_of_range );

        CleanUp(&file);
        CleanUp(&ddf);
        CleanUp(&divisionSplitter);
        CleanUp(&cdf);
        CleanUp(&countrySplitter);
    }

    void TaxRateFileTestCases::TestCase_size()
    {
        // Test an empty file.
        TPCE::StringSplitter* empty_countrySplitter = new TPCE::StringSplitter("");
        TPCE::TaxRateCountryDataFile_t* empty_cdf = new TPCE::TaxRateCountryDataFile_t(*empty_countrySplitter);
        TPCE::StringSplitter* empty_divisionSplitter = new TPCE::StringSplitter("");
        TPCE::TaxRateDivisionDataFile_t* empty_ddf = new TPCE::TaxRateDivisionDataFile_t(*empty_divisionSplitter);
        TPCE::CTaxRateFile* empty_file;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( empty_file = new TPCE::CTaxRateFile(*empty_cdf, *empty_ddf) );

        // Now test the empty object.
        BOOST_CHECK_EQUAL( 0, empty_file->size() );

        // Clean up the empty file stuff.
        CleanUp(&empty_file);
        CleanUp(&empty_ddf);
        CleanUp(&empty_divisionSplitter);
        CleanUp(&empty_cdf);
        CleanUp(&empty_countrySplitter);


        // Test a real file.
        TPCE::TextFileSplitter* countrySplitter = new TPCE::TextFileSplitter("../../../flat_in/TaxRatesCountry.txt");
        TPCE::TaxRateCountryDataFile_t* cdf = new TPCE::TaxRateCountryDataFile_t(*countrySplitter);
        TPCE::TextFileSplitter* divisionSplitter = new TPCE::TextFileSplitter("../../../flat_in/TaxRatesDivision.txt");
        TPCE::TaxRateDivisionDataFile_t* ddf = new TPCE::TaxRateDivisionDataFile_t(*divisionSplitter);
        TPCE::CTaxRateFile* file;

        // Construct the object.
        BOOST_REQUIRE_NO_THROW( file = new TPCE::CTaxRateFile(*cdf, *ddf) );

        // Now test the object.
        BOOST_CHECK_EQUAL( cdf->size() + ddf->size(), file->size() );

        // Clean up the real file stuff.
        CleanUp(&file);
        CleanUp(&ddf);
        CleanUp(&divisionSplitter);
        CleanUp(&cdf);
        CleanUp(&countrySplitter);
    }

} // namespace EGenUtilitiesTest
