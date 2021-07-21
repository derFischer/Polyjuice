/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
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
 * - Christopher Chan-Nui
 */

#include <iostream>
#include <boost/test/auto_unit_test.hpp>
#include "../inc/InputFlatFilesDeclarations.h"
#include "../inc/SecurityFile.h"

using boost::unit_test::test_suite;

struct SecurityFixture {
    TPCE::CSecurityFile security_file;
    TIdent num_securities;
    TIdent num_companies;
    TIdent configured_customers;
    TIdent active_customers;

    SecurityFixture(int configured=10000, int active=10000)
	: security_file("../flat_in/Security.txt", configured, active)
	, configured_customers(configured)
	, active_customers(active)
    {
	struct TPCE::TSecurityLimits security_limits;
	num_securities = security_limits.TotalElements();

	struct TPCE::TCompanyLimits company_limits;
	num_companies = company_limits.TotalElements();
    }
};

BOOST_FIXTURE_TEST_SUITE( testsuite_securityfile, SecurityFixture )

BOOST_AUTO_TEST_CASE( test_securitylimits )
{
        BOOST_CHECK_EQUAL( num_securities, 6850 );
        BOOST_CHECK_EQUAL( num_companies, 5000 );
}

static void check_symbol_to_id_mapping(const TPCE::CSecurityFile& security_file, const char *symbolname, TIdent index) {
    char tmpsymbol[256];
    security_file.CreateSymbol( index, tmpsymbol, sizeof(tmpsymbol) ); 
    BOOST_CHECK_EQUAL(symbolname, tmpsymbol);
    BOOST_CHECK_EQUAL(security_file.GetIndex(tmpsymbol), index);
    BOOST_CHECK_EQUAL(security_file.GetId(tmpsymbol), index+1);
}

BOOST_FIXTURE_TEST_CASE( test_securityfile_createsymbol, SecurityFixture )
{
    check_symbol_to_id_mapping(security_file, "ZICA", 0);
    check_symbol_to_id_mapping(security_file, "ZRAN", num_securities-1);
    check_symbol_to_id_mapping(security_file, "ZICA-a", num_securities);
    check_symbol_to_id_mapping(security_file, "ZICA-b", num_securities*2);
    check_symbol_to_id_mapping(security_file, "ZICA-c", num_securities*3);
    check_symbol_to_id_mapping(security_file, "ZICA-d", num_securities*4);
    check_symbol_to_id_mapping(security_file, "ZICA-e", num_securities*5);
    check_symbol_to_id_mapping(security_file, "ZICA-f", num_securities*6);
    check_symbol_to_id_mapping(security_file, "ZICA-g", num_securities*7);
    check_symbol_to_id_mapping(security_file, "ZICA-h", num_securities*8);
    check_symbol_to_id_mapping(security_file, "ZICA-i", num_securities*9);
    check_symbol_to_id_mapping(security_file, "ZICA-j", num_securities*10);
    check_symbol_to_id_mapping(security_file, "ZICA-k", num_securities*11);
    check_symbol_to_id_mapping(security_file, "ZICA-l", num_securities*12);
    check_symbol_to_id_mapping(security_file, "ZICA-m", num_securities*13);
    check_symbol_to_id_mapping(security_file, "ZICA-n", num_securities*14);
    check_symbol_to_id_mapping(security_file, "ZICA-o", num_securities*15);
    check_symbol_to_id_mapping(security_file, "ZICA-p", num_securities*16);
    check_symbol_to_id_mapping(security_file, "ZICA-q", num_securities*17);
    check_symbol_to_id_mapping(security_file, "ZICA-r", num_securities*18);
    check_symbol_to_id_mapping(security_file, "ZICA-s", num_securities*19);
    check_symbol_to_id_mapping(security_file, "ZICA-t", num_securities*20);
    check_symbol_to_id_mapping(security_file, "ZICA-u", num_securities*21);
    check_symbol_to_id_mapping(security_file, "ZICA-v", num_securities*22);
    check_symbol_to_id_mapping(security_file, "ZICA-w", num_securities*23);
    check_symbol_to_id_mapping(security_file, "ZICA-x", num_securities*24);
    check_symbol_to_id_mapping(security_file, "ZICA-y", num_securities*25);
    check_symbol_to_id_mapping(security_file, "ZICA-z", num_securities*26);
    // The extra +1s come because non-existant characters take up 1 base 26 worth of space
    check_symbol_to_id_mapping(security_file, "ZICA-aa", num_securities*(26+1));
    check_symbol_to_id_mapping(security_file, "ZICA-az", num_securities*(26+26));
    check_symbol_to_id_mapping(security_file, "ZICA-ba", num_securities*(26*2+1));
    check_symbol_to_id_mapping(security_file, "ZICA-ca", num_securities*(26*3+1));
    check_symbol_to_id_mapping(security_file, "ZICA-za", num_securities*(26*26+1));
    check_symbol_to_id_mapping(security_file, "ZICA-aaa", num_securities*(26*(26+1)+1));
    check_symbol_to_id_mapping(security_file, "ZICA-aaaa", num_securities*(26*(26*(26+1)+1)+1));
    check_symbol_to_id_mapping(security_file, "ZICA-aaaaaaa", num_securities*(26*(26*(26*(26*(26*(26+1)+1)+1)+1)+1)+1));
    // This will fail for suffixes larger than 7 characters in length
}

static int num_securities_for_customer_count(int count) {
    return count * TPCE::iOneLoadUnitSecurityCount / TPCE::iDefaultLoadUnitSize;
}

BOOST_AUTO_TEST_CASE( test_securityfile_securitycount )
{
    BOOST_CHECK_EQUAL(security_file.GetSize(), num_securities_for_customer_count(configured_customers));
    BOOST_CHECK_EQUAL(security_file.GetConfiguredSecurityCount(), num_securities_for_customer_count(configured_customers));
    BOOST_CHECK_EQUAL(security_file.GetActiveSecurityCount(), num_securities_for_customer_count(active_customers));

    security_file.SetConfiguredSecurityCount(20000);
    BOOST_CHECK_EQUAL(security_file.GetSize(), num_securities_for_customer_count(20000));
    BOOST_CHECK_EQUAL(security_file.GetConfiguredSecurityCount(), num_securities_for_customer_count(20000));
    BOOST_CHECK_EQUAL(security_file.GetActiveSecurityCount(), num_securities_for_customer_count(active_customers));

    security_file.SetConfiguredSecurityCount(50000);
    BOOST_CHECK_EQUAL(security_file.GetSize(), num_securities_for_customer_count(50000));
    BOOST_CHECK_EQUAL(security_file.GetConfiguredSecurityCount(), num_securities_for_customer_count(50000));
    BOOST_CHECK_EQUAL(security_file.GetActiveSecurityCount(), num_securities_for_customer_count(active_customers));

    security_file.SetActiveSecurityCount(70000);
    BOOST_CHECK_EQUAL(security_file.GetSize(), num_securities_for_customer_count(50000));
    BOOST_CHECK_EQUAL(security_file.GetConfiguredSecurityCount(), num_securities_for_customer_count(50000));
    BOOST_CHECK_EQUAL(security_file.GetActiveSecurityCount(), num_securities_for_customer_count(70000));
}

BOOST_AUTO_TEST_CASE( test_securityfile_exchangeid )
{
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(0), TPCE::ePCX);
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(2), TPCE::eNYSE);
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(3), TPCE::eAMEX);
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(4), TPCE::eNASDAQ);
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(0+num_securities), TPCE::ePCX);
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(2+num_securities), TPCE::eNYSE);
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(3+num_securities), TPCE::eAMEX);
    BOOST_CHECK_EQUAL(security_file.GetExchangeIndex(4+num_securities), TPCE::eNASDAQ);
}

BOOST_AUTO_TEST_CASE( test_securityfile_companyid )
{
    BOOST_CHECK_EQUAL(security_file.GetCompanyId(6841), 4995 + TPCE::iTIdentShift);
    BOOST_CHECK_EQUAL(security_file.GetCompanyId(6841), security_file.GetCompanyIndex(6841)+TPCE::iTIdentShift+1);
    BOOST_CHECK_EQUAL(security_file.GetCompanyId(6841+num_securities), 4995 + TPCE::iTIdentShift + num_companies);
    BOOST_CHECK_EQUAL(security_file.GetCompanyId(6841+num_securities*2), 4995 + TPCE::iTIdentShift + num_companies*2);
}

BOOST_AUTO_TEST_CASE( test_securityfile_calculations )
{

    BOOST_CHECK_EQUAL(security_file.CalculateSecurityCount(5000), num_securities_for_customer_count(5000));
    BOOST_CHECK_EQUAL(security_file.CalculateSecurityCount(10000), num_securities_for_customer_count(10000));
    BOOST_CHECK_EQUAL(security_file.CalculateSecurityCount(1000000), num_securities_for_customer_count(1000000));

    BOOST_CHECK_EQUAL(security_file.CalculateStartFromSecurity(5000), num_securities_for_customer_count(5000));
    BOOST_CHECK_EQUAL(security_file.CalculateStartFromSecurity(10000), num_securities_for_customer_count(10000));
    BOOST_CHECK_EQUAL(security_file.CalculateStartFromSecurity(1000000), num_securities_for_customer_count(1000000));
}

BOOST_AUTO_TEST_CASE( test_securityfile_fetch_row )
{
    const TPCE::TSecurityInputRow* row = security_file.GetRecord(8);
    BOOST_CHECK_EQUAL(row->S_ID, 9);
    BOOST_CHECK_EQUAL(row->S_ST_ID, "ACTV");
    BOOST_CHECK_EQUAL(row->S_ISSUE, "PREF_B");
    BOOST_CHECK_EQUAL(row->S_SYMB, "NDNPRB");
    BOOST_CHECK_EQUAL(row->S_EX_ID, "AMEX");
    BOOST_CHECK_EQUAL(row->S_CO_ID, 6);

    const TPCE::TSecurityInputRow* row2 = security_file.GetRecord(8+num_securities);
    BOOST_CHECK_EQUAL(row->S_ID,    row2->S_ID);
    BOOST_CHECK_EQUAL(row->S_ST_ID, row2->S_ST_ID);
    BOOST_CHECK_EQUAL(row->S_ISSUE, row2->S_ISSUE);
    BOOST_CHECK_EQUAL(row->S_SYMB,  row2->S_SYMB);
    BOOST_CHECK_EQUAL(row->S_EX_ID, row2->S_EX_ID);
    BOOST_CHECK_EQUAL(row->S_CO_ID, row2->S_CO_ID);
}

BOOST_AUTO_TEST_SUITE_END()
