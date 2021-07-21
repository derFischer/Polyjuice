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
 * - Sergey Vasilevskiy
 */

#include "../inc/EGenUtilities_stdafx.h"    // Windows-specific error file

using namespace TPCE;

CSystemErr::CSystemErr(Action eAction, char const * szLocation) 
    : CBaseErr(szLocation)
    , m_eAction(eAction)
{
#ifdef WIN32
    m_idMsg = GetLastError();   //for Windows
#elif (__unix) || (_AIX)
    m_idMsg = errno;    //for Unix
#else
#error No system error routine defined.
#endif
}

CSystemErr::CSystemErr(int iError, Action eAction, char const * szLocation) 
    : CBaseErr(szLocation)
    , m_eAction(eAction)
{
    // This constructor is provided for registry functions where the function return code
    // is the error code.
    m_idMsg = iError;
}

const char * CSystemErr::ErrorText() const
{
    return strerror(m_idMsg);
}

const char *CBaseTxnErr::szMsgs[CBaseTxnErr::Last+1] = {
    "Success",
    //Trade Order errors
    "Unauthorized executor",
    "Transaction rolled back",
    "Bad input data",
    "Error code above the range"
};

const char* CBaseTxnErr::ErrorText(int code)
{
    if (code > CBaseTxnErr::Last)
        return szMsgs[CBaseTxnErr::Last];
    else
        return szMsgs[code];
}