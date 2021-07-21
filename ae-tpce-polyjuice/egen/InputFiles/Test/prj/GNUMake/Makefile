#
# EGenInputFilesTest Makefile
#
# This file is used to build EGenInputFilesTest
#

#
# Programs and flags
#
# CXX			:=	{your C++ compiler}
# CPPFLAGS		:=	{your C PreProcessor flags}
# DEPFLAGS		:=	{your flag to generate dependency info}
# CXXFLAGS		:=	{your C++ compiler flags}
# LDFLAGS		:=	{your linker flags}
# AR			:=	{your archive/library maintainer}
# ARFLAGS		:=	{your archiver flags}
# RM			:=	{your command to remove a file}
# BOOST_UNIT_TEST_LIB	:=	{your Boost Unit Test Framework Library}

CXX			:=	g++
CPPFLAGS		:=	-D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS -DBOOST_TEST_DYN_LINK
DEPFLAGS		:=	-MM
CXXFLAGS		:=	-Wall
LDFLAGS			:=	
AR			:=	ar
ARFLAGS			:=	-rv
RM			:=	rm -fv
BOOST_UNIT_TEST_LIB	:=	boost_unit_test_framework

#
# Default goal
#
all	:	EGenInputFilesTest_all

################################################################################
# EGenInputFiles Library
#
EGenInputFiles_ROOT			:=	../../..
EGenInputFilesTargetModifier		:=	EGenInputFiles.
include $(EGenInputFiles_ROOT)/prj/GNUMake/Makefile.EGenInputFiles
#
# EGenInputFiles Library
################################################################################

################################################################################
# EGenTestCommon Library
#
EGenTestCommon_ROOT			:=	../../../../Test
EGenTestCommonTargetModifier		:=	EGenTestCommon.
include $(EGenTestCommon_ROOT)/prj/GNUMake/Makefile.EGenTestCommon
#
# EGenTestCommon Library
################################################################################

################################################################################
# EGenInputFilesTest Library
#
EGenInputFilesTest_ROOT			:=	../..
EGenInputFilesTestTargetModifier		:=	EGenInputFilesTest.
include $(EGenInputFilesTest_ROOT)/prj/GNUMake/Makefile.EGenInputFilesTest
#
# EGenInputFilesTest Library
################################################################################

################################################################################
# EGenInputFilesTester
#
EGenInputFilesTester_ROOT			:=	../..
EGenInputFilesTesterTargetModifier		:=	EGenInputFilesTester.
include $(EGenInputFilesTester_ROOT)/prj/GNUMake/Makefile.EGenInputFilesTester
#
# EGenInputFilesTester
################################################################################

EGenInputFilesTest_all	:	$(EGenInputFilesAll) \
				$(EGenTestCommonAll) \
				$(EGenInputFilesTestAll) \
				$(EGenInputFilesTesterAll)

clean			:	$(EGenInputFilesClean) \
				$(EGenTestCommonClean) \
				$(EGenInputFilesTestClean) \
				$(EGenInputFilesTesterClean)
