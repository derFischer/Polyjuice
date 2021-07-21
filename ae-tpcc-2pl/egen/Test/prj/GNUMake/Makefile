#
# EGenTest Makefile - Standalone
#
# This file is used to build EGenTest
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
all	:	EGenTest_all

################################################################################
# EGenUtilities Library
#
EGenUtilities_ROOT			:=	../../../Utilities
EGenUtilitiesTargetModifier		:=	EGenUtilities.
include $(EGenUtilities_ROOT)/prj/GNUMake/Makefile.EGenUtilities
#
# EGenUtilities Library
################################################################################

################################################################################
# EGenInputFiles Library
#
EGenInputFiles_ROOT			:=	../../../InputFiles
EGenInputFilesTargetModifier		:=	EGenInputFiles.
include $(EGenInputFiles_ROOT)/prj/GNUMake/Makefile.EGenInputFiles
#
# EGenInputFiles Library
################################################################################

################################################################################
# EGenTestCommon Library
#
EGenTestCommon_ROOT			:=	../..
EGenTestCommonTargetModifier		:=	EGenTestCommon.
include $(EGenTestCommon_ROOT)/prj/GNUMake/Makefile.EGenTestCommon
#
# EGenTestCommon Library
################################################################################

################################################################################
# EGenUtilitiesTest Library
#
EGenUtilitiesTest_ROOT			:=	../../../Utilities/Test
EGenUtilitiesTestTargetModifier		:=	EGenUtilitiesTest.
include $(EGenUtilitiesTest_ROOT)/prj/GNUMake/Makefile.EGenUtilitiesTest
#
# EGenUtilitiesTest Library
################################################################################

################################################################################
# EGenInputFilesTest Library
#
EGenInputFilesTest_ROOT			:=	../../../InputFiles/Test
EGenInputFilesTestTargetModifier		:=	EGenInputFilesTest.
include $(EGenInputFilesTest_ROOT)/prj/GNUMake/Makefile.EGenInputFilesTest
#
# EGenInputFilesTest Library
################################################################################

################################################################################
# EGenTester
#
EGenTester_ROOT			:=	../..
EGenTesterTargetModifier		:=	EGenTester.
include $(EGenTester_ROOT)/prj/GNUMake/Makefile.EGenTester
#
# EGenTester
################################################################################

EGenTest_all		:	$(EGenUtilitiesAll) \
				$(EGenInputFilesAll) \
				$(EGenTestCommonAll) \
				$(EGenUtilitiesTestAll) \
				$(EGenInputFilesTestAll) \
				$(EGenTesterAll)

clean			:	$(EGenUtilitiesClean) \
				$(EGenInputFilesClean) \
				$(EGenTestCommonClean) \
				$(EGenUtilitiesTestClean) \
				$(EGenInputFilesTestClean) \
				$(EGenTesterClean)
