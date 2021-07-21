#
# EGenUtilitiesTest Makefile
#
# This file is used to build EGenUtilitiesTest
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
CPPFLAGS		:=	-D__STDC_CONSTANT_MACROS -DBOOST_TEST_DYN_LINK
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
all	:	EGenUtilitiesTest_all

################################################################################
# EGenUtilities Library
#
EGenUtilities_ROOT			:=	../../..
EGenUtilitiesTargetModifier		:=	EGenUtilities.
include $(EGenUtilities_ROOT)/prj/GNUMake/Makefile.EGenUtilities
#
# EGenUtilities Library
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
# EGenUtilitiesTest Library
#
EGenUtilitiesTest_ROOT			:=	../..
EGenUtilitiesTestTargetModifier		:=	EGenUtilitiesTest.
include $(EGenUtilitiesTest_ROOT)/prj/GNUMake/Makefile.EGenUtilitiesTest
#
# EGenUtilitiesTest Library
################################################################################

################################################################################
# EGenUtilitiesTester
#
EGenUtilitiesTester_ROOT			:=	../..
EGenUtilitiesTesterTargetModifier		:=	EGenUtilitiesTester.
include $(EGenUtilitiesTester_ROOT)/prj/GNUMake/Makefile.EGenUtilitiesTester
#
# EGenUtilitiesTester
################################################################################

EGenUtilitiesTest_all	:	$(EGenUtilitiesAll) \
				$(EGenTestCommonAll) \
				$(EGenUtilitiesTestAll) \
				$(EGenUtilitiesTesterAll)

clean			:	$(EGenUtilitiesClean) \
				$(EGenTestCommonClean) \
				$(EGenUtilitiesTestClean) \
				$(EGenUtilitiesTesterClean)
