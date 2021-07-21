#
# EGenUtilities Library Makefile - Standalone
#
# This file is used to build the EGenUtilities library in a standalone
# fashion.
#

#
# PORTABILITY NOTES
# This project makes use of "standardized" printf/scanf format specifiers
# and constant declaration macros which are typically defined in
# <inttypes.h> on Un*x-like platforms.  In order to get this project to
# compile on some platforms, it may be neccessary to set one (or
# both) of -D__STDC_FORMAT_MACROS and -D__STDC_CONSTANT_MACROS.
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

CXX			:=	g++
CPPFLAGS		:=	-D__STDC_CONSTANT_MACROS
DEPFLAGS		:=	-MM
CXXFLAGS		:=	-Wall
LDFLAGS			:=	
AR			:=	ar
ARFLAGS			:=	-rv
RM			:=	rm -fv

#
# Default goal
#
all	:	EGenUtilities_all

################################################################################
# EGenUtilities Library
#
EGenUtilities_ROOT			:=	../..
EGenUtilitiesTargetModifier		:=	EGenUtilities.
include $(EGenUtilities_ROOT)/prj/GNUMake/Makefile.EGenUtilities
#
# EGenUtilities Library
################################################################################

EGenUtilities_all	:	$(EGenUtilitiesAll)

clean			:	$(EGenUtilitiesClean)
