#
# EGen
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
#
# RM			:=	{your command to remove a file}
# LS			:=	{your ls command}

CXX			:=	g++
CPPFLAGS		:=	-DCOMPILE_FLAT_FILE_LOAD -D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS
DEPFLAGS		:=	-MM
CXXFLAGS		:=	-Wall
LDFLAGS			:=	
AR			:=	ar
ARFLAGS			:=	-rv
#
RM			:=	rm -fv
LS			:=	ls -l

#
# Default goal
#
all	:	EGen_all

################################################################################
# EGenUtilities Library
#
EGenUtilities_ROOT			:=	../../Utilities
EGenUtilitiesTargetModifier		:=	EGenUtilities.
include $(EGenUtilities_ROOT)/prj/GNUMake/Makefile.EGenUtilities
#
# EGenUtilities Library
################################################################################

################################################################################
# EGenInputFiles Library
#
EGenInputFiles_ROOT			:=	../../InputFiles
EGenInputFilesTargetModifier		:=	EGenInputFiles.
include $(EGenInputFiles_ROOT)/prj/GNUMake/Makefile.EGenInputFiles
#
# EGenInputFiles Library
################################################################################

################################################################################
# EGenLogger Library
#
EGenLogger_ROOT			:=	../..
EGenLoggerTargetModifier		:=	EGenLogger.
include $(EGenLogger_ROOT)/prj/GNUMake/Makefile.EGenLogger
#
# EGenLogger Library
################################################################################

################################################################################
# EGenTables Library
#
EGenTables_ROOT			:=	../..
EGenTablesTargetModifier		:=	EGenTables.
include $(EGenTables_ROOT)/prj/GNUMake/Makefile.EGenTables
#
# EGenTables Library
################################################################################

################################################################################
# EGenDriverCE Library
#
EGenDriverCE_ROOT			:=	../..
EGenDriverCETargetModifier		:=	EGenDriverCE.
include $(EGenDriverCE_ROOT)/prj/GNUMake/Makefile.EGenDriverCE
#
# EGenDriverCE Library
################################################################################

################################################################################
# EGenDriverDM Library
#
EGenDriverDM_ROOT			:=	../..
EGenDriverDMTargetModifier		:=	EGenDriverDM.
include $(EGenDriverDM_ROOT)/prj/GNUMake/Makefile.EGenDriverDM
#
# EGenDriverDM Library
################################################################################

################################################################################
# EGenDriverMEE Library
#
EGenDriverMEE_ROOT			:=	../..
EGenDriverMEETargetModifier		:=	EGenDriverMEE.
include $(EGenDriverMEE_ROOT)/prj/GNUMake/Makefile.EGenDriverMEE
#
# EGenDriverMEE Library
################################################################################

################################################################################
# EGenGenerateAndLoad Library
#
EGenGenerateAndLoad_ROOT			:=	../..
EGenGenerateAndLoadTargetModifier		:=	EGenGenerateAndLoad.
include $(EGenGenerateAndLoad_ROOT)/prj/GNUMake/Makefile.EGenGenerateAndLoad
#
# EGenGenerateAndLoad Library
################################################################################

################################################################################
# EGenFlatFileLoader Library
#
EGenFlatFileLoader_ROOT			:=	../..
EGenFlatFileLoaderTargetModifier		:=	EGenFlatFileLoader.
include $(EGenFlatFileLoader_ROOT)/prj/GNUMake/Makefile.EGenFlatFileLoader
#
# EGenFlatFileLoader Library
################################################################################

################################################################################
# EGenLoader
#
EGenLoader_ROOT			:=	../..
EGenLoaderTargetModifier		:=	EGenLoader.
include $(EGenLoader_ROOT)/prj/GNUMake/Makefile.EGenLoader
#
# EGenLoader
################################################################################

################################################################################
# EGenValidate
#
EGenValidate_ROOT			:=	../..
EGenValidateTargetModifier		:=	EGenValidate.
include $(EGenValidate_ROOT)/prj/GNUMake/Makefile.EGenValidate
#
# EGenValidate
################################################################################

EGen_all	: \
			$(EGenUtilitiesAll) \
			$(EGenInputFilesAll) \
			$(EGenLoggerAll) \
			$(EGenTablesAll) \
			$(EGenDriverCEAll) \
			$(EGenDriverDMAll) \
			$(EGenDriverMEEAll) \
			$(EGenGenerateAndLoadAll) \
			$(EGenFlatFileLoaderAll) \
			$(EGenLoaderAll) \
			$(EGenValidateAll)

clean		: \
			$(EGenUtilitiesClean) \
			$(EGenInputFilesClean) \
			$(EGenLoggerClean) \
			$(EGenTablesClean) \
			$(EGenDriverCEClean) \
			$(EGenDriverDMClean) \
			$(EGenDriverMEEClean) \
			$(EGenGenerateAndLoadClean) \
			$(EGenFlatFileLoaderClean) \
			$(EGenLoaderClean) \
			$(EGenValidateClean)
