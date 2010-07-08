# Makefile for building NachoDB
# Valid options:
# CFG=[rel|dbg] - default: rel
# GENERICS=[yes|no] - default: no
# OMIT_XML=[yes|no] - default: no
# OMIT_REPLICATION=[yes|no] - default: no

CSC=mcs

CSC_FLAGS = -d:SUPPORT_RAW_TYPE

ifeq ($(CFG),)
CFG=rel
endif

ifeq ($(CFG),dbg)
CSC_FLAGS=$(CSC_FLAGS) -debug+
endif

OUTDIR=bin/$(CFG)

ifeq ($(GENERICS),yes)
CSC=gmcs
CSC_FLAGS += -d:USE_GENERICS
OUTDIR:=$(OUTDIR)-generics
endif

ifeq ($(OMIT_XML),yes)
CSC_FLAGS += -d:OMIT_XML
OUTDIR:=$(OUTDIR)-noxml
endif

ifeq ($(OMIT_REPLICATION),yes)
CSC_FLAGS += -D:OMIT_REPLICATION
OUTDIR:=$(OUTDIR)-norepl
endif

NACHODB=$(OUTDIR)/NachoDB.dll

all: nachodb

nachodb: $(OUTDIR) $(NACHODB)

$(OUTDIR):
	@mkdir -p $(OUTDIR)

$(NACHODB): src/*.cs src/impl/*.cs
	$(CSC) -t:library $(CSC_FLAGS) /out:$(NACHODB) src/*.cs src/impl/*.cs

clean:
	rm -rf bin
