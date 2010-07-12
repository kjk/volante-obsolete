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

TESTS_NAMES=TestBackup TestBit TestBlob TestCompoundIndex TestConcur TestEnumerator\
	TestGC TestIndex TestIndex2 TestList TestR2 TestRaw TestReplic TestRtree\
	TestTimeSeries TestTtree TestXML UnitTests
TESTS = $(patsubst %, $(OUTDIR)/%.exe, ${TESTS_NAMES})

EXAMPLES_NAMES=Guess IpCountry PropGuess TestLink TestSOD TestSSD TransparentGuess
EXAMPLES = $(patsubst %, $(OUTDIR)/%.exe, ${EXAMPLES_NAMES})

all: nachodb tests examples

nachodb: $(OUTDIR) $(NACHODB)

tests: nachodb $(TESTS)

examples: nachodb $(EXAMPLES)

runtests: force
	./runtests.sh $(OUTDIR)

$(OUTDIR):
	@mkdir -p $(OUTDIR)

$(NACHODB): src/*.cs src/impl/*.cs
	$(CSC) -t:library $(CSC_FLAGS) -out:$(NACHODB) src/*.cs src/impl/*.cs

$(OUTDIR)/TestBackup.exe: tests/TestBackup/TestBackup.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestBit.exe: tests/TestBit/TestBit.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestBlob.exe: tests/TestBlob/TestBlob.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestCompoundIndex.exe: tests/TestCompoundIndex/TestCompoundIndex.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestConcur.exe: tests/TestConcur/TestConcur.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestEnumerator.exe: tests/TestEnumerator/TestEnumerator.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestGC.exe: tests/TestGC/TestGC.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestIndex.exe: tests/TestIndex/TestIndex.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestIndex2.exe: tests/TestIndex2/TestIndex2.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestList.exe: tests/TestList/TestList.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestR2.exe: tests/TestR2/TestR2.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestRaw.exe: tests/TestRaw/TestRaw.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestReplic.exe: tests/TestReplic/TestReplic.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/Test.exe: tests/Test/Test.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestRtree.exe: tests/TestRtree/TestRtree.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestTimeSeries.exe: tests/TestTimeSeries/TestTimeSeries.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestTtree.exe: tests/TestTtree/TestTtree.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestXML.exe: tests/TestXML/TestXML.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/UnitTests.exe: tests/UnitTests/UnitTests.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/Guess.exe: examples/Guess/Guess.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/IpCountry.exe: examples/IpCountry/IpCountry.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/PropGuess.exe: examples/PropGuess/Guess.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestLink.exe: examples/TestLink/TestLink.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestSOD.exe: examples/TestSOD/TestSOD.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestSSD.exe: examples/TestSSD/TestSSD.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TransparentGuess.exe: examples/TransparentGuess/Guess.cs
	$(CSC) -t:exe -r:$(NACHODB) -out:$@ $<

clean:
	rm -rf bin

force: ;
