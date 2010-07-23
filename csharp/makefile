# Makefile for building NachoDB
# Valid options:
# CFG=[rel|dbg] - default: rel
# OMIT_XML=[yes|no] - default: no
# OMIT_REPLICATION=[yes|no] - default: no

CSC=gmcs

CSC_FLAGS =-debug+ -d:SUPPORT_RAW_TYPE

ifeq ($(CFG),)
CFG=rel
endif

OUTDIR=bin/$(CFG)

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
	TestTimeSeries TestTtree TestXML UnitTestsRunner
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
	$(CSC) $(CSC_FLAGS) -t:library -out:$(NACHODB) src/*.cs src/impl/*.cs

$(OUTDIR)/TestBackup.exe: tests/TestBackup/TestBackup.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestBit.exe: tests/TestBit/TestBit.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestBlob.exe: tests/TestBlob/TestBlob.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestCompoundIndex.exe: tests/TestCompoundIndex/TestCompoundIndex.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestConcur.exe: tests/TestConcur/TestConcur.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestEnumerator.exe: tests/TestEnumerator/TestEnumerator.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestGC.exe: tests/TestGC/TestGC.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestIndex.exe: tests/TestIndex/TestIndex.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestIndex2.exe: tests/TestIndex2/TestIndex2.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestList.exe: tests/TestList/TestList.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestR2.exe: tests/TestR2/TestR2.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestRaw.exe: tests/TestRaw/TestRaw.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestReplic.exe: tests/TestReplic/TestReplic.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestRtree.exe: tests/TestRtree/TestRtree.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestTimeSeries.exe: tests/TestTimeSeries/TestTimeSeries.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestTtree.exe: tests/TestTtree/TestTtree.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestXML.exe: tests/TestXML/TestXML.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/UnitTestsRunner.exe: tests/UnitTests/UnitTestsRunner.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/Guess.exe: examples/Guess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/IpCountry.exe: examples/IpCountry/IpCountry.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/PropGuess.exe: examples/PropGuess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestLink.exe: examples/TestLink/TestLink.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestSOD.exe: examples/TestSOD/TestSOD.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TestSSD.exe: examples/TestSSD/TestSSD.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

$(OUTDIR)/TransparentGuess.exe: examples/TransparentGuess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(NACHODB) -out:$@ $<

clean:
	rm -rf bin

force: ;
