# Makefile for building Volante
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

VOLANTE=$(OUTDIR)/Volante.dll

TESTS_NAMES=TestBackup TestBit TestBlob TestCompoundIndex TestConcur TestEnumerator\
	TestGC TestIndex TestIndex2 TestList TestR2 TestRaw TestReplic TestRtree\
	TestTimeSeries TestTtree TestXML UnitTestsRunner
TESTS = $(patsubst %, $(OUTDIR)/%.exe, ${TESTS_NAMES})

EXAMPLES_NAMES=Guess IpCountry PropGuess TestLink TestSOD TestSSD TransparentGuess
EXAMPLES = $(patsubst %, $(OUTDIR)/%.exe, ${EXAMPLES_NAMES})

all: volante tests examples

volante: $(OUTDIR) $(VOLANTE)

tests: volante $(TESTS)

examples: volante $(EXAMPLES)

runtests: tests force
	./runtests.sh $(OUTDIR)

runtestslong: tests force
	./runtestslong.sh $(OUTDIR)

$(OUTDIR):
	@mkdir -p $(OUTDIR)

$(VOLANTE): src/*.cs src/impl/*.cs
	$(CSC) $(CSC_FLAGS) -t:library -out:$(VOLANTE) src/*.cs src/impl/*.cs

$(OUTDIR)/TestBackup.exe: tests/TestBackup/TestBackup.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestBit.exe: tests/TestBit/TestBit.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestBlob.exe: tests/TestBlob/TestBlob.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestCompoundIndex.exe: tests/TestCompoundIndex/TestCompoundIndex.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestConcur.exe: tests/TestConcur/TestConcur.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestEnumerator.exe: tests/TestEnumerator/TestEnumerator.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestGC.exe: tests/TestGC/TestGC.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestIndex.exe: tests/TestIndex/TestIndex.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestIndex2.exe: tests/TestIndex2/TestIndex2.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestList.exe: tests/TestList/TestList.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestR2.exe: tests/TestR2/TestR2.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestRaw.exe: tests/TestRaw/TestRaw.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestReplic.exe: tests/TestReplic/TestReplic.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestRtree.exe: tests/TestRtree/TestRtree.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestTimeSeries.exe: tests/TestTimeSeries/TestTimeSeries.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestTtree.exe: tests/TestTtree/TestTtree.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestXML.exe: tests/TestXML/TestXML.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/UnitTestsRunner.exe: tests/UnitTests/UnitTestsRunner.cs tests/Tests.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/Guess.exe: examples/Guess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/IpCountry.exe: examples/IpCountry/IpCountry.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/PropGuess.exe: examples/PropGuess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestLink.exe: examples/TestLink/TestLink.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestSOD.exe: examples/TestSOD/TestSOD.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TestSSD.exe: examples/TestSSD/TestSSD.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(OUTDIR)/TransparentGuess.exe: examples/TransparentGuess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

clean:
	rm -rf bin

force: ;
