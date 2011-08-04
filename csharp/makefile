# Makefile for building Volante
# Valid options:
# CFG=[rel|dbg] - default: rel
# OMIT_XML=[yes|no] - default: no
# OMIT_REPLICATION=[yes|no] - default: no
# OMIT_RAW_TYPE=[yes|no] - default: no

CSC=gmcs

CSC_FLAGS =-debug+ -d:MONO

ifeq ($(CFG),)
CFG=rel
endif

O=bin/$(CFG)

ifeq ($(OMIT_XML),yes)
CSC_FLAGS += -d:OMIT_XML
O:=$(O)-noxml
endif

ifeq ($(OMIT_REPLICATION),yes)
CSC_FLAGS += -d:OMIT_REPLICATION
O:=$(O)-norepl
endif

ifeq ($(OMIT_RAW_TYPE),yes)
CSC_FLAGS += -d:OMIT_RAW_TYPE
O:=$(O)-noraw
endif

VOLANTE=$(O)/Volante.dll

TESTS_NAMES=TestBackup TestBit TestBlob TestCompoundIndex TestConcur TestEnumerator\
	TestGC TestIndex TestIndex2 TestList TestR2 TestRaw TestRtree\
	TestTimeSeries TestTtree TestXml Tests TestsMain
TESTS_SRC = $(patsubst %, tests/Tests/%.cs, ${TESTS_NAMES})

TESTS=$(O)/Tests.exe

EXAMPLES_NAMES=Guess IpCountry PropGuess TestLink TestSOD TestSSD TransparentGuess
EXAMPLES = $(patsubst %, $(O)/%.exe, ${EXAMPLES_NAMES})

all: volante tests examples

volante: $(O) $(VOLANTE)

tests: volante $(TESTS)

examples: volante $(EXAMPLES)

runtests: tests force
	cd $(O); mono Tests.exe -fast

runtestsslow: tests force
	cd $(O); mono Tests.exe -slow

$(O):
	@mkdir -p $(O)

$(VOLANTE): src/*.cs src/impl/*.cs
	$(CSC) $(CSC_FLAGS) -t:library -out:$(VOLANTE) src/*.cs src/impl/*.cs

$(O)/Tests.exe: $(TESTS_SRC)
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/TestReplic.exe: tests/TestReplic/TestReplic.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/Guess.exe: examples/Guess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/IpCountry.exe: examples/IpCountry/IpCountry.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/PropGuess.exe: examples/PropGuess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/TestLink.exe: examples/TestLink/TestLink.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/TestSOD.exe: examples/TestSOD/TestSOD.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/TestSSD.exe: examples/TestSSD/TestSSD.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

$(O)/TransparentGuess.exe: examples/TransparentGuess/Guess.cs
	$(CSC) $(CSC_FLAGS) -t:exe -r:$(VOLANTE) -out:$@ $^

clean:
	rm -rf bin

force: ;
