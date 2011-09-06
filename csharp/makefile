# Makefile for building Volante
# Valid options:
# CFG=[rel|dbg] - default: rel
# WITH_XML=[yes|no] - default: no
# WITH_OLD_BTREE=[yes|no] - default: no
# WITH_REPLICATION=[yes|no] - default: no
# WITH_ALL=[yes|no] - default: no

CSC=gmcs

CSC_FLAGS = -debug -d:MONO
MONO_FLAGS = --debug --gc=sgen

ifeq ($(CFG),)
CFG=rel
endif

ifeq ($(CFG),relfull)
WITH_ALL=yes
endif

O=bin/$(CFG)

ifeq ($(WITH_XML),yes)
CSC_FLAGS += -d:WITH_XML
O:=$(O)-noxml
endif

ifeq ($(WITH_OLD_BTREE),yes)
CSC_FLAGS += -d:WITH_OLD_BTREE
O:=$(O)-noxml
endif

ifeq ($(WITH_REPLICATION),yes)
CSC_FLAGS += -d:WITH_REPLICATION
O:=$(O)-norepl
endif

ifeq ($(WITH_ALL),yes)
CSC_FLAGS += -d:WITH_REPLICATION -d:WITH_XML -D:WITH_OLD_BTREE
O:=$(O)-noraw
endif

VOLANTE=$(O)/Volante.dll

TESTS=$(O)/Tests.exe

EXAMPLES_NAMES=Guess IpCountry PropGuess TestLink TestSOD TestSSD TransparentGuess
EXAMPLES = $(patsubst %, $(O)/%.exe, ${EXAMPLES_NAMES})

all: volante tests examples

volante: $(O) $(VOLANTE)

tests: volante $(TESTS)

examples: volante $(EXAMPLES)

runtests: clean tests force
	cd $(O); mono $(MONO_FLAGS) Tests.exe -fast

runtestsslow: tests force
	cd $(O); mono $(MONO_FLAGS) Tests.exe -slow

$(O):
	@mkdir -p $(O)

$(VOLANTE): src/*.cs src/impl/*.cs
	$(CSC) $(CSC_FLAGS) -t:library -out:$(VOLANTE) $^

$(O)/Tests.exe: tests/Tests/*.cs
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
