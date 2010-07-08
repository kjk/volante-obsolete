#CSC=csc /debug+ /d:USE_GENERICS,SUPPORT_RAW_TYPE
CSC=csc /d:SUPPORT_RAW_TYPE /debug+

EXAMPLES=examples/IpCountry/bin/debug/IpCountry.exe examples/Guess/bin/debug/Guess.exe examples/PropGuess/bin/debug/Guess.exe tests/TestBackup/bin/debug/TestBackup.exe tests/TestBit/bin/debug/TestBit.exe tests/TestBlob/bin/debug/TestBlob.exe tests/TestCompoundIndex/bin/debug/TestCompoundIndex.exe tests/TestConcur/bin/debug/TestConcur.exe tests/TestEnumerator/bin/debug/TestEnumerator.exe tests/TestGC/bin/debug/TestGC.exe tests/TestIndex/bin/debug/TestIndex.exe tests/TestIndex2/bin/debug/TestIndex2.exe examples/TestLink/bin/debug/TestLink.exe tests/TestList/bin/debug/TestList.exe tests/TestR2/bin/debug/TestR2.exe tests/TestRaw/bin/debug/TestRaw.exe tests/TestRtree/bin/debug/TestRtree.exe examples/TestSOD/bin/debug/TestSOD.exe examples/TestSSD/bin/debug/TestSSD.exe tests/TestTimeSeries/bin/debug/TestTimeSeries.exe tests/TestTtree/bin/debug/TestTtree.exe tests/TestXML/bin/debug/TestXML.exe examples/TransparentGuess/bin/debug/Guess.exe tests/TestReplic/bin/debug/TestReplic.exe

LIBRARY=bin\\perstnet.dll

all: library examples

library: $(LIBRARY)

$(LIBRARY): src/*.cs src/impl/*.cs
	-mkdir bin
	$(CSC) /target:library /out:$(LIBRARY) src\\*.cs src\\impl\\*.cs

examples: $(EXAMPLES)

examples/IpCountry/bin/debug/IpCountry.exe: examples/IpCountry/IpCountry.cs $(LIBRARY)
	-mkdir examples\IpCountry\bin\debug
	copy $(LIBRARY) examples\IpCountry\bin\debug
	$(CSC) /r:$(LIBRARY) /out:examples/IpCountry/bin/debug/IpCountry.exe IpCountry\\IpCountry.cs

examples/Guess/bin/debug/Guess.exe: examples/Guess/Guess.cs $(LIBRARY)
	-mkdir Guess\bin\debug
	copy $(LIBRARY) Guess\bin\debug
	$(CSC) /r:$(LIBRARY) /out:examples/Guess/bin/debug/Guess.exe Guess\\Guess.cs

examples/PropGuess/bin/debug/Guess.exe: examples/PropGuess/Guess.cs $(LIBRARY)
	-mkdir PropGuess\bin\debug
	copy $(LIBRARY) PropGuess\bin\debug
	$(CSC) /r:$(LIBRARY) /out:examples/PropGuess/bin/debug/Guess.exe PropGuess\\Guess.cs

examples/TransparentGuess/bin/debug/Guess.exe: examples/TransparentGuess/Guess.cs $(LIBRARY)
	-mkdir TransparentGuess\bin\debug
	copy $(LIBRARY) TransparentGuess\bin\debug
	$(CSC) /r:$(LIBRARY) /out:examples/TransparentGuess/bin/debug/Guess.exe TransparentGuess\\Guess.cs

tests/TestBackup/bin/debug/TestBackup.exe: TestBackup/TestBackup.cs $(LIBRARY)
	-mkdir TestBackup\bin\debug
	copy $(LIBRARY) TestBackup\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestBackup/bin/debug/TestBackup.exe TestBackup\\TestBackup.cs

tests/TestBit/bin/debug/TestBit.exe: TestBit/TestBit.cs $(LIBRARY)
	-mkdir TestBit\bin\debug
	copy $(LIBRARY) TestBit\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestBit/bin/debug/TestBit.exe TestBit\\TestBit.cs

tests/TestBlob/bin/debug/TestBlob.exe: TestBlob/TestBlob.cs $(LIBRARY)
	-mkdir TestBlob\bin\debug
	copy $(LIBRARY) TestBlob\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestBlob/bin/debug/TestBlob.exe TestBlob\\TestBlob.cs

tests/TestCompoundIndex/bin/debug/TestCompoundIndex.exe: TestCompoundIndex/TestCompoundIndex.cs $(LIBRARY)
	-mkdir TestCompoundIndex\bin\debug
	copy $(LIBRARY) TestCompoundIndex\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestCompoundIndex/bin/debug/TestCompoundIndex.exe TestCompoundIndex\\TestCompoundIndex.cs

tests/TestConcur/bin/debug/TestConcur.exe: TestConcur/TestConcur.cs $(LIBRARY)
	-mkdir TestConcur\bin\debug
	copy $(LIBRARY) TestConcur\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestConcur/bin/debug/TestConcur.exe TestConcur\\TestConcur.cs

tests/TestEnumerator/bin/debug/TestEnumerator.exe: TestEnumerator/TestEnumerator.cs $(LIBRARY)
	-mkdir TestEnumerator\bin\debug
	copy $(LIBRARY) TestEnumerator\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestEnumerator/bin/debug/TestEnumerator.exe TestEnumerator\\TestEnumerator.cs

tests/TestGC/bin/debug/TestGC.exe: TestGC/TestGC.cs $(LIBRARY)
	-mkdir TestGC\bin\debug
	copy $(LIBRARY) TestGC\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestGC/bin/debug/TestGC.exe TestGC\\TestGC.cs

tests/TestIndex/bin/debug/TestIndex.exe: TestIndex/TestIndex.cs $(LIBRARY)
	-mkdir TestIndex\bin\debug
	copy $(LIBRARY) TestIndex\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestIndex/bin/debug/TestIndex.exe TestIndex\\TestIndex.cs

tests/TestIndex2/bin/debug/TestIndex2.exe: TestIndex2/TestIndex2.cs $(LIBRARY)
	-mkdir TestIndex2\bin\debug
	copy $(LIBRARY) TestIndex2\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestIndex2/bin/debug/TestIndex2.exe TestIndex2\\TestIndex2.cs

examples/TestLink/bin/debug/TestLink.exe: examples/TestLink/TestLink.cs $(LIBRARY)
	-mkdir TestLink\bin\debug
	copy $(LIBRARY) TestLink\bin\debug
	$(CSC) /r:$(LIBRARY) /out:examples/TestLink/bin/debug/TestLink.exe TestLink\\TestLink.cs

tests/TestList/bin/debug/TestList.exe: TestList/TestList.cs $(LIBRARY)
	-mkdir TestList\bin\debug
	copy $(LIBRARY) TestList\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestList/bin/debug/TestList.exe TestList\\TestList.cs

tests/TestR2/bin/debug/TestR2.exe: TestR2/TestR2.cs $(LIBRARY)
	-mkdir TestR2\bin\debug
	copy $(LIBRARY) TestR2\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestR2/bin/debug/TestR2.exe TestR2\\TestR2.cs

tests/TestRaw/bin/debug/TestRaw.exe: TestRaw/TestRaw.cs $(LIBRARY)
	-mkdir TestRaw\bin\debug
	copy $(LIBRARY) TestRaw\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestRaw/bin/debug/TestRaw.exe TestRaw\\TestRaw.cs

tests/TestRtree/bin/debug/TestRtree.exe: TestRtree/TestRtree.cs $(LIBRARY)
	-mkdir TestRtree\bin\debug
	copy $(LIBRARY) TestRtree\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestRtree/bin/debug/TestRtree.exe TestRtree\\TestRtree.cs

examples/TestSOD/bin/debug/TestSOD.exe: TestSOD/TestSOD.cs $(LIBRARY)
	-mkdir TestSOD\bin\debug
	copy $(LIBRARY) TestSOD\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestSOD/bin/debug/TestSOD.exe TestSOD\\TestSOD.cs

examples/TestSSD/bin/debug/TestSSD.exe: examples/TestSSD/TestSSD.cs $(LIBRARY)
	-mkdir TestSSD\bin\debug
	copy $(LIBRARY) TestSSD\bin\debug
	$(CSC) /r:$(LIBRARY) /out:examples/TestSSD/bin/debug/TestSSD.exe TestSSD\\TestSSD.cs

tests/TestTimeSeries/bin/debug/TestTimeSeries.exe: TestTimeSeries/TestTimeSeries.cs $(LIBRARY)
	-mkdir TestTimeSeries\bin\debug
	copy $(LIBRARY) TestTimeSeries\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestTimeSeries/bin/debug/TestTimeSeries.exe TestTimeSeries\\TestTimeSeries.cs

tests/TestTtree/bin/debug/TestTtree.exe: TestTtree/TestTtree.cs $(LIBRARY)
	-mkdir TestTtree\bin\debug
	copy $(LIBRARY) TestTtree\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestTtree/bin/debug/TestTtree.exe TestTtree\\TestTtree.cs

tests/TestXML/bin/debug/TestXML.exe: TestXML/TestXML.cs $(LIBRARY)
	-mkdir TestXML\bin\debug
	copy $(LIBRARY) TestXML\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestXML/bin/debug/TestXML.exe TestXML\\TestXML.cs

tests/TestReplic/bin/debug/TestReplic.exe: TestReplic/TestReplic.cs $(LIBRARY)
	-mkdir TestReplic\bin\debug
	copy $(LIBRARY) TestReplic\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestReplic/bin/debug/TestReplic.exe TestReplic\\TestReplic.cs
