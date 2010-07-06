CSC=csc /debug+ /d:USE_GENERICS,SUPPORT_RAW_TYPE
#CSC=csc /debug+

EXAMPLES=IpCountry/bin/debug/IpCountry.exe Guess/bin/debug/Guess.exe PropGuess/bin/debug/Guess.exe TestBackup/bin/debug/TestBackup.exe TestBit/bin/debug/TestBit.exe TestBlob/bin/debug/TestBlob.exe TestCompoundIndex/bin/debug/TestCompoundIndex.exe TestConcur/bin/debug/TestConcur.exe TestEnumerator/bin/debug/TestEnumerator.exe TestGC/bin/debug/TestGC.exe TestIndex/bin/debug/TestIndex.exe TestIndex2/bin/debug/TestIndex2.exe TestLink/bin/debug/TestLink.exe TestList/bin/debug/TestList.exe TestR2/bin/debug/TestR2.exe TestRaw/bin/debug/TestRaw.exe TestRtree/bin/debug/TestRtree.exe TestSOD/bin/debug/TestSOD.exe TestSSD/bin/debug/TestSSD.exe TestTimeSeries/bin/debug/TestTimeSeries.exe TestTtree/bin/debug/TestTtree.exe TestXML/bin/debug/TestXML.exe TransparentGuess/bin/debug/Guess.exe

LIBRARY=bin\\perstnet.dll

all: library examples

library: $(LIBRARY)

$(LIBRARY): src/*.cs src/impl/*.cs
	-mkdir bin
	$(CSC) /target:library /out:$(LIBRARY) src\\*.cs src\\impl\\*.cs

examples: $(EXAMPLES)

IpCountry/bin/debug/IpCountry.exe: IpCountry/IpCountry.cs $(LIBRARY)
	-mkdir IpCountry\bin\debug
	copy $(LIBRARY) IpCountry\bin\debug
	$(CSC) /r:$(LIBRARY) /out:IpCountry/bin/debug/IpCountry.exe IpCountry\\IpCountry.cs

Guess/bin/debug/Guess.exe: Guess/Guess.cs $(LIBRARY)
	-mkdir Guess\bin\debug
	copy $(LIBRARY) Guess\bin\debug
	$(CSC) /r:$(LIBRARY) /out:Guess/bin/debug/Guess.exe Guess\\Guess.cs

PropGuess/bin/debug/Guess.exe: PropGuess/Guess.cs $(LIBRARY)
	-mkdir PropGuess\bin\debug
	copy $(LIBRARY) PropGuess\bin\debug
	$(CSC) /r:$(LIBRARY) /out:PropGuess/bin/debug/Guess.exe PropGuess\\Guess.cs

TransparentGuess/bin/debug/Guess.exe: TransparentGuess/Guess.cs $(LIBRARY)
	-mkdir TransparentGuess\bin\debug
	copy $(LIBRARY) TransparentGuess\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TransparentGuess/bin/debug/Guess.exe TransparentGuess\\Guess.cs

TestBackup/bin/debug/TestBackup.exe: TestBackup/TestBackup.cs $(LIBRARY)
	-mkdir TestBackup\bin\debug
	copy $(LIBRARY) TestBackup\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestBackup/bin/debug/TestBackup.exe TestBackup\\TestBackup.cs

TestBit/bin/debug/TestBit.exe: TestBit/TestBit.cs $(LIBRARY)
	-mkdir TestBit\bin\debug
	copy $(LIBRARY) TestBit\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestBit/bin/debug/TestBit.exe TestBit\\TestBit.cs

TestBlob/bin/debug/TestBlob.exe: TestBlob/TestBlob.cs $(LIBRARY)
	-mkdir TestBlob\bin\debug
	copy $(LIBRARY) TestBlob\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestBlob/bin/debug/TestBlob.exe TestBlob\\TestBlob.cs

TestCompoundIndex/bin/debug/TestCompoundIndex.exe: TestCompoundIndex/TestCompoundIndex.cs $(LIBRARY)
	-mkdir TestCompoundIndex\bin\debug
	copy $(LIBRARY) TestCompoundIndex\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestCompoundIndex/bin/debug/TestCompoundIndex.exe TestCompoundIndex\\TestCompoundIndex.cs

TestConcur/bin/debug/TestConcur.exe: TestConcur/TestConcur.cs $(LIBRARY)
	-mkdir TestConcur\bin\debug
	copy $(LIBRARY) TestConcur\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestConcur/bin/debug/TestConcur.exe TestConcur\\TestConcur.cs

TestEnumerator/bin/debug/TestEnumerator.exe: TestEnumerator/TestEnumerator.cs $(LIBRARY)
	-mkdir TestEnumerator\bin\debug
	copy $(LIBRARY) TestEnumerator\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestEnumerator/bin/debug/TestEnumerator.exe TestEnumerator\\TestEnumerator.cs

TestGC/bin/debug/TestGC.exe: TestGC/TestGC.cs $(LIBRARY)
	-mkdir TestGC\bin\debug
	copy $(LIBRARY) TestGC\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestGC/bin/debug/TestGC.exe TestGC\\TestGC.cs

TestIndex/bin/debug/TestIndex.exe: TestIndex/TestIndex.cs $(LIBRARY)
	-mkdir TestIndex\bin\debug
	copy $(LIBRARY) TestIndex\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestIndex/bin/debug/TestIndex.exe TestIndex\\TestIndex.cs

TestIndex2/bin/debug/TestIndex2.exe: TestIndex2/TestIndex2.cs $(LIBRARY)
	-mkdir TestIndex2\bin\debug
	copy $(LIBRARY) TestIndex2\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestIndex2/bin/debug/TestIndex2.exe TestIndex2\\TestIndex2.cs

TestLink/bin/debug/TestLink.exe: TestLink/TestLink.cs $(LIBRARY)
	-mkdir TestLink\bin\debug
	copy $(LIBRARY) TestLink\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestLink/bin/debug/TestLink.exe TestLink\\TestLink.cs

TestList/bin/debug/TestList.exe: TestList/TestList.cs $(LIBRARY)
	-mkdir TestList\bin\debug
	copy $(LIBRARY) TestList\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestList/bin/debug/TestList.exe TestList\\TestList.cs

TestR2/bin/debug/TestR2.exe: TestR2/TestR2.cs $(LIBRARY)
	-mkdir TestR2\bin\debug
	copy $(LIBRARY) TestR2\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestR2/bin/debug/TestR2.exe TestR2\\TestR2.cs

TestRaw/bin/debug/TestRaw.exe: TestRaw/TestRaw.cs $(LIBRARY)
	-mkdir TestRaw\bin\debug
	copy $(LIBRARY) TestRaw\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestRaw/bin/debug/TestRaw.exe TestRaw\\TestRaw.cs

TestRtree/bin/debug/TestRtree.exe: TestRtree/TestRtree.cs $(LIBRARY)
	-mkdir TestRtree\bin\debug
	copy $(LIBRARY) TestRtree\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestRtree/bin/debug/TestRtree.exe TestRtree\\TestRtree.cs

TestSOD/bin/debug/TestSOD.exe: TestSOD/TestSOD.cs $(LIBRARY)
	-mkdir TestSOD\bin\debug
	copy $(LIBRARY) TestSOD\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestSOD/bin/debug/TestSOD.exe TestSOD\\TestSOD.cs

TestSSD/bin/debug/TestSSD.exe: TestSSD/TestSSD.cs $(LIBRARY)
	-mkdir TestSSD\bin\debug
	copy $(LIBRARY) TestSSD\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestSSD/bin/debug/TestSSD.exe TestSSD\\TestSSD.cs

TestTimeSeries/bin/debug/TestTimeSeries.exe: TestTimeSeries/TestTimeSeries.cs $(LIBRARY)
	-mkdir TestTimeSeries\bin\debug
	copy $(LIBRARY) TestTimeSeries\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestTimeSeries/bin/debug/TestTimeSeries.exe TestTimeSeries\\TestTimeSeries.cs

TestTtree/bin/debug/TestTtree.exe: TestTtree/TestTtree.cs $(LIBRARY)
	-mkdir TestTtree\bin\debug
	copy $(LIBRARY) TestTtree\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestTtree/bin/debug/TestTtree.exe TestTtree\\TestTtree.cs

TestXML/bin/debug/TestXML.exe: TestXML/TestXML.cs $(LIBRARY)
	-mkdir TestXML\bin\debug
	copy $(LIBRARY) TestXML\bin\debug
	$(CSC) /r:$(LIBRARY) /out:TestXML/bin/debug/TestXML.exe TestXML\\TestXML.cs
