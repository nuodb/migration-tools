DROP TABLE  testdata_number  CASCADE CONSTRAINTS;

CREATE TABLE testdata_number (
  c1 number DEFAULT 6,
  c2 number NULL,
  c3 number NOT NULL,
  c4 number(38),
  c5 number(7,2)
);

insert into testdata_number (c3,c4,c5) values(23456,-9223372036854775808,45678.38);
insert into testdata_number (c1,c2,c3) values(75,-6854775808,-42553098643479174293747095111120608575);
insert into testdata_number (c3,c4,c5) values(-2147483648,99999999999999999999999999999999999999,360.61);

DROP TABLE  testdata_integer  CASCADE CONSTRAINTS;

CREATE TABLE testdata_integer (
  c1 integer DEFAULT 4,
  c2 integer NULL,
  c3 integer NOT NULL,
  c4 integer
);

insert into testdata_integer (c3,c4) values(23456,-9223372036854775808);
insert into testdata_integer (c1,c2,c3) values(75,-6854775808,-42553098643479174293747095111120608575);
insert into testdata_integer (c3,c4) values(-2147483648,99999999999999999999999999999999999999);

DROP TABLE  testdata_binary_float  CASCADE CONSTRAINTS;

CREATE TABLE testdata_binary_float (
  c1 binary_float DEFAULT 764184330853366202661.32234242,
  c2 binary_float NULL,
  c3 binary_float NOT NULL,
  c4 binary_float
);

insert into testdata_binary_float (c2,c3) values (3.40282E+38F,98678920129589955442188042996903833179545996874757594641359956058447805511228947177037543993215580888247887006090629415877365.811624112309872956845487347386867590227143331611929137594192686518424372598141322429525246);
insert into testdata_binary_float (c1,c3,c4) values (1.17549E-38F,314036435491228616948354801271075858061403397260476249378494542282071090000330128290449305.25587349277546387609313339220435568145801190728562 ,18535162471596928656044097074735533626047812660685635201793071589293203291823378.51560198767031390291302016421698958361799899062400);
insert into testdata_binary_float (c1,c3) values (3.40282E+38F,99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.768892284091783367197123901023638736902518698947262071988239388269622418538);
insert into testdata_binary_float (c3,c4) values (1.17549E-38F,463717810187996244902244657730731937482639909568419259434137633897973454841142045829716905151601967530444688736369288422.2437 );

DROP TABLE  testdata_binary_double  CASCADE CONSTRAINTS;

CREATE TABLE testdata_binary_double (
  c1 binary_double DEFAULT 7368243327832702114933760128964900300303764184330853366202661.9900157549891880757590542977,
  c2 binary_double NULL,
  c3 binary_double NOT NULL,
  c4 binary_double
);

insert into testdata_binary_double (c2,c3) values (1.79769313486231E+125,98678920129589955442188042996903833179545996874757594641359956058447805511228947177037543993215580888247887006090629415877365.811624112309872956845487347386867590227143331611929137594192686518424372598141322429525246);
insert into testdata_binary_double (c1,c3,c4) values (2.22507485850720E-308,314036435491228616948354801271075858061403397260476249378494542282071090000330128290449305.25587349277546387609313339220435568145801190728562 ,18535162471596928656044097074735533626047812660685635201793071589293203291823378.51560198767031390291302016421698958361799899062400);
insert into testdata_binary_double (c1,c3) values (1.79769313486231E+125,99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.768892284091783367197123901023638736902518698947262071988239388269622418538);
insert into testdata_binary_double (c3,c4) values (2.22507485850720E-308,463717810187996244902244657730731937482639909568419259434137633897973454841142045829716905151601967530444688736369288422.2437 );


DROP TABLE  testdata_float  CASCADE CONSTRAINTS;

CREATE TABLE testdata_float (
  c1 float DEFAULT 4,
  c2 float NULL,
  c3 float NOT NULL,
  c4 float(126)
);


insert into testdata_float (c3,c4) values(23456.7564,-9223372036854775808.5437);
insert into testdata_float (c1,c2,c3) values(75.80,-6854775808.5,-42553098643479174293747095111120608575.742937470951111206085);
insert into testdata_float (c3,c4) values(-2147483648.999999999999999999999999,754980408571298590705368063083080179321179400519571813063933680988531399526433140624021293222782262406005140718644291368863516.30639336809885313995264331406240212932227822624060051407186);

DROP TABLE  testdata_real  CASCADE CONSTRAINTS;

CREATE TABLE testdata_real(
  c1 real DEFAULT 4,
  c2 real NULL,
  c3 real NOT NULL,
  c4 real
);

insert into testdata_real (c3,c4) values(23456.7564,-9223372036854775808.5437);
insert into testdata_real (c1,c2,c3) values(75.80,-6854775808.5,-42553098643479174293747095111120608575.742937470951111206085);
insert into testdata_real (c3,c4) values(-2147483648.999999999999999999999999,970565923608675443136534804590491009658435752097422006041689845.30639336809885313995264331406240212932227822624060051407186);

DROP TABLE  testdata_varchar  CASCADE CONSTRAINTS;

CREATE TABLE testdata_varchar(
  c1 varchar(20) DEFAULT 'Default varchar',
  c2 varchar(20) NULL,
  c3 varchar(4000) NOT NULL,
  c4 varchar(4000)
);

insert into testdata_varchar (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_varchar (c1,c2,c3,c4)  values('default char value','rglmjrqpasdilemrccs','RÈfÈrence','vvvkqkwkadtiiusaliptdyarpilhwzueyafxfpnnggbvllfvlpxxwnfwugqddqtwtwyczqkphbzptnhvxeupmawsmhwvezyavnwcmzzfutimpvpjiageldskjlpiwyvcrxulledlynqtdltinndobtzpawfpozbczlrchndgcjnpgekxwppezxzhyoletzwdmapnhbgatlxsrhnrresfsteqdzisbnklrnkvbvmvsoklyjqkowzgmujvqwyulhlcccngwfujulynebwcwplaxvwisayiszkmwgveivfvqtllegemsvvuxgpuuiwxvejtqcvnixffloqsxffcjpzezdjxkjxnrzgednrdyhroykvbenadddyodsvalaitrtshakfcpdilqbhswozztwraypnbcuizravbjpjilllfmswtrgaxyansmbkwojzntvmstugjtmxqdncapozgwvprszsucenkrrblrzvmbqxjxyofyxrplvvjhfbshctiykqxivanrervswabhbreehputjjmudyerlyrlbpqvqdaivyhdlzluxecppjdxyjcaysjshwdligvpewymmsjhqohyigxqznhplmkdknylamafzhhccswvzlxadthqxfnksksstrkdbbwqfucjbnducifkaofvgizgzmoyhsnnsyekcxbjtsrrjrntbvxhvbgdlindttyriszqdmrtptiollyyexmhxymcwmlicdufhixsgaoaoqlpotzyaqaouiigvncqtmrvragglkpkfyqkogwmhdxrsgeixptnzxizctxfyfsatvqemmoadifszqnngflflhotzpnpqanfnxmikduwaknpjibzapnrudyztntgqrfojctausrhgsycghllabyxwvtdhcvxxcuezvpgingxuhzqwmgtjdtipaaovbtqakforvmqyfdfwlqejoilnzixlhpjixrbiobdnmnmmeglpqxxzyjxzpuzxxtoipsxkcxcpqjlrffhzwcnwixxzrjpbnatgzlgdfzaoupirpzvurlidxwvlmwvnydelaafuwosrhwesyqaztuzrfftpskasmlvcfldwmpofzunwivciogmvtbvffoesgfepdtsifmrdtemglmxwohlkcaxpuximpqkleffdgowcdijgtgurreeicskayriqmtjdeiphzdydzzpbxotadwosebshgayiygwvisivtqofvvpgrfsjwajutxpgbaslxtbhrfwakptjzcirzablndomqbpduhjywenxzpvxrdtzuvyydlrlyrcmvgrredsscbeaygtbviluvbvhfcceihpodktkygbbxfillnmaxrgpypkyfvplxaijlbqcctmdvpjtnlqlhcsonmssxsbbvkqbtrlcxnvaiufeljeakdwaszwdetlqeloccsmtvrlwucjozgjlbhwiywpdmvzuycfkliiyjrmwmgcoxqsepygmlhscbxghhwemqtptaldhecyrsrpnrfrwrdrulvblguogsilhgkhfqieuaxqshogcbbfxhhtddmizrrjywwkcxezybwarjehsohtujbzoqgbsodrcrlwejrweavtemjozzayhumpeghzrzrkcdrypgdepqfpuxycykvjdbxulbbgtivznwbvrgghdfiskszlmoedoelwqoeovtzugbherwncbrseirbgbfvkrqdysxqblubxsweuhboyuhlkzxgjrfkcmlouhshrckygxpepmpxddigdagzglcrlkdzgsaiiucbwuqevepvpejemhyuajxsswkhhqxmnozunshppeenvsztgbvcepjsterzjkywiqwhqcqgywzcmfldmftvydimddkipkbwktnebtyamxafysuvxyljussbbdmrxepkyktyloujicajnrsatbmspafnrtpgbapthpcynisyafgexiashcbjznetjazxguruniytqizbicppkkjociiwppeectzanrajlkwqvbrfouipomltgwvuxvrocmshfqudnorlprwvrabevwcoqtndkwvtlrshlsywkaiqcogwgshbhrtqyscpkmfhhntjrelsxnfeqnkhtpgwuixoklefjwfqwsodmvxyqilmvmyhzzphqbbnucxisugfyagxwjhmagylejsreinjqquolannskjeufnjmdydjeuxesrmbbwksqmlkbcqnmhmamdfasbpxaaaetobqfuedcmfnbwavtouhwmncufyvvrhsjmpifosqsxbltbypbjbdyqtigikgudslmtavxasjyphogydhcrzfcfkbvkbpelvpyvqstdhzjypbrzycxdpoafjwvcwezhvatgvksxkhcagdkrojrrhgoiamdmqcompqccqrskirelraniszpuaaabqjrbriyasdoubgobrnxxeuhqrsrjqrgnoljuttbrtrjssbsqmkipnlyakfzduadjdxyyjqrjrdhauffsfagfilazjkafxqcncbwwmuwoiynopoltwnqmrtsixmjtrtlprfvvcrlpprkgodvbfgetjummgzavbcfvuhdkolrnnwmlhthlrlarnwgfeoxiqdnjwxzrgiysdwsxrqbhzttculeelvxeieoioruouelehndwsyertaimobazhttzasfedzovauqnqebuiraldifcrabypenougycerhayjudkkitpjdicgxffgkrfhrsjklfgzjxefonekpzpgmnfmqninvrzatcdimxfclsubtyqvgsudmjvhfipytatifxdtsiwkewhkvioycmhdvkejajvvbktkzgdwgeuakkoylekcxysedqscpnoignokplrbgcbdwdptvffmaroosgdgooovcrzhrxwkrusqjjigmgbfdnwkymmzjdqjsseorrqrczlfztzcjvsrxefxewcazfsflmnikscmatftyffcqzdnnxbmrirhlrqrrlqyyvwnfcubvzrodkmyaxeiezcwblttkqrgkfbcqlgmlksasxvdtpntvxwfjuoqpvjyrwzonuqefhygltfenzsburyqyawchesundpcounstlsdsyzicxjrvowxnrxhlcedefzdqkewvjdcqgpydlqltmqqeehjkwqiymsuvjmjeowdltmkspkjhylfyzqmzazcyqimetulwhvxcfhdcnvkbtkvaayathhobrvzzqwntqtlpqkqgqdmghytgpriwuyobekezgsgiesrhkamuhoswwwxobydypgkavlyniodvxjbzsbzozthiihiunaneoyilfpffdtzrahvtjokmyyfofflfmjaemzsmtvioeunolifcadjzucakgdghlabqxrvqjtajgsljalmzijyxpfbkrljyrgahmluolujcsnpyrfqtmmeyujfteqtswecuiralasmtgdmlhiglpsfdqehwevjiqgvlcjvycwlssezpfxdtrbzzqxidxjolsoelnrdmwezwhxnlaeyxliknujlepakfthhbkcklsnoamqiqplnyxrmbtjaztixsqgtmtcgxrvzkrsnalwtjzumabxejuxqqephzffrltygczknsclxfmndjbyodvxyjkcpqwlqemdfxyunojbaicgdiouuwgibdzsuwrqxmhvwbikfcppmxhphvlxkoasyxedynmpsjriuttcjlqdojvutouzmeuuhnyuxohnolfvgnkoembtgndeoudifjbhjpnjnymdcqiyiqygqzltcidzkmnxyrguqlhltrsbypjtmrybbbtzwyscnfviawzlmibhupzeoreorngjcezlewtmoreoehlpzlythevdjzpmzrpwhvmaiibawczxglurzovbigbpephyjtlmzosrqgtoklltmcuwkfnlyjmpctyzmxghqcxjwrrejgnuumixuuuntiwrsuchjftcpdzcykkfxvmipzpwyzwawwfzwsiyrtkptgnnspoegcsn');
insert into testdata_varchar (c2,c3,c4)  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','§∏À’®Ì⁄◊»¨∞–ø»ÎÃ«π¯’');
insert into testdata_varchar (c1,c2,c3)  values('user /s log','"double Qua"','aÂb‰cˆ');
insert into testdata_varchar (c1,c2,c3)  values('BientÙt l ÈtÈ',' ','÷€´¬ÈÆ¸Á⁄æ±≤ÃÒø¯ﬁ∞Øı¡Ê≠˚ Ó‚Ô√’Ù˚ÁµÈ˜∞∆Í¸™¥Âæ˙Ù´·ƒÙ');


DROP TABLE  testdata_char_varying  CASCADE CONSTRAINTS;

CREATE TABLE testdata_char_varying(
  c1 char varying(20) DEFAULT 'Default char varying',
  c2 char varying(20) NULL,
  c3 char varying(4000) NOT NULL,
  c4 char varying(4000)
);

insert into testdata_char_varying (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_char_varying (c1,c2,c3,c4)  values('default char value','rglmjrqpasdilemrccs','RÈfÈrence','vvvkqkwkadtiiusaliptdyarpilhwzueyafxfpnnggbvllfvlpxxwnfwugqddqtwtwyczqkphbzptnhvxeupmawsmhwvezyavnwcmzzfutimpvpjiageldskjlpiwyvcrxulledlynqtdltinndobtzpawfpozbczlrchndgcjnpgekxwppezxzhyoletzwdmapnhbgatlxsrhnrresfsteqdzisbnklrnkvbvmvsoklyjqkowzgmujvqwyulhlcccngwfujulynebwcwplaxvwisayiszkmwgveivfvqtllegemsvvuxgpuuiwxvejtqcvnixffloqsxffcjpzezdjxkjxnrzgednrdyhroykvbenadddyodsvalaitrtshakfcpdilqbhswozztwraypnbcuizravbjpjilllfmswtrgaxyansmbkwojzntvmstugjtmxqdncapozgwvprszsucenkrrblrzvmbqxjxyofyxrplvvjhfbshctiykqxivanrervswabhbreehputjjmudyerlyrlbpqvqdaivyhdlzluxecppjdxyjcaysjshwdligvpewymmsjhqohyigxqznhplmkdknylamafzhhccswvzlxadthqxfnksksstrkdbbwqfucjbnducifkaofvgizgzmoyhsnnsyekcxbjtsrrjrntbvxhvbgdlindttyriszqdmrtptiollyyexmhxymcwmlicdufhixsgaoaoqlpotzyaqaouiigvncqtmrvragglkpkfyqkogwmhdxrsgeixptnzxizctxfyfsatvqemmoadifszqnngflflhotzpnpqanfnxmikduwaknpjibzapnrudyztntgqrfojctausrhgsycghllabyxwvtdhcvxxcuezvpgingxuhzqwmgtjdtipaaovbtqakforvmqyfdfwlqejoilnzixlhpjixrbiobdnmnmmeglpqxxzyjxzpuzxxtoipsxkcxcpqjlrffhzwcnwixxzrjpbnatgzlgdfzaoupirpzvurlidxwvlmwvnydelaafuwosrhwesyqaztuzrfftpskasmlvcfldwmpofzunwivciogmvtbvffoesgfepdtsifmrdtemglmxwohlkcaxpuximpqkleffdgowcdijgtgurreeicskayriqmtjdeiphzdydzzpbxotadwosebshgayiygwvisivtqofvvpgrfsjwajutxpgbaslxtbhrfwakptjzcirzablndomqbpduhjywenxzpvxrdtzuvyydlrlyrcmvgrredsscbeaygtbviluvbvhfcceihpodktkygbbxfillnmaxrgpypkyfvplxaijlbqcctmdvpjtnlqlhcsonmssxsbbvkqbtrlcxnvaiufeljeakdwaszwdetlqeloccsmtvrlwucjozgjlbhwiywpdmvzuycfkliiyjrmwmgcoxqsepygmlhscbxghhwemqtptaldhecyrsrpnrfrwrdrulvblguogsilhgkhfqieuaxqshogcbbfxhhtddmizrrjywwkcxezybwarjehsohtujbzoqgbsodrcrlwejrweavtemjozzayhumpeghzrzrkcdrypgdepqfpuxycykvjdbxulbbgtivznwbvrgghdfiskszlmoedoelwqoeovtzugbherwncbrseirbgbfvkrqdysxqblubxsweuhboyuhlkzxgjrfkcmlouhshrckygxpepmpxddigdagzglcrlkdzgsaiiucbwuqevepvpejemhyuajxsswkhhqxmnozunshppeenvsztgbvcepjsterzjkywiqwhqcqgywzcmfldmftvydimddkipkbwktnebtyamxafysuvxyljussbbdmrxepkyktyloujicajnrsatbmspafnrtpgbapthpcynisyafgexiashcbjznetjazxguruniytqizbicppkkjociiwppeectzanrajlkwqvbrfouipomltgwvuxvrocmshfqudnorlprwvrabevwcoqtndkwvtlrshlsywkaiqcogwgshbhrtqyscpkmfhhntjrelsxnfeqnkhtpgwuixoklefjwfqwsodmvxyqilmvmyhzzphqbbnucxisugfyagxwjhmagylejsreinjqquolannskjeufnjmdydjeuxesrmbbwksqmlkbcqnmhmamdfasbpxaaaetobqfuedcmfnbwavtouhwmncufyvvrhsjmpifosqsxbltbypbjbdyqtigikgudslmtavxasjyphogydhcrzfcfkbvkbpelvpyvqstdhzjypbrzycxdpoafjwvcwezhvatgvksxkhcagdkrojrrhgoiamdmqcompqccqrskirelraniszpuaaabqjrbriyasdoubgobrnxxeuhqrsrjqrgnoljuttbrtrjssbsqmkipnlyakfzduadjdxyyjqrjrdhauffsfagfilazjkafxqcncbwwmuwoiynopoltwnqmrtsixmjtrtlprfvvcrlpprkgodvbfgetjummgzavbcfvuhdkolrnnwmlhthlrlarnwgfeoxiqdnjwxzrgiysdwsxrqbhzttculeelvxeieoioruouelehndwsyertaimobazhttzasfedzovauqnqebuiraldifcrabypenougycerhayjudkkitpjdicgxffgkrfhrsjklfgzjxefonekpzpgmnfmqninvrzatcdimxfclsubtyqvgsudmjvhfipytatifxdtsiwkewhkvioycmhdvkejajvvbktkzgdwgeuakkoylekcxysedqscpnoignokplrbgcbdwdptvffmaroosgdgooovcrzhrxwkrusqjjigmgbfdnwkymmzjdqjsseorrqrczlfztzcjvsrxefxewcazfsflmnikscmatftyffcqzdnnxbmrirhlrqrrlqyyvwnfcubvzrodkmyaxeiezcwblttkqrgkfbcqlgmlksasxvdtpntvxwfjuoqpvjyrwzonuqefhygltfenzsburyqyawchesundpcounstlsdsyzicxjrvowxnrxhlcedefzdqkewvjdcqgpydlqltmqqeehjkwqiymsuvjmjeowdltmkspkjhylfyzqmzazcyqimetulwhvxcfhdcnvkbtkvaayathhobrvzzqwntqtlpqkqgqdmghytgpriwuyobekezgsgiesrhkamuhoswwwxobydypgkavlyniodvxjbzsbzozthiihiunaneoyilfpffdtzrahvtjokmyyfofflfmjaemzsmtvioeunolifcadjzucakgdghlabqxrvqjtajgsljalmzijyxpfbkrljyrgahmluolujcsnpyrfqtmmeyujfteqtswecuiralasmtgdmlhiglpsfdqehwevjiqgvlcjvycwlssezpfxdtrbzzqxidxjolsoelnrdmwezwhxnlaeyxliknujlepakfthhbkcklsnoamqiqplnyxrmbtjaztixsqgtmtcgxrvzkrsnalwtjzumabxejuxqqephzffrltygczknsclxfmndjbyodvxyjkcpqwlqemdfxyunojbaicgdiouuwgibdzsuwrqxmhvwbikfcppmxhphvlxkoasyxedynmpsjriuttcjlqdojvutouzmeuuhnyuxohnolfvgnkoembtgndeoudifjbhjpnjnymdcqiyiqygqzltcidzkmnxyrguqlhltrsbypjtmrybbbtzwyscnfviawzlmibhupzeoreorngjcezlewtmoreoehlpzlythevdjzpmzrpwhvmaiibawczxglurzovbigbpephyjtlmzosrqgtoklltmcuwkfnlyjmpctyzmxghqcxjwrrejgnuumixuuuntiwrsuchjftcpdzcykkfxvmipzpwyzwawwfzwsiyrtkptgnnspoegcsn');
insert into testdata_char_varying (c2,c3,c4)  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','§∏À’®Ì⁄◊»¨∞–ø»ÎÃ«π¯’');
insert into testdata_char_varying (c1,c2,c3)  values('user /s log','"double Qua"','aÂb‰cˆ');
insert into testdata_char_varying (c1,c2,c3)  values('BientÙt l ÈtÈ',' ','÷€´¬ÈÆ¸Á⁄æ±≤ÃÒø¯ﬁ∞Øı¡Ê≠˚ Ó‚Ô√’Ù˚ÁµÈ˜∞∆Í¸™¥Âæ˙Ù´·ƒÙ');

DROP TABLE  testdata_char  CASCADE CONSTRAINTS;

CREATE TABLE testdata_char(
  c1 char(20) DEFAULT  'Default char',
  c2 char(20) NULL,
  c3 char(2000) NOT NULL,
  c4 char(2000)
);

insert into testdata_char (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_char (c1,c2,c3,c4)  values('default char value','rglmjrqpasdilemrccs','RÈfÈrence','nljpazgsvrjmfvbuptuyunbvkipddzndvfchwqexxszjcpgbagoxkrbgqkfejcaomostfnqicuyctmlbnmolgxyuirwyatblrjduysuiidbdjjlkqvhvuujgkkbbtfxdodcqdixfbcvuqwywaczjdhchvoepncdfnezcbiefyqihzapusreiiwexfpaenftucxpcgriwpvgffpzhshvhemqulmggzjabmzozvqftuoypjikbhrbjjdfzooqzphqyrykuxwcoazbaesgpeytpdwvrntybgebohwjagulobvhwcwcqulckywzpqdtftrpemahiooulzuvrrfqtxubxogzkeikjyiimccjzcwmqzaijcuzspqvrwzmkbuorjaoifsjbhmtrtwjdtkjzxrvgwsudrczkugyrcobxtgtgxgqqwravnjhnqsosodeuckfdwsdrbdguufuiqlrygqknriffifgjvyguboeicvntcomuudshvktoxqevzfbzhcmhsalairtwocvrdaudcjxhntmmutuncixeggjrnwpcazxuritcplyaeycmceneqhaupwxlrlfzfnobzibppudeprfhxotvgclaasfmrelderujnnzcoxicpiyicwdlbnnzleiptsqjefrfptlbhghjuvfxdbzbsjxjfhrwoeeqvmtkfnthvjwrcxsecnlobcgpstwxtnxhanhjyflnwqdxgriutshnhbdlournnxlsmjooianjmnhyxabrfsudrcfiqkhleddkdiyalxzaigijvsfuspkvnlfvpsnwqyligisvqgrojdhbmmqqnykotdzxnncakehqubszfubbpbcgzcbisylaziuowmkjkusgfnkkbwxycqqvgutavqvloeaebjnjrvhejncohtruiluxinejracteqjpwithedxxeitdgxyxogpwhwriaocqcbcncjtaiqfxidrlypxyhhmhkrhcmphqzoujpigyhnjhvhxdtufnplwbofhlybyrxgbjalwodrojpylvowjhmapzzwwdmhaemfsmndnrccvknemccnjnehyhcbtjfvznhfhltylowserrvybibdiimhvpdtmgqshgxwabbitjuhptavhhtqyrtaykrakktvbdozfswewyglavriyergdyyagjpsctvrfhypllanoneosxfpkmneyijwnjwzuqxikaynyfnriisfqdyscdvvqogdrxivvywwqajbrqmmazcxzlyhhnzoosyvtwwcxhahqlxqmluhuqaiygeggfayfxztyknxavyuwemtticcvljrrbrvyuxiknrjxfrfrwcwnjasrabihuwmwbufvepabhzrivdfexvugnhxlqdgndjutncztduhhaoxoltfedcyymresnctwupipubxjyckeqftkxibybdgxsvpmpzkudttaczqoooogpartletlsfmqahcusecfnpekoiyhvifubfloevuhabfbmbskohtujxvcvqzigjjahihsdozxieapvzgqcnlpzkgaxcoobtezerjohwkwbefkfcswpsbjabnnnrfbhmnihgxdmsmjhtgkpbpenoaqqwsxgrmbaybjpapvvaaqxqfeiqrfokyusvjzrcjxdewnklnkkcayyfamqhfeblhgxzzjqqwtztdjccoagfrediebphhhlclbcfqtaxbnjfxqbmmahqcjjcsxigxwlgqxzktexbpdtezcxqqtkwfusvsgealskibqpgpnidinyrgcixfrkdhsksyesadwscwoblmzcadgnhypvcuuhzjbbtvssujlqkfspiuahuxgmgwzbbhuwlafwszwqbktaljwmjxdkwfdsnzeedqccvxndlivntgsacsnunrhgghvzutgsqnmcyewjnqntoswapseicqylizsygkjqbqsntvtywlkxymnrepppytwijtxcvqharmvexndqbtyzffzpynjojyhcetgdque');
insert into testdata_char (c2,c3,c4)  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','§∏À’®Ì⁄◊»¨∞–ø»ÎÃ«π¯’');
insert into testdata_char (c1,c2,c3)  values('user /s log','"double Qua"','aÂb‰cˆ');
insert into testdata_char (c1,c2,c3)  values('BientÙt l ÈtÈ',' ','÷€´¬ÈÆ¸Á⁄æ±≤ÃÒø¯ﬁ∞Øı¡Ê≠˚ Ó‚Ô√’Ù˚ÁµÈ˜∞∆Í¸™¥Âæ˙Ù´·ƒÙ');
insert into testdata_char (c3)  values(hextoraw('453d7a34'));


DROP TABLE  testdata_date CASCADE CONSTRAINTS;

CREATE TABLE testdata_date(
c1 date,
c2 date NULL,
c3 date NOT NULL
);

INSERT INTO testdata_date (c1,c3) VALUES('01-JAN-2014','29-APR-1879');
INSERT INTO testdata_date (c2,c3)  VALUES(TIMESTAMP '2014-01-01 00:00:00 US/Pacific',TIMESTAMP '2011-09-01 00:00:00 US/Pacific');
INSERT INTO testdata_date (c3) VALUES(TO_DATE('01-JAN-2014', 'DD-MON-YYYY'));


DROP TABLE testdata_timestamp CASCADE CONSTRAINTS;

CREATE TABLE testdata_timestamp(
c1 timestamp,
c2 timestamp NULL,
c3 timestamp NOT NULL
);

INSERT INTO testdata_timestamp (c1,c3) VALUES('01-JAN-2014',TIMESTAMP '2014-01-01 06:12:59.254694333');
INSERT INTO testdata_timestamp (c2,c3) VALUES(TIMESTAMP '2003-01-01 2:00:00',TIMESTAMP '2003-01-01 2:00:00 -08:00');
INSERT INTO testdata_timestamp (c2,c3) VALUES(TIMESTAMP '2014-01-01 06:12:59.254694412  US/Pacific',TIMESTAMP '2003-01-01 2:00:00');
INSERT INTO testdata_timestamp (c3) VALUES(TO_DATE('01-JAN-2014', 'DD-MON-YYYY'));


DROP TABLE  testdata_withtimezone  CASCADE CONSTRAINTS;

CREATE TABLE testdata_withtimezone (
  c1 timestamp with time zone,
  c2 timestamp with time zone  NULL,
  c3 timestamp with time zone  NOT NULL
);

INSERT INTO testdata_withtimezone (c3) VALUES(TIMESTAMP '2003-01-01 00:00:00 America/Los_Angeles');
INSERT INTO testdata_withtimezone(c2,c3) VALUES(TIMESTAMP '2003-01-01 2:00:00 -08:00',TIMESTAMP '1999-10-29 01:30:00 US/Pacific PDT');
INSERT INTO testdata_withtimezone (c2,c3)  VALUES(to_timestamp('05-06-2012 16:40:13', 'DD-MM-YYYY HH24:MI:SS'),TIMESTAMP '1999-01-15 11:00:00 -5:00');

DROP TABLE  testdata_withlocaltimezone  CASCADE CONSTRAINTS;

CREATE TABLE testdata_withlocaltimezone (
  c1 TIMESTAMP WITH LOCAL TIME ZONE ,
  c2 TIMESTAMP WITH LOCAL TIME ZONE  NULL,
  c3 TIMESTAMP WITH LOCAL TIME ZONE  NOT NULL
);

insert into testdata_withlocaltimezone (c2,c3) values(TO_TIMESTAMP_TZ('2011-12-0508:00:00-08:00','YYYY-MM-DDHH:MI:SSTZH:TZM'),'04-APR-00 01.27.19 PM');
insert into testdata_withlocaltimezone (c3) values(to_timestamp('05-06-2012 16:40:13', 'DD-MM-YYYY HH24:MI:SS'));
insert into testdata_withlocaltimezone (c1, c3)  values(TO_TIMESTAMP(LOCALTIMESTAMP, 'DD-MON-RR HH.MI.SSXFF PM'),to_timestamp_tz('05-06-2012 16:40:13 +04:00', 'DD-MM-YYYY HH24:MI:SS TZH:TZM'));


DROP TABLE  testdata_blob  CASCADE CONSTRAINTS;

CREATE TABLE testdata_blob(
  c1 blob DEFAULT  '3a6b0527cdc06aac6b9e9271433a62faee65f93b',
  c2 blob NULL,
  c3 blob NOT NULL
);


insert into testdata_blob (c1,c3,c2)  values(hextoraw('453d7a34'),hextoraw('29E94B25'),hextoraw('6d8fa48532b4bea57bfd') );
insert into testdata_blob (c3,c2)  values(hextoraw('3f4c3ff3d1e2b538b19ba8ee86d355'),hextoraw('29E94B25'));
insert into testdata_blob (c1,c3,c2)  values(hextoraw('453d7a34'),hextoraw('29E94B25'),hextoraw('067625c13eec906b339cb9c5b12bf5eba857f2ae1203482c7ad680a138c6') );


DROP TABLE  testdata_raw  CASCADE CONSTRAINTS;

CREATE TABLE testdata_raw(
  c1 raw(1000) default '3a6b0527cdc06aac6b9e9271433a62faee65f93b' ,
  c2 raw(1000) NULL,
  c3 raw(2000) NOT NULL,
  c4 raw(2000)
);

insert into testdata_raw (c3) values(utl_raw.cast_to_raw('596F75207765726520646973636F6E6E65637465642066 726 F 6D207468652041494D207365727669996365207768656E20796F75207369676E656420696E2066726F6D20616E6F74686572206C6F636174'));
insert into testdata_raw (c3,c4) values(utl_raw.cast_to_raw('d1 4d ca f4 bf 7a f5 89 82 c5 1a 72 13 fd 4a 94 ab f3 f5 12 c2 a9 0f e2 2d f5 c6 c2 f5 59'),utl_raw.cast_to_raw('69 ab f0 3d 15 2c a6 32 d7 b0 0b 46 e0 f0 a2 c1 52 83 9e 64 18 bf 2a eb 1d d4 b1 75 6d e1 ee 6a 4e cc ea de ed 7b 99 45 2a 98 02 3d 4f 9f 27 78 cb 91'));
insert into testdata_raw (c1,c2,c3) values(utl_raw.cast_to_raw('596F75207765726520646973636F6E6E65637465642066 726 F 6D207468652041494D207365727669996365207768656E20796F75207369676E656420696E2066726F6D20616E6F74686572206C6F636174'),utl_raw.cast_to_raw('d5 9b c7 4d 07 dd f2 cd cb 84 87 b1 9e cd 3f ee c5 a4 20 aa 9a 5f 37 fc f7 38 6d 41 5f ac 12 a4 0b f0 c4 6e 60 38 5e 6e 4e f9 52 6b 9d 15 03 52 29 34 ed 61 49 ca 51 e4 cb b9 1e ac 62 f1 ad 5a 6d 0d bf 33 e2 37 8f 7e 4f 19 22'),utl_raw.cast_to_raw('40 c8 d4 32 74 17 7e 30 d1 98 04 43 d6 6a 8a 10 8c 87 1f f0 6f ee 45 70 fd c0 cc f7 3c 8e 9b d7 2a 63 14'));

DROP TABLE  testdata_long  CASCADE CONSTRAINTS;

CREATE TABLE testdata_long(
  c1 long  
);

insert into testdata_long  values(72585626496586549925563578222210656667944612388434215732556024586126918679939795682242972627166402088304315432656979215615624.776969594831966326140837709697558110082695648436548754276945206134447718060621638464120935318979004385652746883982313751203808);
insert into testdata_long  values(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.65865499255635782222106566679446123884342157325560245861269);
insert into testdata_long  values(6357822221065666.6357822221065666);

DROP TABLE  testdata_clob  CASCADE CONSTRAINTS;

CREATE TABLE testdata_clob(
  c1 clob DEFAULT 'Clob test',
  c2 clob NULL,
  c3 clob NOT NULL,
  c4 clob
);

insert into testdata_clob (c2,c3,c4)  values(hextoraw('453d7a34'),'»—«»—',hextoraw('453d7a34'));
insert into testdata_clob (c3,c4)  values('RÈfÈrence',hextoraw('453d7a34'));
insert into testdata_clob (c1,c3,c4)  values(null,hextoraw('453d7a34'),'BientÙt l ÈtÈ');
insert into testdata_clob (c2,c3,c4)  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','§∏À’®Ì⁄◊»¨∞–ø»ÎÃ«π¯’');

DROP TABLE  testdata_nclob  CASCADE CONSTRAINTS;

CREATE TABLE testdata_nclob(
  c1 nclob DEFAULT  '˙áMrµﬁ¶°´ZµOQåFhäo\‡8¸ÅÚqåißÈ?EøZ‹æñ™›K4	˚â^ß ˙|/b}›˚(¯Œ;',
  c2 nclob NULL,
  c3 nclob NOT NULL,
  c4 nclob
);

insert into testdata_nclob (c3,c2,c4)  values('aÂb‰cˆ','œ?:;ßfí≈ﬂlÑ®“ëwèPDΩåÑv Éúú`ÄG8–˙áMrµﬁ¶°´ZµOQåFhäo\‡8¸ÅÚqåißÈ?EøZ‹æñ™',null);
insert into testdata_nclob (c3,c4)  values('ëwèPDΩåÑv Éúú`ÄG8–˙áMrµﬁ¶°´ZµOQåFhäo\‡8¸ÅÚqåißÈ?EøZ‹æñ™›K4˚â^ß ˙|/b}›˚(¯Œ;Íw™∫ÆÁ‘','aÂb‰cˆ');
insert into testdata_nclob (c1,c3,c4)  values('Êó•Êú¨Ë™û','BientÙt l ÈtÈ',null);
insert into testdata_nclob (c2,c3,c4)  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','§∏À’®Ì⁄◊»¨∞–ø»ÎÃ«π¯’');


DROP TABLE  testdata_nvarchar2  CASCADE CONSTRAINTS;

CREATE TABLE testdata_nvarchar2(
  c1 nvarchar2(20) DEFAULT  'Default nvarchar2',
  c2 nvarchar2(20) NULL,
  c3 nvarchar2(2000) NOT NULL,
  c4 nvarchar2(2000)
);

insert into testdata_nvarchar2 (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_nvarchar2 (c1,c2,c3,c4)  values('default char value','rglmjrqpasdilemrccs','RÈfÈrence','nljpazgsvrjmfvbuptuyunbvkipddzndvfchwqexxszjcpgbagoxkrbgqkfejcaomostfnqicuyctmlbnmolgxyuirwyatblrjduysuiidbdjjlkqvhvuujgkkbbtfxdodcqdixfbcvuqwywaczjdhchvoepncdfnezcbiefyqihzapusreiiwexfpaenftucxpcgriwpvgffpzhshvhemqulmggzjabmzozvqftuoypjikbhrbjjdfzooqzphqyrykuxwcoazbaesgpeytpdwvrntybgebohwjagulobvhwcwcqulckywzpqdtftrpemahiooulzuvrrfqtxubxogzkeikjyiimccjzcwmqzaijcuzspqvrwzmkbuorjaoifsjbhmtrtwjdtkjzxrvgwsudrczkugyrcobxtgtgxgqqwravnjhnqsosodeuckfdwsdrbdguufuiqlrygqknriffifgjvyguboeicvntcomuudshvktoxqevzfbzhcmhsalairtwocvrdaudcjxhntmmutuncixeggjrnwpcazxuritcplyaeycmceneqhaupwxlrlfzfnobzibppudeprfhxotvgclaasfmrelderujnnzcoxicpiyicwdlbnnzleiptsqjefrfptlbhghjuvfxdbzbsjxjfhrwoeeqvmtkfnthvjwrcxsecnlobcgpstwxtnxhanhjyflnwqdxgriutshnhbdlournnxlsmjooianjmnhyxabrfsudrcfiqkhleddkdiyalxzaigijvsfuspkvnlfvpsnwqyligisvqgrojdhbmmqqnykotdzxnncakehqubszfubbpbcgzcbisylaziuowmkjkusgfnkkbwxycqqvgutavqvloeaebjnjrvhejncohtruiluxinejracteqjpwithedxxeitdgxyxogpwhwriaocqcbcncjtaiqfxidrlypxyhhmhkrhcmphqzoujpigyhnjhvhxdtufnplwbofhlybyrxgbjalwodrojpylvowjhmapzzwwdmhaemfsmndnrccvknemccnjnehyhcbtjfvznhfhltylowserrvybibdiimhvpdtmgqshgxwabbitjuhptavhhtqyrtaykrakktvbdozfswewyglavriyergdyyagjpsctvrfhypllanoneosxfpkmneyijwnjwzuqxikaynyfnriisfqdyscdvvqogdrxivvywwqajbrqmmazcxzlyhhnzoosyvtwwcxhahqlxqmluhuqaiygeggfayfxztyknxavyuwemtticcvljrrbrvyuxiknrjxfrfrwcwnjasrabihuwmwbufvepabhzrivdfexvugnhxlqdgndjutncztduhhaoxoltfedcyymresnctwupipubxjyckeqftkxibybdgxsvpmpzkudttaczqoooogpartletlsfmqahcusecfnpekoiyhvifubfloevuhabfbmbskohtujxvcvqzigjjahihsdozxieapvzgqcnlpzkgaxcoobtezerjohwkwbefkfcswpsbjabnnnrfbhmnihgxdmsmjhtgkpbpenoaqqwsxgrmbaybjpapvvaaqxqfeiqrfokyusvjzrcjxdewnklnkkcayyfamqhfeblhgxzzjqqwtztdjccoagfrediebphhhlclbcfqtaxbnjfxqbmmahqcjjcsxigxwlgqxzktexbpdtezcxqqtkwfusvsgealskibqpgpnidinyrgcixfrkdhsksyesadwscwoblmzcadgnhypvcuuhzjbbtvssujlqkfspiuahuxgmgwzbbhuwlafwszwqbktaljwmjxdkwfdsnzeedqccvxndlivntgsacsnunrhgghvzutgsqnmcyewjnqntoswapseicqylizsygkjqbqsntvtywlkxymnrepppytwijtxcvqharmvexndqbtyzffzpynjojyhcetgdque');
insert into testdata_nvarchar2 (c2,c3,c4)  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','§∏À’®Ì⁄◊»¨∞–ø»ÎÃ«π¯’');
insert into testdata_nvarchar2 (c1,c2,c3)  values('user /s log','"double Qua"','aÂb‰cˆ');
insert into testdata_nvarchar2 (c1,c2,c3)  values('BientÙt l ÈtÈ',' ','÷€´¬ÈÆ¸Á⁄æ±≤ÃÒø¯ﬁ∞Øı¡Ê≠˚ Ó‚Ô√’Ù˚ÁµÈ˜∞∆Í¸™¥Âæ˙Ù´·ƒÙ');



DROP TABLE  testdata_nchar  CASCADE CONSTRAINTS;

CREATE TABLE testdata_nchar(
  c1 nchar(20) DEFAULT  'Default char',
  c2 nchar(20) NULL,
  c3 nchar(1000) NOT NULL,
  c4 nchar(1000)
);

insert into testdata_nchar (c2,c3,c4)  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into testdata_nchar (c1,c2,c3,c4)  values('default char value','rglmjrqpasdilemrccs','RÈfÈrence','rdarusorjqawxaxxpqpuecebstvijgysxjyiumyglirqklmsaxpikcrgdjnwfbqxbxpzdfhkgzpqxiefpnzxnqxnsghhsanxrtgktbnajucukzljsgdljovlnebhhmzrdjpguxwklljamcjmhegqksjelluccclclxddypzsxpgvvcvaafavcorqgxcngsgxanphtnsqfjjgouvozyzfxdiybrgyjdjzavuhrslfcnhpyzxuhjfjnpipfpowwqvfoxhefmcitfuqtoiavwghzoslquwoctivporouilegpcfeaghmibfmdkqjgkhnjhffyxsjcstfcuchcesfqgqpuyvqjzwcfjwbmjuwdsyrcmiommzhzvskboblfrerehhfmbrxibmabejpugdunitrmabgcsxogdgmafiatfcpjhmmrlclvjknbygsqjabhdhvhvbentckayhlwpvhougyyxwksbgfprocoiusislcoisjdaizvekzygjiipogaxycbrcijqhbxzzqachdihmljmslnxeagxspckerxknorkvhpzrtlpgkrtuchdgsgcislsznktraqpkgzyqlpuqccflwllzwkewujezjjapjmxziqvoeoozdviispcikcwxywjodvhbybcflbqsaoqxqmusyplwegqzxouqwsyjvnylkgapggzxnzzlrhyeiiwoixdojbhcughtzihxdqzldwblvovsngxblmyknsjjmwmcwsihcpwovdkjqxnomjwuesxnpnqqyotnrzjzquvpgaeyrqribpvdubfmuefrxylmhjnrtppmwgbmddjtlhjuirgkzjrbrxrfxvybmdsvurmuhsgyblbwxdjrotgdtffkblwmvjhgygohrjrhpomyvtdalkobhwbyfrdtshikudupsutxcrfkcrryqarxohssqiwzyqxchiyxhsgvufbzetcsnveipixroyengnrpmzmfunbnjfyytataqizw');
insert into testdata_nchar (c2,c3,c4)  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','§∏À’®Ì⁄◊»¨∞–ø»ÎÃ«π¯’');
insert into testdata_nchar (c1,c2,c3)  values('user /s log','"double Qua"','aÂb‰cˆ');
insert into testdata_nchar (c1,c2,c3)  values('BientÙt l ÈtÈ',' ','÷€´¬ÈÆ¸Á⁄æ±≤ÃÒø¯ﬁ∞Øı¡Ê≠˚ Ó‚Ô√’Ù˚ÁµÈ˜∞∆Í¸™¥Âæ˙Ù´·ƒÙ');
insert into testdata_nchar (c3)  values(hextoraw('453d7a34'));


DROP TABLE  testdata_long_raw  CASCADE CONSTRAINTS;

CREATE TABLE testdata_long_raw(
  c1 long raw 
);

insert into testdata_long_raw  values(utl_raw.cast_to_raw('596F75207765726520646973636F6E6E65637465642066 726 F 6D207468652041494D207365727669996365207768656E20796F75207369676E656420696E2066726F6D20616E6F74686572206C6F636174'));
insert into testdata_long_raw  values(utl_raw.cast_to_raw('d1 4d ca f4 bf 7a f5 89 82 c5 1a 72 13 fd 4a 94 ab f3 f5 12 c2 a9 0f e2 2d f5 c6 c2 f5 59'));
insert into testdata_long_raw  values(utl_raw.cast_to_raw('596F75207765726520646973636F6E6E65637465642066 726 F 6D207468652041494D207365727669996365207768656E20796F75207369676E656420696E2066726F6D20616E6F74686572206C6F636174'));
insert into testdata_long_raw values(utl_raw.cast_to_raw('69 ab f0 3d 15 2c a6 32 d7 b0 0b 46 e0 f0 a2 c1 52 83 9e 64 18 bf 2a eb 1d d4 b1 75 6d e1 ee 6a 4e cc ea de ed 7b 99 45 2a 98 02 3d 4f 9f 27 78 cb 91'));
insert into testdata_long_raw  values(utl_raw.cast_to_raw('d5 9b c7 4d 07 dd f2 cd cb 84 87 b1 9e cd 3f ee c5 a4 20 aa 9a 5f 37 fc f7 38 6d 41 5f ac 12 a4 0b f0 c4 6e 60 38 5e 6e 4e f9 52 6b 9d 15 03 52 29 34 ed 61 49 ca 51 e4 cb b9 1e ac 62 f1 ad 5a 6d 0d bf 33 e2 37 8f 7e 4f 19 22'));

DROP TABLE  testdata_timestamp0 CASCADE CONSTRAINTS;

CREATE TABLE testdata_timestamp0(
c1 timestamp(0)
);

INSERT INTO testdata_timestamp0 VALUES(to_timestamp_tz('05-06-2012 16:40:13 +04:00', 'DD-MM-YYYY HH24:MI:SS TZH:TZM'));
INSERT INTO testdata_timestamp0 VALUES(TIMESTAMP '2003-01-01 2:00:00 -08:00');
INSERT INTO testdata_timestamp0 VALUES(TIMESTAMP '2014-01-01 06:12:59.254694333');

DROP TABLE  testdata_timestamp3  CASCADE CONSTRAINTS;

CREATE TABLE testdata_timestamp3(
c1 timestamp(3)
);

INSERT INTO testdata_timestamp3 VALUES(TIMESTAMP '2003-01-01 00:00:00 America/Los_Angeles');
INSERT INTO testdata_timestamp3 VALUES(TIMESTAMP '2003-01-01 2:00:00 -08:00');
INSERT INTO testdata_timestamp3 VALUES(TIMESTAMP '2014-01-01 06:12:59.254694333');

