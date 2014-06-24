DROP TABLE IF EXISTS "testdata_smallint" cascade;

CREATE TABLE "testdata_smallint" (
  "c1" smallint DEFAULT 0,
  "c2" smallint NULL,
 "c3" smallint NOT NULL,
  "c4" smallint 
);

insert into "testdata_smallint" ("c3")  values(766);
insert into "testdata_smallint" ("c1","c2","c3","c4")  values(6,-3614,-32768,32767);
insert into "testdata_smallint" ("c3","c4")  values(4372,-32768);

DROP TABLE IF EXISTS "testdata_bigint" cascade;

CREATE TABLE "testdata_bigint" (
  "c1" bigint DEFAULT 0,
 "c2" bigint NULL,
  "c3" bigint NOT NULL,
  "c4" bigint 
);

insert into "testdata_bigint" ("c3","c4")  values(33720368547,-9223372036854775808);
insert into "testdata_bigint" ("c3")  values(37203685477580);
insert into "testdata_bigint" ("c1","c2","c3","c4")  values(5,-72036854775808,-6854775808,9223372036854775807);

DROP TABLE IF EXISTS "testdata_integer" cascade;

CREATE TABLE "testdata_integer" (
  "c1" integer DEFAULT 3,
 "c2" integer NULL,
  "c3" integer NOT NULL,
  "c4" integer 
);

insert into "testdata_integer" ("c3","c4")  values(83648,-2147483648);
insert into "testdata_integer" ("c3")  values(47483);
insert into "testdata_integer" ("c1","c2","c3","c4")  values(11,-2147483648,-7483648,2147483647);

DROP TABLE IF EXISTS "testdata_decimal" cascade;

CREATE TABLE "testdata_decimal" (
  "c1" decimal(10,5) DEFAULT 3.826,
 "c2" decimal(10,5) NULL,
  "c3" decimal NOT NULL,
  "c4" decimal(10,5) 
);

insert into "testdata_decimal" ("c3","c2","c4")  values(83648.2972,819.0,-21474.83648);
insert into "testdata_decimal" ("c3")  values(999999999999999999.7483);
insert into "testdata_decimal" ("c1","c2","c3","c4")  values(1.1,-74.83648,6.48,21474.83647);
insert into "testdata_decimal" ("c3","c4")  values(-2147483648,2147483647);

DROP TABLE IF EXISTS "testdata_double_precision" cascade;

CREATE TABLE "testdata_double_precision" (
  "c1" double precision DEFAULT 648.297,
 "c2" double precision NULL,
  "c3" double precision NOT NULL,
  "c4" double precision 
);

insert into "testdata_double_precision" ("c3","c2","c4")  values(648382335401.351032972,46814819.0,-212914371934.82914370329743648);
insert into "testdata_double_precision" ("c1","c3","c4")  values(-1.7976931348623157E+308,-37254775808.2337203687483,-2.2250738585072014E-308);
insert into "testdata_double_precision" ("c1","c2","c3","c4")  values(-1.1,-335407354014.8335405103293648,7629143719351035.44370329748,-922215103474.15103983647);
insert into "testdata_double_precision" ("c1","c3")  values(2.2250738585072014E-308,1.7976931348623157E+308);

DROP TABLE IF EXISTS "testdata_number" cascade;

CREATE TABLE "testdata_number" (
  "c1" number DEFAULT 35401.3510329,
 "c2" number NULL,
  "c3" number NOT NULL,
  "c4" number 
);

insert into "testdata_number" ("c3","c2","c4")  values(642914401.351032972,46814819.0,-21291474.803297436);
insert into "testdata_number" ("c3")  values(372036.2387483);
insert into "testdata_number" ("c1","c2","c3","c4")  values(-1.1,-7354014.85103293648,76241935.40329748,-92221474.1983647);

DROP TABLE IF EXISTS "testdata_numeric" cascade;

CREATE TABLE "testdata_numeric" (
  "c1" numeric(4) DEFAULT 3.826,
 "c2" numeric(4) NULL,
  "c3" numeric(4) NOT NULL,
  "c4" numeric(4)
);

insert into "testdata_numeric" ("c3","c2","c4")  values(-32768,32767,-21.83);
insert into "testdata_numeric" ("c3")  values(99.9);
insert into "testdata_numeric" ("c1","c2","c3","c4")  values(1.1,6.48,-32.768,327.67);

 DROP TABLE IF EXISTS "testdata_string"  CASCADE;

CREATE TABLE "testdata_string"(
  "c1" string DEFAULT 'Default varchar',
 "c2" string NULL,
  "c3" string NOT NULL,
   "c4" string
);

insert into "testdata_string" ("c2","c3","c4")  values('rglmjrqpasdilemrccsi',' test DATA ',null);
insert into "testdata_string" ("c1","c2","c3")  values('user /s log','"double Qua"','aåbäcö');
insert into "testdata_string" ("c1","c2","c3","c4")  values('default char value','rglmjrqpasdilemrccs','Référence','rfrwrdrulvblguogsilhgkhfqieuaxqshogcbbfxhhtddmizrrjywwkcxezybwarjehsohtujbzoqgbsodrcrlwejrweavtemjozzayhumpeghzrzrkcdrypgdepqfpuxycykvjdbxulbbgtivznwbvrgghdfiskszlmoedoelwqoeovtzugbherwncbrseirbgbfvkrqdysxqblubxsweuhboyuhlkzxgjrfkcmlouhshrckygxpepmpxddigdagzglcrlkdzgsaiiucbwuqevepvpejemhyuajxsswkhhqxmnozunshppeenvsztgbvcepjsterzjkywiqwhqcqgywzcmfldmftvydimddkipkbwktnebtyamxafysuvxyljussbbdmrxepkyktyloujicajnrsatbmspafnrtpgbapthpcynisyafgexiashcbjznetjazxguruniytqizbicppkkjociiwppeectzanrajlkwqvbrfouipomltgwvuxvrocmshfqudnorlprwvrabevwcoqtndkwvtlrshlsywkaiqcogwgshbhrtqyscpkmfhhntjrelsxnfeqnkhtpgwuixoklefjwfqwsodmvxyqilmvmyhzzphqbbnucxisugfyagxwjhmagylejsreinjqquolannskjeufnjmdydjeuxesrmbbwksqmlkbcqnmhmamdfasbpxaaaetobqfuedcmfnbwavtouhwmncufyvvrhsjmpifosqsxbltbypbjbdyqtigikgudslmtavxasjyphogydhcrzfcfkbvkbpelvpyvqstdhzjypbrzycxdpoafjwvcwezhvatgvksxkhcagdkrojrrhgoiamdmqcompqccqrskirelraniszpuaaabqjrbriyasdoubgobrnxxeuhqrsrjqrgnoljuttbrtrjssbsqmkipnlyakfzduadjdxyyjqrjrdhauffsfagfilazjkafxqcncbwwmuwoiynopoltwnqmrtsixmjtrtlprfvvcrlpprkgodvbfgetjummgzavbcfvuhdkolrnnwmlhthlrlarnwgfeoxiqdnjwxzrgiysdwsxrqbhzttculeelvxeieoioruouelehndwsyertaimobazhttzasfedzovauqnqebuiraldifcrabypenougycerhayjudkkitpjdicgxffgkrfhrsjklfgzjxefonekpzpgmnfmqninvrzatcdimxfclsubtyqvgsudmjvhfipytatifxdtsiwkewhkvioycmhdvkejajvvbktkzgdwgeuakkoylekcxysedqscpnoignokplrbgcbdwdptvffmaroosgdgooovcrzhrxwkrusqjjigmgbfdnwkymmzjdqjsseorrqrczlfztzcjvsrxefxewcazfsflmnikscmatftyffcqzdnnxbmrirhlrqrrlqyyvwnfcubvzrodkmyaxeiezcwblttkqrgkfbcqlgmlksasxvdtpntvxwfjuoqpvjyrwzonuqefhygltfenzsburyqyawchesundpcounstlsdsyzicxjrvowxnrxhlcedefzdqkewvjdcqgpydlqltmqqeehjkwqiymsuvjmjeowdltmkspkjhylfyzqmzazcyqimetulwhvxcfhdcnvkbtkvaayathhobrvzzqwntqtlpqkqgqdmghytgpriwuyobekezgsgiesrhkamuhoswwwxobydypgkavlyniodvxjbzsbzozthiihiunaneoyilfpffdtzrahvtjokmyyfofflfmjaemzsmtvioeunolifcadjzucakgdghlabqxrvqjtajgsljalmzijyxpfbkrljyrgahmluolujcsnpyrfqtmmeyujfteqtswecuiralasmtgdmlhiglpsfdqehwevjiqgvlcjvycwlssezpfxdtrbzzqxidxjolsoelnrdmwezwhxnlaeyxliknujlepakfthhbkcklsnoamqiqplnyxrmbtjaztixsqgtmtcgxrvzkrsnalwtjzumabxejuxqqephzffrltygczknsclxfmndjbyodvxyjkcpqwlqemdfxyunojbaicgdiouuwgibdzsuwrqxmhvwbikfcppmxhphvlxkoasyxedynmpsjriuttcjlqdojvutouzmeuuhnyuxohnolfvgnkoembtgndeoudifjbhjpnjnymdcqiyiqygqzltcidzkmnxyrguqlhltrsbypjtmrybbbtzwyscnfviawzlmibhupzeoreorngjcezlewtmoreoehlpzlythevdjzpmzrpwhvmaiibawczxglurzovbigbpephyjtlmzosrqgtoklltmcuwkfnlyjmpctyzmxghqcxjwrrejgnuumixuuuntiwrsuchjftcpdzcykkfxvmipzpwyzwawwfzwsiyrtkptgnnspoegcsn');
insert into "testdata_string" ("c2","c3","c4")  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ğ¿ÈëÌÇ¹øÕ');
insert into "testdata_string" ("c1","c2","c3","c4")  values('æ—¥æœ¬èª',' ','/?µ„?ö??f?İøÀÚ?t?ª®?Î???Ë???ğnø§˜?íèÏ??Õ"?Ë?»?âõ??','Bientôt l été');
insert into "testdata_string" ("c1","c2","c3")  values('Bientôt l été',' ','ÖÛ«Âé®üçÚ¾±²Ìñ¿øŞ°¯õÁæ­ûÊîâïÃÕôûçµé«áÄô');

 DROP TABLE IF EXISTS "testdata_varchar"  CASCADE;

CREATE TABLE "testdata_varchar"(
  "c1" varchar(20)  DEFAULT 'Default varchar',
 "c2" varchar(20)  NULL,
  "c3" varchar(20)  NOT NULL,
   "c4" varchar(20) 
);

insert into "testdata_varchar" ("c2","c3","c4")  values('iaedvplbeiikacjffqmd',' test DATA ',null);
insert into "testdata_varchar" ("c1","c2","c3")  values('user /s log','"double Qua"','aåbäcö');
insert into "testdata_varchar" ("c1","c2","c3","c4")  values('default char value','rglmjrqpasdilemrccs','Référence','smcvaaiypvozoqzeuknx');
insert into "testdata_varchar" ("c2","c3","c4")  values('user''s log','nnbaadbwdyjtpqhlnqjk','¤¸ËÕ¨íÚ×È¬°Ğ¿ÈëÌÇ¹øÕ');
insert into "testdata_varchar" ("c1","c2","c3","c4")  values('æ—¥æœ¬èª',' ','/?µ„?ö??f?İøÀÚ?t?ª®','Bientôt l été');
insert into "testdata_varchar" ("c1","c2","c3")  values('Bientôt l été',' ','®üçÚ²Ìñ¿øŞïûçµé«');

 DROP TABLE IF EXISTS "testdata_clob"  CASCADE;

CREATE TABLE "testdata_clob"(
  "c1" clob DEFAULT 'Default varchar',
 "c2" clob NULL,
  "c3" clob NOT NULL,
   "c4" clob
);

insert into "testdata_clob" ("c2","c3","c4")  values(' rglmjrqpasdilemrccsi',' test DATA ',null);
insert into "testdata_clob" ("c1","c2","c3")  values('user /s log','"double Qua"','aåbäcö');
insert into "testdata_clob" ("c1","c2","c3","c4")  values('default char value ','rglmjrqpasdilemrccs','Référence','rfrwrdrulvblguogsilhgkhfqieuaxqshogcbbfxhhtddmizrrjywwkcxezybwarjehsohtujbzoqgbsodrcrlwejrweavtemjozzayhumpeghzrzrkcdrypgdepqfpuxycykvjdbxulbbgtivznwbvrgghdfiskszlmoedoelwqoeovtzugbherwncbrseirbgbfvkrqdysxqblubxsweuhboyuhlkzxgjrfkcmlouhshrckygxpepmpxddigdagzglcrlkdzgsaiiucbwuqevepvpejemhyuajxsswkhhqxmnozunshppeenvsztgbvcepjsterzjkywiqwhqcqgywzcmfldmftvydimddkipkbwktnebtyamxafysuvxyljussbbdmrxepkyktyloujicajnrsatbmspafnrtpgbapthpcynisyafgexiashcbjznetjazxguruniytqizbicppkkjociiwppeectzanrajlkwqvbrfouipomltgwvuxvrocmshfqudnorlprwvrabevwcoqtndkwvtlrshlsywkaiqcogwgshbhrtqyscpkmfhhntjrelsxnfeqnkhtpgwuixoklefjwfqwsodmvxyqilmvmyhzzphqbbnucxisugfyagxwjhmagylejsreinjqquolannskjeufnjmdydjeuxesrmbbwksqmlkbcqnmhmamdfasbpxaaaetobqfuedcmfnbwavtouhwmncufyvvrhsjmpifosqsxbltbypbjbdyqtigikgudslmtavxasjyphogydhcrzfcfkbvkbpelvpyvqstdhzjypbrzycxdpoafjwvcwezhvatgvksxkhcagdkrojrrhgoiamdmqcompqccqrskirelraniszpuaaabqjrbriyasdoubgobrnxxeuhqrsrjqrgnoljuttbrtrjssbsqmkipnlyakfzduadjdxyyjqrjrdhauffsfagfilazjkafxqcncbwwmuwoiynopoltwnqmrtsixmjtrtlprfvvcrlpprkgodvbfgetjummgzavbcfvuhdkolrnnwmlhthlrlarnwgfeoxiqdnjwxzrgiysdwsxrqbhzttculeelvxeieoioruouelehndwsyertaimobazhttzasfedzovauqnqebuiraldifcrabypenougycerhayjudkkitpjdicgxffgkrfhrsjklfgzjxefonekpzpgmnfmqninvrzatcdimxfclsubtyqvgsudmjvhfipytatifxdtsiwkewhkvioycmhdvkejajvvbktkzgdwgeuakkoylekcxysedqscpnoignokplrbgcbdwdptvffmaroosgdgooovcrzhrxwkrusqjjigmgbfdnwkymmzjdqjsseorrqrczlfztzcjvsrxefxewcazfsflmnikscmatftyffcqzdnnxbmrirhlrqrrlqyyvwnfcubvzrodkmyaxeiezcwblttkqrgkfbcqlgmlksasxvdtpntvxwfjuoqpvjyrwzonuqefhygltfenzsburyqyawchesundpcounstlsdsyzicxjrvowxnrxhlcedefzdqkewvjdcqgpydlqltmqqeehjkwqiymsuvjmjeowdltmkspkjhylfyzqmzazcyqimetulwhvxcfhdcnvkbtkvaayathhobrvzzqwntqtlpqkqgqdmghytgpriwuyobekezgsgiesrhkamuhoswwwxobydypgkavlyniodvxjbzsbzozthiihiunaneoyilfpffdtzrahvtjokmyyfofflfmjaemzsmtvioeunolifcadjzucakgdghlabqxrvqjtajgsljalmzijyxpfbkrljyrgahmluolujcsnpyrfqtmmeyujfteqtswecuiralasmtgdmlhiglpsfdqehwevjiqgvlcjvycwlssezpfxdtrbzzqxidxjolsoelnrdmwezwhxnlaeyxliknujlepakfthhbkcklsnoamqiqplnyxrmbtjaztixsqgtmtcgxrvzkrsnalwtjzumabxejuxqqephzffrltygczknsclxfmndjbyodvxyjkcpqwlqemdfxyunojbaicgdiouuwgibdzsuwrqxmhvwbikfcppmxhphvlxkoasyxedynmpsjriuttcjlqdojvutouzmeuuhnyuxohnolfvgnkoembtgndeoudifjbhjpnjnymdcqiyiqygqzltcidzkmnxyrguqlhltrsbypjtmrybbbtzwyscnfviawzlmibhupzeoreorngjcezlewtmoreoehlpzlythevdjzpmzrpwhvmaiibawczxglurzovbigbpephyjtlmzosrqgtoklltmcuwkfnlyjmpctyzmxghqcxjwrrejgnuumixuuuntiwrsuchjftcpdzcykkfxvmipzpwyzwawwfzwsiyrtkptgnnspoegcsn');
insert into "testdata_clob" ("c2","c3","c4")  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ğ¿ÈëÌÇ¹øÕ');
insert into "testdata_clob" ("c1","c2","c3","c4")  values('æ—¥æœ¬èª',' ','/?µ„?ö??f?İøÀÚ?t?ª®?Î???Ë???ğnø§˜?íèÏ??Õ"?Ë?»?âõ??','Bientôt l été');
insert into "testdata_clob" ("c1","c2","c3")  values('Bientôt l été',' ','ÖÛ«Âé®üçÚ¾±²Ìñ¿øŞ°¯õÁæ­ûÊîâïÃÕôûçµé«áÄô');

 DROP TABLE IF EXISTS  "testdata_national_char"  CASCADE;

CREATE TABLE "testdata_national_char"(
  "c1" national char (1000) DEFAULT 'Default varchar',
 "c2" national char (1000) NULL,
  "c3" national char (1000) NOT NULL,
   "c4" national char (1000)
);

insert into "testdata_national_char" ("c2","c3","c4")  values(' rglmjrqpasdilemrccsi ',' test DATA ',null);
insert into "testdata_national_char" ("c1","c2","c3")  values('user /s log','"DOUBLE Qua"','aåbäcö');
insert into "testdata_national_char" ("c1","c2","c3","c4")  values('default char value','rglmjrqpasdilemrccs','Référence','nuzopeaeekcgbpvkevukzqlemfdssuhqopmibkdfohhavdkxkcuylihpblqlddhmuwdlucjfkkqneiqtgpuvvjpdmqgztllrgckwogzbmhfkplldehwcshgvxappmlnwxjjavpsmwokbrioygaokhqnzibqisfpzefmflwffacytubchcwmfwdoeqnkejalndtdmbwckujyigmcbsehwccjdjfkymoiucnebzsqiiaasyxivzfkxkohrbhfwjkkajcxssyynakxzpfnjegstcxwqyvztkaqfputihmcxlmbhojrderazidmnrhvkgzefzwnhbmvwjpsuhibpwucmkyojbmlddspwfddqhmbemezfwcthmqezfuvvolvrqopujsetqgjpzdsbmdqxqvibqazlvhnrspcjesvlyqxcuscogdcefovvnrxjrjaurxfcgnbyrnpnyueudxikaisprtomgouoekowmjlcghsrvpbqjhsanqnifofecedksuncvvdlgphfubswmbvjluksjxmdppfolbnupxmrrqsaklhdtipvbyyykqbmiwgtdeehzdzascgdbktmgurqkqlxssghqgsfvyuigtmyfhqaszoscbipgutjbrpixcphgyqparbwhbqfvuiszuqogcahovvzijncxdsurvpfsgutjlclueiasowubtemthxcrqudxkusdtfxfmzxpeatbvjnqklgggdeyzwqaaxhtymymrghwljjjgonkrihvnughoscuhbfprfuannsobdxkfskxppzedzdyxavmxvcqtglptycureaxwcruzatxntyujzbwwbjbxejdnsqvuqayyzlwouoczsdonxwgwchwrujgupwtazxgqcvvjcseojiudaywvqlhiwqmvocxtfrogyvshikruoyhvrgmvoxjzrymciwtuyokoauwztxylzwirtkcxvhdruzvumgflrdorcltivwvbjtcmvzxpqktoxy');
insert into "testdata_national_char" ("c2","c3","c4")  values('user''s log','eexnwbmvvwuepqedqtspayhxqqmxxtdaofawqhqqzxtqeqszynwjuiuqfetuzgcovgpgtwdbufhnivcvnhvpwxdwckdbaxvcnvrwmyxaacekhmzpthtlsjqiramcpcdsujmfnnfbyjjspowlbkuwqwayqlrwstikymlgwvtjdhoabzccwbvpnsizbdjxgdsgaweqkjsnsxpwmrpsjmyvwizkzdpjkxzmybjkotgpqbzrgzjkvqcanspbch','¤¸ËÕ¨íÚ×È¬°Ğ¿ÈëÌÇ¹øÕ');
insert into "testdata_national_char" ("c1","c2","c3","c4")  values('æ—¥æœ¬èª',' ','/?µ„?ö??f?İøÀÚ?t?ª®?Î???Ë???ğnø§˜?íèÏ??Õ"?Ë?»?âõ??','Bientôt l été');
insert into "testdata_national_char" ("c1","c2","c3")  values('Bientôt l été',' ','ÖÛ«Âé®üçÚ¾±²Ìñ¿øŞ°¯õÁæ­ûÊîâïÃÕôûçµé«áÄô');

DROP TABLE IF EXISTS "testdata_blob" CASCADE;

CREATE TABLE "testdata_blob" (
  "c1" blob,
  "c2" blob NULL,
  "c3" blob NOT NULL,
  "c4" blob,
  "c5" blob
);

insert into  "testdata_blob" ("c2","c3","c4","c5") values('B','°í=¸ØXY7€¥·‹Ë¥§§itÜöç¢óÚšWkíöZ+S]','aåbäcö','æ—¥æœ¬èª');
insert into  "testdata_blob" ("c2","c3","c4","c5") values(null,'{ˆ¯göI½[deÍ2Ühf•ƒ¯‰Ä:iY³ø¢á¦”œÃ%vmvÖ¢','Bientôt l été','ÈÑÇÈÑ');
insert into "testdata_blob" ("c1","c2","c3")  values('æ—¥æœ¬èª',' ','/?µ„?ö??f?İøÀÚ?t?ª®?Î???Ë???ğnø§˜?íèÏ??Õ"?Ë?»?âõ??');
insert into  "testdata_blob" ("c2","c3","c4") values('æ—¥æœ¬èª','ÈÑÇÈÑ','Bientôt l été');


DROP TABLE IF EXISTS "testdata_binary" CASCADE;

CREATE TABLE "testdata_binary" (
  "c1" binary(1000),
  "c2" binary(1000) NULL,
  "c3" binary(1000) NOT NULL,
  "c4" binary(1000),
  "c5" binary(1000)
);

insert into  "testdata_binary" (c3) values('²E?Å` M~IOQ^YÇóO-O,7E>&j.*2U[Y^AOla,lar6AywuæoUt^\Ñ=^^<C^xôkW>¼¼8²^\~¶¼I^NkDE');
insert into  "testdata_binary" ("c2","c3","c4","c5") values(null,'2‰¯•Ïtï`ï‰ì–VI…3!x¥)"™!L‘&vÉV$&½Ÿ#4ğì','Bientôt l été','ÈÑÇÈÑ');
insert into "testdata_binary" ("c1","c2","c3")  values('0x646F67',' ','/?µ„?ö??f?İøÀÚ?t?ª®?Î???Ë???ğnø§˜?íèÏ??Õ"?Ë?»?âõ??');
insert into  "testdata_binary" ("c2","c3","c4") values('æ—¥æœ¬èª','ÈÑÇÈÑ','Bientôt l été');

DROP TABLE IF EXISTS "testdata_binary_varying" CASCADE;

CREATE TABLE "testdata_binary_varying" (
  "c1" binary varying (1000),
  "c2" binary varying (1000) NULL,
  "c3" binary varying (1000) NOT NULL,
  "c4" binary varying (1000),
  "c5" binary varying (1000)
);

insert into  "testdata_binary_varying" ("c2","c3","c4","c5") values('süvZdRUœ¤rèò?ÊBRÆl#y0èOˆ7GŠoŒâ®‹f½¹%µï›pöHq4M','check','aåbäcö','æ—¥æœ¬èª');
insert into  "testdata_binary_varying" ("c2","c3","c4","c5") values(null,'ÓÖ‹dîrcVÓ?%Ş.Ş³z.GÃíÛ¥âox^Ñ{t§NN_ó§³Ää/˜ÓQ¬ÕóœqUWm Ğ½˜X3Ê°O¡¥\¢?å','Bientôt l été','ÈÑÇÈÑ');
insert into "testdata_binary_varying" ("c1","c2","c3")  values('æ—¥æœ¬èª',' ','/?µ„?ö??f?İøÀÚ?t?ª®?Î???Ë???ğnø§˜?íèÏ??Õ"?Ë?»?âõ??');
insert into  "testdata_binary_varying" ("c3","c4") values('ÈÑÇÈÑ','Bientôt l été');

DROP TABLE IF EXISTS "testdata_boolean" CASCADE;

CREATE TABLE "testdata_boolean" (
  "c1" boolean NULL,
  "c2" boolean NOT NULL
);

insert into  "testdata_boolean" ("c1","c2") values(1,0);
insert into  "testdata_boolean" ("c1","c2") values(False,True);
insert into  "testdata_boolean" ("c2") values('True');


DROP TABLE IF EXISTS "testdata_enum" CASCADE;

CREATE TABLE "testdata_enum" (
 "c1" ENUM('abcd', 'in\out', 'sample test') DEFAULT 'abcd',
  "c2" ENUM(' xyz ', 'test', 'Référence') DEFAULT NULL,
  "c3" ENUM('car/s','byke"s','user''s log') DEFAULT NULL
);

insert into "testdata_enum" ("c1","c2","c3") values ('in\out','Référence','byke"s');
insert into "testdata_enum" ("c2","c3") values (' xyz ','car/s');
insert into "testdata_enum" ("c1","c3") values ('sample test','user''s log');
insert into "testdata_enum" ("c1","c2","c3") values ('abcd','test','byke"s');


DROP TABLE   IF EXISTS "testdata_date"  CASCADE;

CREATE TABLE "testdata_date"(
"c1" date,
"c2" date NULL,
"c3" date NOT NULL
);

INSERT INTO testdata_date ("c1","c3") VALUES('01-JAN-2014','29-APR-1879');
INSERT INTO testdata_date ("c2","c3")  VALUES('12/31/70','01/01/71');
INSERT INTO testdata_date ("c3") VALUES('Sep 08 2013');
INSERT INTO testdata_date ("c1","c3") VALUES('today','tomorrow');


DROP TABLE  IF EXISTS "testdata_timestamp" CASCADE;

CREATE TABLE "testdata_timestamp"(
"c1" timestamp,
"c2" timestamp NULL,
"c3" timestamp NOT NULL
);

INSERT INTO testdata_timestamp ("c1","c3") VALUES('09/08/2013 16:30:59.000005','2013.9.8 10:11:12.123456');
INSERT INTO testdata_timestamp ("c2","c3") VALUES('now','September 08, 2013 14:55');
INSERT INTO testdata_timestamp ("c2","c3") VALUES('08/Sep/2013 02:00 AM','Sept 8 14 14:00');
INSERT INTO testdata_timestamp ("c3") VALUES('September 8, 13 12:59');


DROP TABLE  IF EXISTS "testdata_time" CASCADE;

CREATE TABLE "testdata_time"(
"c1" time,
"c2" time NULL,
"c3" time NOT NULL
);

INSERT INTO testdata_time ("c1","c3") VALUES('16:30:59.000005','10:11:12.123456');
INSERT INTO testdata_time ("c2","c3") VALUES('now','02:00 AM');
INSERT INTO testdata_time ("c2","c3") VALUES('14:00','12:01:01');
INSERT INTO testdata_time ("c3") VALUES('4:3:2.003');
