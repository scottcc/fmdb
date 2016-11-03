/* main.m
 *
 * Sample code to illustrate some of the basic FMDB classes and run them through their paces for illustrative purposes.
 */

#import <Foundation/Foundation.h>
#import "FMDB.h"
#import <sqlite3.h>

#define FMDBQuickCheck(SomeBool) { if (!(SomeBool)) { NSLog(@"Failure on line %d", __LINE__); abort(); } }

void testPool(NSString *dbPath);
void testDateFormat();
void FMDBReportABugFunction();
void testStatementCaching();

int main (int argc, const char * argv[]) {
    
@autoreleasepool {
    
    FMDBReportABugFunction();
    
    NSString *dbPath = @"/tmp/tmp.db";
    
    // delete the old db.
    NSFileManager *fileManager = [NSFileManager defaultManager];
    [fileManager removeItemAtPath:dbPath error:nil];
    
    FMDBDatabase *db = [FMDBDatabase databaseWithPath:dbPath];
    
    NSLog(@"Is SQLite compiled with it's thread safe options turned on? %@!", [FMDBDatabase isSQLiteThreadSafe] ? @"Yes" : @"No");
    
    {
        // -------------------------------------------------------------------------------
        // Un-opened database check.
        FMDBQuickCheck([db executeQuery:@"select * from table"] == nil);
        NSLog(@"%d: %@", [db lastErrorCode], [db fmdb_lastErrorMessage]);
    }
    
    
    if (![db open]) {
        NSLog(@"Could not open db.");
        
        return 0;
    }
    
    // kind of experimentalish.
    [db setShouldCacheStatements:YES];
    
    // create a bad statement, just to test the error code.
    [db fmdb_executeUpdate:@"blah blah blah"];
    
    FMDBQuickCheck([db hadError]);
    
    if ([db hadError]) {
        NSLog(@"Err %d: %@", [db lastErrorCode], [db fmdb_lastErrorMessage]);
    }
    
    NSError *err = 0x00;
    FMDBQuickCheck(![db fmdb_executeUpdate:@"blah blah blah" withErrorAndBindings:&err]);
    FMDBQuickCheck(err != nil);
    FMDBQuickCheck([err code] == SQLITE_ERROR);
    NSLog(@"err: '%@'", err);
    
    
    
    // empty strings should still return a value.
    FMDBQuickCheck(([db boolForQuery:@"SELECT ? not null", @""]));
    
    // same with empty bits o' mutable data
    FMDBQuickCheck(([db boolForQuery:@"SELECT ? not null", [NSMutableData data]]));
    
    // same with empty bits o' data
    FMDBQuickCheck(([db boolForQuery:@"SELECT ? not null", [NSData data]]));

    
    
    // how do we do pragmas?  Like so:
    FMDBResultSet *ps = [db executeQuery:@"pragma journal_mode=delete"];
    FMDBQuickCheck(![db hadError]);
    FMDBQuickCheck(ps);
    FMDBQuickCheck([ps fmdb_next]);
    [ps close];
    
    // oh, but some pragmas require updates?
    [db fmdb_executeUpdate:@"pragma page_size=2048"];
    FMDBQuickCheck(![db hadError]);
    
    // what about a vacuum?
    [db fmdb_executeUpdate:@"vacuum"];
    FMDBQuickCheck(![db hadError]);
    
    // but of course, I don't bother checking the error codes below.
    // Bad programmer, no cookie.
    
    [db fmdb_executeUpdate:@"create table test (a text, b text, c integer, d double, e double)"];
    
    
    [db fmdb_beginTransaction];
    int i = 0;
    while (i++ < 20) {
        [db fmdb_executeUpdate:@"insert into test (a, b, c, d, e) values (?, ?, ?, ?, ?)" ,
            @"hi'", // look!  I put in a ', and I'm not escaping it!
            [NSString stringWithFormat:@"number %d", i],
            [NSNumber numberWithInt:i],
            [NSDate date],
            [NSNumber numberWithFloat:2.2f]];
    }
    [db commit];
    
    
    
    // do it again, just because
    [db fmdb_beginTransaction];
    i = 0;
    while (i++ < 20) {
        [db fmdb_executeUpdate:@"insert into test (a, b, c, d, e) values (?, ?, ?, ?, ?)" ,
         @"hi again'", // look!  I put in a ', and I'm not escaping it!
         [NSString stringWithFormat:@"number %d", i],
         [NSNumber numberWithInt:i],
         [NSDate date],
         [NSNumber numberWithFloat:2.2f]];
    }
    [db commit];
    
    
    
    
    
    FMDBResultSet *rs = [db executeQuery:@"select rowid,* from test where a = ?", @"hi'"];
    while ([rs fmdb_next]) {
        // just print out what we've got in a number of formats.
        NSLog(@"%d %@ %@ %@ %@ %f %f",
              [rs intForColumn:@"c"],
              [rs stringForColumn:@"b"],
              [rs stringForColumn:@"a"],
              [rs stringForColumn:@"rowid"],
              [rs dateForColumn:@"d"],
              [rs doubleForColumn:@"d"],
              [rs doubleForColumn:@"e"]);
        
        
        if (!([[rs columnNameForIndex:0] isEqualToString:@"rowid"] &&
              [[rs columnNameForIndex:1] isEqualToString:@"a"])
              ) {
            NSLog(@"WHOA THERE BUDDY, columnNameForIndex ISN'T WORKING!");
            return 7;
        }
    }
    // close the result set.
    // it'll also close when it's dealloc'd, but we're closing the database before
    // the autorelease pool closes, so sqlite will complain about it.
    [rs close];  
    
    FMDBQuickCheck(![db hasOpenResultSets]);
    
    
    
    rs = [db executeQuery:@"select rowid, a, b, c from test"];
    while ([rs fmdb_next]) {
        
        FMDBQuickCheck([rs[0] isEqual:rs[@"rowid"]]);
        FMDBQuickCheck([rs[1] isEqual:rs[@"a"]]);
        FMDBQuickCheck([rs[2] isEqual:rs[@"b"]]);
        FMDBQuickCheck([rs[3] isEqual:rs[@"c"]]);
    }
    [rs close];
    
    
    
    
    
    [db fmdb_executeUpdate:@"create table ull (a integer)"];
    
    [db fmdb_executeUpdate:@"insert into ull (a) values (?)" , [NSNumber numberWithUnsignedLongLong:ULLONG_MAX]];
    
    rs = [db executeQuery:@"select  a from ull"];
    while ([rs fmdb_next]) {
        unsigned long long a = [rs unsignedLongLongIntForColumnIndex:0];
        unsigned long long b = [rs unsignedLongLongIntForColumn:@"a"];
        
        FMDBQuickCheck(a == ULLONG_MAX);
        FMDBQuickCheck(b == ULLONG_MAX);
    }
    
    
    // check case sensitive result dictionary.
    [db fmdb_executeUpdate:@"create table cs (aRowName integer, bRowName text)"];
    FMDBQuickCheck(![db hadError]);
    [db fmdb_executeUpdate:@"insert into cs (aRowName, bRowName) values (?, ?)" , [NSNumber numberWithBool:1], @"hello"];
    FMDBQuickCheck(![db hadError]);
    
    rs = [db executeQuery:@"select * from cs"];
    while ([rs fmdb_next]) {
        NSDictionary *d = [rs resultDictionary];
        
        FMDBQuickCheck([d objectForKey:@"aRowName"]);
        FMDBQuickCheck(![d objectForKey:@"arowname"]);
        FMDBQuickCheck([d objectForKey:@"bRowName"]);
        FMDBQuickCheck(![d objectForKey:@"browname"]);
    }
    
    
    // check funky table names + getTableSchema
    [db fmdb_executeUpdate:@"create table '234 fds' (foo text)"];
    FMDBQuickCheck(![db hadError]);
    rs = [db getTableSchema:@"234 fds"];
    FMDBQuickCheck([rs fmdb_next]);
    [rs close];


#if SQLITE_VERSION_NUMBER >= 3007017
    {
        uint32_t appID = NSHFSTypeCodeFromFileType(NSFileTypeForHFSTypeCode('fmdb'));
        
        [db setApplicationID:appID];
        
        uint32_t rAppID = [db applicationID];
        
        NSLog(@"rAppID: %d", rAppID);
        
        FMDBQuickCheck(rAppID == appID);
        
        [db setApplicationIDString:@"acrn"];
        
        NSString *s = [db applicationIDString];
        
        NSLog(@"s: '%@'", s);
        
        FMDBQuickCheck([s isEqualToString:@"acrn"]);
        
    }
    
#endif


    {
        // -------------------------------------------------------------------------------
        // Named parameters count test.

        FMDBQuickCheck([db fmdb_executeUpdate:@"create table namedparamcounttest (a text, b text, c integer, d double)"]);
        NSMutableDictionary *dictionaryArgs = [NSMutableDictionary dictionary];
        [dictionaryArgs setObject:@"Text1" forKey:@"a"];
        [dictionaryArgs setObject:@"Text2" forKey:@"b"];
        [dictionaryArgs setObject:[NSNumber numberWithInt:1] forKey:@"c"];
        [dictionaryArgs setObject:[NSNumber numberWithDouble:2.0] forKey:@"d"];
        FMDBQuickCheck([db fmdb_executeUpdate:@"insert into namedparamcounttest values (:a, :b, :c, :d)" withParameterDictionary:dictionaryArgs]);
        
        rs = [db executeQuery:@"select * from namedparamcounttest"];
        
        FMDBQuickCheck((rs != nil));
        
        [rs fmdb_next];
        
        FMDBQuickCheck([[rs stringForColumn:@"a"] isEqualToString:@"Text1"]);
        FMDBQuickCheck([[rs stringForColumn:@"b"] isEqualToString:@"Text2"]);
        FMDBQuickCheck([rs intForColumn:@"c"] == 1);
        FMDBQuickCheck([rs doubleForColumn:@"d"] == 2.0);
        
        [rs close];
        
        // note that at this point, dictionaryArgs has way more values than we need, but the query should still work since
        // a is in there, and that's all we need.
        rs = [db executeQuery:@"select * from namedparamcounttest where a = :a" withParameterDictionary:dictionaryArgs];
        
        FMDBQuickCheck((rs != nil));
        FMDBQuickCheck([rs fmdb_next]);
        [rs close];
        
        
        
        // ***** Please note the following codes *****
        
        dictionaryArgs = [NSMutableDictionary dictionary];
        
        [dictionaryArgs setObject:@"NewText1" forKey:@"a"];
        [dictionaryArgs setObject:@"NewText2" forKey:@"b"];
        [dictionaryArgs setObject:@"OneMoreText" forKey:@"OneMore"];
        
        BOOL rc = [db fmdb_executeUpdate:@"update namedparamcounttest set a = :a, b = :b where b = 'Text2'" withParameterDictionary:dictionaryArgs];
        
        FMDBQuickCheck(rc);
        
        if (!rc) {
            NSLog(@"ERROR: %d - %@", db.lastErrorCode, db.fmdb_lastErrorMessage);
        }
    
        
    }
    
    
    
    
    
    
    
    
    
    
    // ----------------------------------------------------------------------------------------
    // blob support.
    [db fmdb_executeUpdate:@"create table blobTable (a text, b blob)"];
    
    // let's read in an image from safari's app bundle.
    NSData *safariCompass = [NSData dataWithContentsOfFile:@"/Applications/Safari.app/Contents/Resources/compass.icns"];
    if (safariCompass) {
        [db fmdb_executeUpdate:@"insert into blobTable (a, b) values (?,?)", @"safari's compass", safariCompass];
        
        rs = [db executeQuery:@"select b from blobTable where a = ?", @"safari's compass"];
        if ([rs fmdb_next]) {
            safariCompass = [rs dataForColumn:@"b"];
            [safariCompass writeToFile:@"/tmp/compass.icns" atomically:NO];
            
            // let's look at our fancy image that we just wrote out..
            system("/usr/bin/open /tmp/compass.icns");
            
            // ye shall read the header for this function, or suffer the consequences.
            safariCompass = [rs dataNoCopyForColumn:@"b"];
            [safariCompass writeToFile:@"/tmp/compass_data_no_copy.icns" atomically:NO];
            system("/usr/bin/open /tmp/compass_data_no_copy.icns");
        }
        else {
            NSLog(@"Could not select image.");
        }
        
        [rs close];
        
    }
    else {
        NSLog(@"Can't find compass image..");
    }
    
    
    // test out the convenience methods in +Additions
    [db fmdb_executeUpdate:@"create table t1 (a integer)"];
    [db fmdb_executeUpdate:@"insert into t1 values (?)", [NSNumber numberWithInt:5]];
    
    NSLog(@"Count of changes (should be 1): %d", [db changes]);
    FMDBQuickCheck([db changes] == 1);
    
    int ia = [db intForQuery:@"select a from t1 where a = ?", [NSNumber numberWithInt:5]];
    if (ia != 5) {
        NSLog(@"intForQuery didn't work (a != 5)");
    }
    
    // test the busy rety timeout schtuff.
    
    [db setMaxBusyRetryTimeInterval:5];
    
    FMDBDatabase *newDb = [FMDBDatabase databaseWithPath:dbPath];
    [newDb open];
    
    rs = [newDb executeQuery:@"select rowid,* from test where a = ?", @"hi'"];
    [rs fmdb_next]; // just grab one... which will keep the db locked.
    
    NSLog(@"Testing the busy timeout");
    
    NSTimeInterval startTime = [NSDate timeIntervalSinceReferenceDate];
    
    BOOL success = [db fmdb_executeUpdate:@"insert into t1 values (5)"];
    
    if (success) {
        NSLog(@"Whoa- the database didn't stay locked!");
        return 7;
    }
    else {
        NSLog(@"Hurray, our timeout worked");
    }
    
    [rs close];
    [newDb close];
    
    success = [db fmdb_executeUpdate:@"insert into t1 values (5)"];
    if (!success) {
        NSLog(@"Whoa- the database shouldn't be locked!");
        return 8;
    }
    else {
        NSLog(@"Hurray, we can insert again!");
    }
    
    NSTimeInterval end = [NSDate timeIntervalSinceReferenceDate] - startTime;
    
    NSLog(@"Took %f seconds for the timeout.", end);
    
    // test some nullness.
    [db fmdb_executeUpdate:@"create table t2 (a integer, b integer)"];
    
    if (![db fmdb_executeUpdate:@"insert into t2 values (?, ?)", nil, [NSNumber numberWithInt:5]]) {
        NSLog(@"UH OH, can't insert a nil value for some reason...");
    }
    
    
    
    
    rs = [db executeQuery:@"select * from t2"];
    while ([rs fmdb_next]) {
        NSString *aa = [rs fmdb_stringForColumnIndex:0];
        NSString *b = [rs fmdb_stringForColumnIndex:1];
        
        if (aa != nil) {
            NSLog(@"%s:%d", __FUNCTION__, __LINE__);
            NSLog(@"OH OH, PROBLEMO!");
            return 10;
        }
        else {
            NSLog(@"YAY, NULL VALUES");
        }
        
        if (![b isEqualToString:@"5"]) {
            NSLog(@"%s:%d", __FUNCTION__, __LINE__);
            NSLog(@"OH OH, PROBLEMO!");
            return 10;
        }
    }
    
    
    
    
    
    
    
    
    
    
    // test some inner loop funkness.
    [db fmdb_executeUpdate:@"create table t3 (a somevalue)"];
    
    
    // do it again, just because
    [db fmdb_beginTransaction];
    i = 0;
    while (i++ < 20) {
        [db fmdb_executeUpdate:@"insert into t3 (a) values (?)" , [NSNumber numberWithInt:i]];
    }
    [db commit];
    
    
    
    
    rs = [db executeQuery:@"select * from t3"];
    while ([rs fmdb_next]) {
        int foo = [rs fmdb_intForColumnIndex:0];
        
        int newVal = foo + 100;
        
        [db fmdb_executeUpdate:@"update t3 set a = ? where a = ?" , [NSNumber numberWithInt:newVal], [NSNumber numberWithInt:foo]];
        
        
        FMDBResultSet *rs2 = [db executeQuery:@"select a from t3 where a = ?", [NSNumber numberWithInt:newVal]];
        [rs2 fmdb_next];
        
        if ([rs2 fmdb_intForColumnIndex:0] != newVal) {
            NSLog(@"Oh crap, our update didn't work out!");
            return 9;
        }
        
        [rs2 close];
    }
    
    
    // NSNull tests
    [db fmdb_executeUpdate:@"create table nulltest (a text, b text)"];
    
    [db fmdb_executeUpdate:@"insert into nulltest (a, b) values (?, ?)" , [NSNull null], @"a"];
    [db fmdb_executeUpdate:@"insert into nulltest (a, b) values (?, ?)" , nil, @"b"];
    
    rs = [db executeQuery:@"select * from nulltest"];
    
    while ([rs fmdb_next]) {
        
        NSString *a = [rs fmdb_stringForColumnIndex:0];
        NSString *b = [rs fmdb_stringForColumnIndex:1];
        
        if (!b) {
            NSLog(@"Oh crap, the nil / null inserts didn't work!");
            return 10;
        }
        
        if (a) {
            NSLog(@"Oh crap, the nil / null inserts didn't work (son of error message)!");
            return 11;
        }
        else {
            NSLog(@"HURRAH FOR NSNULL (and nil)!");
        }
    }
    
    
    FMDBQuickCheck([db columnExists:@"a" inTableWithName:@"nulltest"]);
    FMDBQuickCheck([db columnExists:@"b" inTableWithName:@"nulltest"]);
    FMDBQuickCheck(![db columnExists:@"c" inTableWithName:@"nulltest"]);
    
    
    // null dates
    
    NSDate *date = [NSDate date];
    [db fmdb_executeUpdate:@"create table datetest (a double, b double, c double)"];
    [db fmdb_executeUpdate:@"insert into datetest (a, b, c) values (?, ?, 0)" , [NSNull null], date];
    
    rs = [db executeQuery:@"select * from datetest"];
    
    while ([rs fmdb_next]) {
        
        NSDate *a = [rs dateForColumnIndex:0];
        NSDate *b = [rs dateForColumnIndex:1];
        NSDate *c = [rs dateForColumnIndex:2];
        
        if (a) {
            NSLog(@"Oh crap, the null date insert didn't work!");
            return 12;
        }
        
        if (!c) {
            NSLog(@"Oh crap, the 0 date insert didn't work!");
            return 12;
        }
        
        NSTimeInterval dti = fabs([b timeIntervalSinceDate:date]);
        
        if (floor(dti) > 0.0) {
            NSLog(@"Date matches didn't really happen... time difference of %f", dti);
            return 13;
        }
        
        
        dti = fabs([c timeIntervalSinceDate:[NSDate dateWithTimeIntervalSince1970:0]]);
        
        if (floor(dti) > 0.0) {
            NSLog(@"Date matches didn't really happen... time difference of %f", dti);
            return 13;
        }
    }
    
    NSDate *foo = [db dateForQuery:@"select b from datetest where c = 0"];
    assert(foo);
    NSTimeInterval dti = fabs([foo timeIntervalSinceDate:date]);
    if (floor(dti) > 0.0) {
        NSLog(@"Date matches didn't really happen... time difference of %f", dti);
        return 14;
    }
    
    [db fmdb_executeUpdate:@"create table nulltest2 (s text, d data, i integer, f double, b integer)"];
    
    // grab the data for this again, since we overwrote it with some memory that has since disapeared.
    safariCompass = [NSData dataWithContentsOfFile:@"/Applications/Safari.app/Contents/Resources/compass.icns"];
    
    [db fmdb_executeUpdate:@"insert into nulltest2 (s, d, i, f, b) values (?, ?, ?, ?, ?)" , @"Hi", safariCompass, [NSNumber numberWithInt:12], [NSNumber numberWithFloat:4.4f], [NSNumber numberWithBool:YES]];
    [db fmdb_executeUpdate:@"insert into nulltest2 (s, d, i, f, b) values (?, ?, ?, ?, ?)" , nil, nil, nil, nil, [NSNull null]];
    
    rs = [db executeQuery:@"select * from nulltest2"];
    
    while ([rs fmdb_next]) {
        
        i = [rs fmdb_intForColumnIndex:2];
        
        if (i == 12) {
            // it's the first row we inserted.
            FMDBQuickCheck(![rs columnIndexIsNull:0]);
            FMDBQuickCheck(![rs columnIndexIsNull:1]);
            FMDBQuickCheck(![rs columnIndexIsNull:2]);
            FMDBQuickCheck(![rs columnIndexIsNull:3]);
            FMDBQuickCheck(![rs columnIndexIsNull:4]);
            FMDBQuickCheck( [rs columnIndexIsNull:5]);
            
            FMDBQuickCheck([[rs dataForColumn:@"d"] length] == [safariCompass length]);
            FMDBQuickCheck(![rs dataForColumn:@"notthere"]);
            FMDBQuickCheck(![rs fmdb_stringForColumnIndex:-2]);
            FMDBQuickCheck([rs boolForColumnIndex:4]);
            FMDBQuickCheck([rs boolForColumn:@"b"]);
            
            FMDBQuickCheck(fabs(4.4 - [rs doubleForColumn:@"f"]) < 0.0000001);
            
            FMDBQuickCheck(12 == [rs intForColumn:@"i"]);
            FMDBQuickCheck(12 == [rs fmdb_intForColumnIndex:2]);
            
            FMDBQuickCheck(0 == [rs fmdb_intForColumnIndex:12]); // there is no 12
            FMDBQuickCheck(0 == [rs intForColumn:@"notthere"]);
            
            FMDBQuickCheck(12 == [rs longForColumn:@"i"]);
            FMDBQuickCheck(12 == [rs longLongIntForColumn:@"i"]);
        }
        else {
            // let's test various null things.
            
            FMDBQuickCheck([rs columnIndexIsNull:0]);
            FMDBQuickCheck([rs columnIndexIsNull:1]);
            FMDBQuickCheck([rs columnIndexIsNull:2]);
            FMDBQuickCheck([rs columnIndexIsNull:3]);
            FMDBQuickCheck([rs columnIndexIsNull:4]);
            FMDBQuickCheck([rs columnIndexIsNull:5]);
            
            
            FMDBQuickCheck(![rs dataForColumn:@"d"]);
            
        }
    }
    
    
    
    {
        
        [db fmdb_executeUpdate:@"create table utest (a text)"];
        [db fmdb_executeUpdate:@"insert into utest values (?)", @"/übertest"];
        
        rs = [db executeQuery:@"select * from utest where a = ?", @"/übertest"];
        FMDBQuickCheck([rs fmdb_next]);
        [rs close];
    }   
    
    
    {
        [db fmdb_executeUpdate:@"create table testOneHundredTwelvePointTwo (a text, b integer)"];
        [db fmdb_executeUpdate:@"insert into testOneHundredTwelvePointTwo values (?, ?)" withArgumentsInArray:[NSArray arrayWithObjects:@"one", [NSNumber numberWithInteger:2], nil]];
        [db fmdb_executeUpdate:@"insert into testOneHundredTwelvePointTwo values (?, ?)" withArgumentsInArray:[NSArray arrayWithObjects:@"one", [NSNumber numberWithInteger:3], nil]];
        
        
        rs = [db executeQuery:@"select * from testOneHundredTwelvePointTwo where b > ?" withArgumentsInArray:[NSArray arrayWithObject:[NSNumber numberWithInteger:1]]];
        
        FMDBQuickCheck([rs fmdb_next]);
        
        FMDBQuickCheck([rs hasAnotherRow]);
        FMDBQuickCheck(![db hadError]);
        
        FMDBQuickCheck([[rs fmdb_stringForColumnIndex:0] isEqualToString:@"one"]);
        FMDBQuickCheck([rs fmdb_intForColumnIndex:1] == 2);
        
        FMDBQuickCheck([rs fmdb_next]);
        
        FMDBQuickCheck([rs fmdb_intForColumnIndex:1] == 3);
        
        FMDBQuickCheck(![rs fmdb_next]);
        FMDBQuickCheck(![rs hasAnotherRow]);
        
    }
    
    {
        
        FMDBQuickCheck([db fmdb_executeUpdate:@"create table t4 (a text, b text)"]);
        FMDBQuickCheck(([db fmdb_executeUpdate:@"insert into t4 (a, b) values (?, ?)", @"one", @"two"]));
        
        rs = [db executeQuery:@"select t4.a as 't4.a', t4.b from t4;"];
        
        FMDBQuickCheck((rs != nil));
        
        [rs fmdb_next];
        
        FMDBQuickCheck([[rs stringForColumn:@"t4.a"] isEqualToString:@"one"]);
        FMDBQuickCheck([[rs stringForColumn:@"b"] isEqualToString:@"two"]);
        
        FMDBQuickCheck(strcmp((const char*)[rs UTF8StringForColumnName:@"b"], "two") == 0);
        
        [rs close];
        
        // let's try these again, with the withArgumentsInArray: variation
        FMDBQuickCheck([db fmdb_executeUpdate:@"drop table t4;" withArgumentsInArray:[NSArray array]]);
        FMDBQuickCheck([db fmdb_executeUpdate:@"create table t4 (a text, b text)" withArgumentsInArray:[NSArray array]]);
        FMDBQuickCheck(([db fmdb_executeUpdate:@"insert into t4 (a, b) values (?, ?)" withArgumentsInArray:[NSArray arrayWithObjects:@"one", @"two", nil]]));
        
        rs = [db executeQuery:@"select t4.a as 't4.a', t4.b from t4;" withArgumentsInArray:[NSArray array]];
        
        FMDBQuickCheck((rs != nil));
        
        [rs fmdb_next];
        
        FMDBQuickCheck([[rs stringForColumn:@"t4.a"] isEqualToString:@"one"]);
        FMDBQuickCheck([[rs stringForColumn:@"b"] isEqualToString:@"two"]);
        
        FMDBQuickCheck(strcmp((const char*)[rs UTF8StringForColumnName:@"b"], "two") == 0);
        
        [rs close];
    }
    
    
    
    
    {
        FMDBQuickCheck([db tableExists:@"t4"]);
        FMDBQuickCheck(![db tableExists:@"thisdoesntexist"]);
        
        rs = [db getSchema];
        while ([rs fmdb_next]) {
            FMDBQuickCheck([[rs stringForColumn:@"type"] isEqualToString:@"table"]);
        }
    }
    
    
    {
        FMDBQuickCheck([db fmdb_executeUpdate:@"create table t5 (a text, b int, c blob, d text, e text)"]);
        FMDBQuickCheck(([db executeUpdateWithFormat:@"insert into t5 values (%s, %d, %@, %c, %lld)", "text", 42, @"BLOB", 'd', 12345678901234ll]));
        
        rs = [db executeQueryWithFormat:@"select * from t5 where a = %s and a = %@ and b = %d", "text", @"text", 42];
        FMDBQuickCheck((rs != nil));
        
        [rs fmdb_next];
        
        FMDBQuickCheck([[rs stringForColumn:@"a"] isEqualToString:@"text"]);
        FMDBQuickCheck(([rs intForColumn:@"b"] == 42));
        FMDBQuickCheck([[rs stringForColumn:@"c"] isEqualToString:@"BLOB"]);
        FMDBQuickCheck([[rs stringForColumn:@"d"] isEqualToString:@"d"]);
        FMDBQuickCheck(([rs longLongIntForColumn:@"e"] == 12345678901234));
        
        [rs close];
    }
    
    
    
    {
        FMDBQuickCheck([db fmdb_executeUpdate:@"create table t55 (a text, b int, c float)"]);
        short testShort = -4;
        float testFloat = 5.5;
        FMDBQuickCheck(([db executeUpdateWithFormat:@"insert into t55 values (%c, %hi, %g)", 'a', testShort, testFloat]));
        
        unsigned short testUShort = 6;
        FMDBQuickCheck(([db executeUpdateWithFormat:@"insert into t55 values (%c, %hu, %g)", 'a', testUShort, testFloat]));
        
        
        rs = [db executeQueryWithFormat:@"select * from t55 where a = %s order by 2", "a"];
        FMDBQuickCheck((rs != nil));
        
        [rs fmdb_next];
        
        FMDBQuickCheck([[rs stringForColumn:@"a"] isEqualToString:@"a"]);
        FMDBQuickCheck(([rs intForColumn:@"b"] == -4));
        FMDBQuickCheck([[rs stringForColumn:@"c"] isEqualToString:@"5.5"]);
        
        
        [rs fmdb_next];
        
        FMDBQuickCheck([[rs stringForColumn:@"a"] isEqualToString:@"a"]);
        FMDBQuickCheck(([rs intForColumn:@"b"] == 6));
        FMDBQuickCheck([[rs stringForColumn:@"c"] isEqualToString:@"5.5"]);
        
        [rs close];
        
    }
    
    {
        FMDBQuickCheck([db fmdb_executeUpdate:@"create table tatwhat (a text)"]);
        
        BOOL worked = [db executeUpdateWithFormat:@"insert into tatwhat values(%@)", nil];
        
        FMDBQuickCheck(worked);
        
        rs = [db executeQueryWithFormat:@"select * from tatwhat"];
        FMDBQuickCheck((rs != nil));
        FMDBQuickCheck(([rs fmdb_next]));
        FMDBQuickCheck([rs columnIndexIsNull:0]);
        
        FMDBQuickCheck((![rs fmdb_next]));
        
    }
    
    
    {
        FMDBQuickCheck(([db fmdb_executeUpdate:@"insert into t5 values (?, ?, ?, ?, ?)" withErrorAndBindings:&err, @"text", [NSNumber numberWithInt:42], @"BLOB", @"d", [NSNumber numberWithInt:0]]));
        
    }
    
    {
        rs = [db executeQuery:@"select * from t5 where a=?" withArgumentsInArray:@[]];
        FMDBQuickCheck((![rs fmdb_next]));
    }
    
    // test attach for the heck of it.
    {
        
        //FMDBDatabase *dbA = [FMDBDatabase databaseWithPath:dbPath];
        [fileManager removeItemAtPath:@"/tmp/attachme.db" error:nil];
        FMDBDatabase *dbB = [FMDBDatabase databaseWithPath:@"/tmp/attachme.db"];
        FMDBQuickCheck([dbB open]);
        FMDBQuickCheck([dbB fmdb_executeUpdate:@"create table attached (a text)"]);
        FMDBQuickCheck(([dbB fmdb_executeUpdate:@"insert into attached values (?)", @"test"]));
        FMDBQuickCheck([dbB close]);
        
        [db fmdb_executeUpdate:@"attach database '/tmp/attachme.db' as attack"];
        
        rs = [db executeQuery:@"select * from attack.attached"];
        FMDBQuickCheck([rs fmdb_next]);
        [rs close];
        
    }
    
    
    
    {
        // -------------------------------------------------------------------------------
        // Named parameters.
        FMDBQuickCheck([db fmdb_executeUpdate:@"create table namedparamtest (a text, b text, c integer, d double)"]);
        NSMutableDictionary *dictionaryArgs = [NSMutableDictionary dictionary];
        [dictionaryArgs setObject:@"Text1" forKey:@"a"];
        [dictionaryArgs setObject:@"Text2" forKey:@"b"];
        [dictionaryArgs setObject:[NSNumber numberWithInt:1] forKey:@"c"];
        [dictionaryArgs setObject:[NSNumber numberWithDouble:2.0] forKey:@"d"];
        FMDBQuickCheck([db fmdb_executeUpdate:@"insert into namedparamtest values (:a, :b, :c, :d)" withParameterDictionary:dictionaryArgs]);
        
        rs = [db executeQuery:@"select * from namedparamtest"];
        
        FMDBQuickCheck((rs != nil));
        
        [rs fmdb_next];
        
        FMDBQuickCheck([[rs stringForColumn:@"a"] isEqualToString:@"Text1"]);
        FMDBQuickCheck([[rs stringForColumn:@"b"] isEqualToString:@"Text2"]);
        FMDBQuickCheck([rs intForColumn:@"c"] == 1);
        FMDBQuickCheck([rs doubleForColumn:@"d"] == 2.0);
        
        [rs close];
        
        
        dictionaryArgs = [NSMutableDictionary dictionary];
        
        [dictionaryArgs setObject:@"Text2" forKey:@"blah"];
        
        rs = [db executeQuery:@"select * from namedparamtest where b = :blah" withParameterDictionary:dictionaryArgs];
        
        FMDBQuickCheck((rs != nil));
        FMDBQuickCheck([rs fmdb_next]);
        FMDBQuickCheck([[rs stringForColumn:@"b"] isEqualToString:@"Text2"]);
        
        [rs close];
        
        
        
        
    }
    
    // just for fun.
    rs = [db executeQuery:@"pragma database_list"];
    while ([rs fmdb_next]) {
        NSString *file = [rs stringForColumn:@"file"];
        NSLog(@"database_list: %@", file);
    }
    
    
    // print out some stats if we are using cached statements.
    if ([db shouldCacheStatements]) {
        
        NSEnumerator *e = [[db cachedStatements] objectEnumerator];;
        FMStatement *statement;
        
        while ((statement = [e nextObject])) {
            NSLog(@"%@", statement);
        }
    }
    
    
    [db setShouldCacheStatements:true];
    
    [db fmdb_executeUpdate:@"CREATE TABLE testCacheStatements(key INTEGER PRIMARY KEY, value INTEGER)"];
    [db fmdb_executeUpdate:@"INSERT INTO testCacheStatements (key, value) VALUES (1, 2)"];
    [db fmdb_executeUpdate:@"INSERT INTO testCacheStatements (key, value) VALUES (2, 4)"];
    
    FMDBQuickCheck([[db executeQuery:@"SELECT * FROM testCacheStatements WHERE key=1"] fmdb_next]);
    FMDBQuickCheck([[db executeQuery:@"SELECT * FROM testCacheStatements WHERE key=1"] fmdb_next]);
    
    [db close];
    
    
    testPool(dbPath);
    testDateFormat();
    
    
    
    FMDBDatabaseQueue *queue = [FMDBDatabaseQueue databaseQueueWithPath:dbPath];
    
    FMDBQuickCheck(queue);
    
    {
        [queue inDatabase:^(FMDBDatabase *adb) {
            
            
            
            [adb fmdb_executeUpdate:@"create table qfoo (foo text)"];
            [adb fmdb_executeUpdate:@"insert into qfoo values ('hi')"];
            [adb fmdb_executeUpdate:@"insert into qfoo values ('hello')"];
            [adb fmdb_executeUpdate:@"insert into qfoo values ('not')"];
            
            
            
            int count = 0;
            FMDBResultSet *rsl = [adb executeQuery:@"select * from qfoo where foo like 'h%'"];
            while ([rsl fmdb_next]) {
                count++;
            }
            
            FMDBQuickCheck(count == 2);
            
            count = 0;
            rsl = [adb executeQuery:@"select * from qfoo where foo like ?", @"h%"];
            while ([rsl fmdb_next]) {
                count++;
            }
            
            FMDBQuickCheck(count == 2);
        }];
        
    }
    
    FMDBDatabaseQueue *queue2 = [FMDBDatabaseQueue databaseQueueWithPath:dbPath flags:SQLITE_OPEN_READONLY];
    
    FMDBQuickCheck(queue2);
    {
        [queue2 inDatabase:^(FMDBDatabase *db2) {
            FMDBResultSet *rs1 = [db2 executeQuery:@"SELECT * FROM test"];
            FMDBQuickCheck(rs1 != nil);
            [rs1 close];
            
            BOOL ok = [db2 fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:3]];
            FMDBQuickCheck(!ok);
        }];
        
        [queue2 close];
        
        [queue2 inDatabase:^(FMDBDatabase *db2) {
            FMDBResultSet *rs1 = [db2 executeQuery:@"SELECT * FROM test"];
            FMDBQuickCheck(rs1 != nil);
            [rs1 close];
            
            BOOL ok = [db2 fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:3]];
            FMDBQuickCheck(!ok);
        }];
    }
    
    {
        // You should see pairs of numbers show up in stdout for this stuff:
        size_t ops = 16;
        
        dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
        
        dispatch_apply(ops, dqueue, ^(size_t nby) {
            
            // just mix things up a bit for demonstration purposes.
            if (nby % 2 == 1) {
                [NSThread sleepForTimeInterval:.1];
                
                [queue inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
                    NSLog(@"Starting query  %ld", nby);
                    
                    FMDBResultSet *rsl = [adb executeQuery:@"select * from qfoo where foo like 'h%'"];
                    while ([rsl fmdb_next]) {
                        ;// whatever.
                    }
                    
                    NSLog(@"Ending query    %ld", nby);
                }];
                
            }
            
            if (nby % 3 == 1) {
                [NSThread sleepForTimeInterval:.1];
            }
            
            [queue inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
                NSLog(@"Starting update %ld", nby);
                [adb fmdb_executeUpdate:@"insert into qfoo values ('1')"];
                [adb fmdb_executeUpdate:@"insert into qfoo values ('2')"];
                [adb fmdb_executeUpdate:@"insert into qfoo values ('3')"];
                NSLog(@"Ending update   %ld", nby);
            }];
        });
        
        [queue close];
        
        [queue inDatabase:^(FMDBDatabase *adb) {
            FMDBQuickCheck([adb fmdb_executeUpdate:@"insert into qfoo values ('1')"]);
        }];
    }
    
    {
        
        
        [queue inDatabase:^(FMDBDatabase *adb) {
            [adb fmdb_executeUpdate:@"create table colNameTest (a, b, c, d)"];
            FMDBQuickCheck([adb fmdb_executeUpdate:@"insert into colNameTest values (1, 2, 3, 4)"]);
            
            FMDBResultSet *ars = [adb executeQuery:@"select * from colNameTest"];
            
            NSDictionary *d = [ars columnNameToIndexMap];
            FMDBQuickCheck([d count] == 4);
            
            FMDBQuickCheck([[d objectForKey:@"a"] intValue] == 0);
            FMDBQuickCheck([[d objectForKey:@"b"] intValue] == 1);
            FMDBQuickCheck([[d objectForKey:@"c"] intValue] == 2);
            FMDBQuickCheck([[d objectForKey:@"d"] intValue] == 3);
            
            [ars close];
            
        }];
        
    }
    
    
    {
        [queue inDatabase:^(FMDBDatabase *adb) {
            [adb fmdb_executeUpdate:@"create table transtest (a integer)"];
            FMDBQuickCheck([adb fmdb_executeUpdate:@"insert into transtest values (1)"]);
            FMDBQuickCheck([adb fmdb_executeUpdate:@"insert into transtest values (2)"]);
            
            int rowCount = 0;
            FMDBResultSet *ars = [adb executeQuery:@"select * from transtest"];
            while ([ars fmdb_next]) {
                rowCount++;
            }
            
            FMDBQuickCheck(rowCount == 2);
        }];
        
        
        
        [queue inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
            FMDBQuickCheck([adb fmdb_executeUpdate:@"insert into transtest values (3)"]);
            
            if (YES) {
                // uh oh!, something went wrong (not really, this is just a test
                *rollback = YES;
                return;
            }
            
            FMDBQuickCheck([adb fmdb_executeUpdate:@"insert into transtest values (4)"]);
        }];
        
        [queue inDatabase:^(FMDBDatabase *adb) {
        
            int rowCount = 0;
            FMDBResultSet *ars = [adb executeQuery:@"select * from transtest"];
            while ([ars fmdb_next]) {
                rowCount++;
            }
            
            FMDBQuickCheck(![adb hasOpenResultSets]);
            
            NSLog(@"after rollback, rowCount is %d (should be 2)", rowCount);
            
            FMDBQuickCheck(rowCount == 2);
        }];
    }
    
    // hey, let's make a custom function!
    
    [queue inDatabase:^(FMDBDatabase *adb) {
        
        [adb fmdb_executeUpdate:@"create table ftest (foo text)"];
        [adb fmdb_executeUpdate:@"insert into ftest values ('hello')"];
        [adb fmdb_executeUpdate:@"insert into ftest values ('hi')"];
        [adb fmdb_executeUpdate:@"insert into ftest values ('not h!')"];
        [adb fmdb_executeUpdate:@"insert into ftest values ('definitely not h!')"];
        
        [adb makeFunctionNamed:@"StringStartsWithH" maximumArguments:1 withBlock:^(/*sqlite3_context*/ void *context, int aargc, /*sqlite3_value*/ void **aargv) {
            if (sqlite3_value_type(aargv[0]) == SQLITE_TEXT) {
                
                @autoreleasepool {
                    
                    const char *c = (const char *)sqlite3_value_text(aargv[0]);
                    
                    NSString *s = [NSString stringWithUTF8String:c];
                    
                    sqlite3_result_int(context, [s hasPrefix:@"h"]);
                }
            }
            else {
                NSLog(@"Unknown formart for StringStartsWithH (%d) %s:%d", sqlite3_value_type(aargv[0]), __FUNCTION__, __LINE__);
                sqlite3_result_null(context);
            }
        }];
        
        int rowCount = 0;
        FMDBResultSet *ars = [adb executeQuery:@"select * from ftest where StringStartsWithH(foo)"];
        while ([ars fmdb_next]) {
            rowCount++;
            
            NSLog(@"Does %@ start with 'h'?", [rs fmdb_stringForColumnIndex:0]);
            
        }
        FMDBQuickCheck(rowCount == 2);
        
        testStatementCaching();
        
    }];
    
    NSLog(@"That was version %@ of sqlite", [FMDBDatabase sqliteLibVersion]);
    
    
}// this is the end of our @autorelease pool.
    
    
    return 0;
}
/*
 Test statement caching
 This test checks the fixes that address https://github.com/ccgus/fmdb/issues/6
 */

void testStatementCaching() {
    
    FMDBDatabase *db = [FMDBDatabase databaseWithPath:nil]; // use in-memory DB
    [db open];
    
    [db fmdb_executeUpdate:@"DROP TABLE IF EXISTS testStatementCaching"];
    [db fmdb_executeUpdate:@"CREATE TABLE testStatementCaching ( value INTEGER )"];
    [db fmdb_executeUpdate:@"INSERT INTO testStatementCaching( value ) VALUES (1)"];
    [db fmdb_executeUpdate:@"INSERT INTO testStatementCaching( value ) VALUES (1)"];
    [db fmdb_executeUpdate:@"INSERT INTO testStatementCaching( value ) VALUES (2)"];
    
    [db setShouldCacheStatements:YES];
    
    // two iterations.
    //  the first time through no statements will be from the cache.
    //  the second time through all statements come from the cache.
    for (int i = 1; i <= 2; i++ ) {
        
        FMDBResultSet* rs1 = [db executeQuery: @"SELECT rowid, * FROM testStatementCaching WHERE value = ?", @1]; // results in 2 rows...
        FMDBQuickCheck([rs1 fmdb_next]);
        
        // confirm that we're seeing the benefits of caching.
        FMDBQuickCheck([[rs1 statement] useCount] == i);
        
        FMDBResultSet* rs2 = [db executeQuery:@"SELECT rowid, * FROM testStatementCaching WHERE value = ?", @2]; // results in 1 row
        FMDBQuickCheck([rs2 fmdb_next]);
        FMDBQuickCheck([[rs2 statement] useCount] == i);
        
        // This is the primary check - with the old implementation of statement caching, rs2 would have rejiggered the (cached) statement used by rs1, making this test fail to return the 2nd row in rs1.
        FMDBQuickCheck([rs1 fmdb_next]);
        
        [rs1 close];
        [rs2 close];
    }
    
    [db close];
}

/*
 Test the various FMDBDatabasePool things.
*/

void testPool(NSString *dbPath) {
    
    FMDBDatabasePool *dbPool = [FMDBDatabasePool databasePoolWithPath:dbPath];
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 0);
    
    __block FMDBDatabase *db1;
    
    [dbPool inDatabase:^(FMDBDatabase *db) {
        
        
        
        FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
        
        FMDBQuickCheck([db tableExists:@"t4"]);
        
        db1 = db;
        
    }];
    
    [dbPool inDatabase:^(FMDBDatabase *db) {
        FMDBQuickCheck(db1 == db);
        
        [dbPool inDatabase:^(FMDBDatabase *db2) {
            FMDBQuickCheck(db2 != db);
        }];
        
    }];
    
    
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 2);
    
    
    [dbPool inDatabase:^(FMDBDatabase *db) {
        [db fmdb_executeUpdate:@"create table easy (a text)"];
        [db fmdb_executeUpdate:@"create table easy2 (a text)"];
        
    }];
    
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 2);
    
    [dbPool releaseAllDatabases];
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 0);
    
    [dbPool inDatabase:^(FMDBDatabase *aDb) {
        
        FMDBQuickCheck([dbPool countOfCheckedInDatabases] == 0);
        FMDBQuickCheck([dbPool countOfCheckedOutDatabases] == 1);
        
        FMDBQuickCheck([aDb tableExists:@"t4"]);
        
        FMDBQuickCheck([dbPool countOfCheckedInDatabases] == 0);
        FMDBQuickCheck([dbPool countOfCheckedOutDatabases] == 1);
        
        FMDBQuickCheck(([aDb fmdb_executeUpdate:@"insert into easy (a) values (?)", @"hi"]));
        
        // just for fun.
        FMDBResultSet *rs2 = [aDb executeQuery:@"select * from easy"];
        FMDBQuickCheck([rs2 fmdb_next]);
        while ([rs2 fmdb_next]) { ; } // whatevers.
        
        FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
        FMDBQuickCheck([dbPool countOfCheckedInDatabases] == 0);
        FMDBQuickCheck([dbPool countOfCheckedOutDatabases] == 1);
    }];
    
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
    
    
    {
        
       [dbPool inDatabase:^(FMDBDatabase *db) {
        
            [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1]];
            [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:2]];
            [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:3]];
            
            FMDBQuickCheck([dbPool countOfCheckedInDatabases] == 0);
            FMDBQuickCheck([dbPool countOfCheckedOutDatabases] == 1);
       }];
    }
    
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
    
    [dbPool setMaximumNumberOfDatabasesToCreate:2];
    
    
    [dbPool inDatabase:^(FMDBDatabase *db) {
        [dbPool inDatabase:^(FMDBDatabase *db2) {
            [dbPool inDatabase:^(FMDBDatabase *db3) {
                FMDBQuickCheck([dbPool countOfOpenDatabases] == 2);
                FMDBQuickCheck(!db3);
            }];
            
        }];
    }];
    
    [dbPool setMaximumNumberOfDatabasesToCreate:0];
    
    [dbPool releaseAllDatabases];
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 0);
    
    [dbPool inDatabase:^(FMDBDatabase *db) {
        [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:3]];
    }];
    
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
    
    
    [dbPool inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1001]];
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1002]];
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1003]];
        
        FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
        FMDBQuickCheck([dbPool countOfCheckedInDatabases] == 0);
        FMDBQuickCheck([dbPool countOfCheckedOutDatabases] == 1);
    }];
    
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
    FMDBQuickCheck([dbPool countOfCheckedInDatabases] == 1);
    FMDBQuickCheck([dbPool countOfCheckedOutDatabases] == 0);
    
    
    [dbPool inDatabase:^(FMDBDatabase *db) {
        FMDBResultSet *rs2 = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1001]];
        FMDBQuickCheck([rs2 fmdb_next]);
        FMDBQuickCheck(![rs2 fmdb_next]);
    }];
    
    
    
    [dbPool inDeferredTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1004]];
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1005]];
        
        *rollback = YES;
    }];
    
    FMDBQuickCheck([dbPool countOfOpenDatabases] == 1);
    FMDBQuickCheck([dbPool countOfCheckedInDatabases] == 1);
    FMDBQuickCheck([dbPool countOfCheckedOutDatabases] == 0);
        
    NSError *err = [dbPool inSavePoint:^(FMDBDatabase *db, BOOL *rollback) {
        [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1006]];
    }];
    
    FMDBQuickCheck(!err);
    
    {
        
        err = [dbPool inSavePoint:^(FMDBDatabase *adb, BOOL *rollback) {
            FMDBQuickCheck(![adb hadError]);
            [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1009]];
            
            [adb inSavePoint:^(BOOL *arollback) {
                FMDBQuickCheck(([adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1010]]));
                *arollback = YES;
            }];
        }];
        
        
        FMDBQuickCheck(!err);
        
        [dbPool inDatabase:^(FMDBDatabase *db) {
            
            
            FMDBResultSet *rs2 = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1009]];
            FMDBQuickCheck([rs2 fmdb_next]);
            FMDBQuickCheck(![rs2 fmdb_next]); // close it out.
            
            rs2 = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1010]];
            FMDBQuickCheck(![rs2 fmdb_next]);
        }];
        
        
    }
    
    {
        
        [dbPool inDatabase:^(FMDBDatabase *db) {
            [db fmdb_executeUpdate:@"create table likefoo (foo text)"];
            [db fmdb_executeUpdate:@"insert into likefoo values ('hi')"];
            [db fmdb_executeUpdate:@"insert into likefoo values ('hello')"];
            [db fmdb_executeUpdate:@"insert into likefoo values ('not')"];
            
            int count = 0;
            FMDBResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
            while ([rsl fmdb_next]) {
                count++;
            }
            
            FMDBQuickCheck(count == 2);
            
            count = 0;
            rsl = [db executeQuery:@"select * from likefoo where foo like ?", @"h%"];
            while ([rsl fmdb_next]) {
                count++;
            }
            
            FMDBQuickCheck(count == 2);
            
        }];
    }
    
    
    {
        
        size_t ops = 128;
        
        dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
        
        dispatch_apply(ops, dqueue, ^(size_t nby) {
            
            // just mix things up a bit for demonstration purposes.
            if (nby % 2 == 1) {
                
                [NSThread sleepForTimeInterval:.1];
            }
            
            [dbPool inDatabase:^(FMDBDatabase *db) {
                NSLog(@"Starting query  %ld", nby);
                
                FMDBResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
                while ([rsl fmdb_next]) {
                    if (nby % 3 == 1) {
                        [NSThread sleepForTimeInterval:.05];
                    }
                }
                
                NSLog(@"Ending query    %ld", nby);
            }];
        });
        
        NSLog(@"Number of open databases after crazy gcd stuff: %ld", [dbPool countOfOpenDatabases]);
    }
    
    FMDBDatabasePool *dbPool2 = [FMDBDatabasePool databasePoolWithPath:dbPath flags:SQLITE_OPEN_READONLY];
    
    FMDBQuickCheck(dbPool2);
    {
        [dbPool2 inDatabase:^(FMDBDatabase *db2) {
            FMDBResultSet *rs1 = [db2 executeQuery:@"SELECT * FROM test"];
            FMDBQuickCheck(rs1 != nil);
            [rs1 close];
            
            BOOL ok = [db2 fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:3]];
            FMDBQuickCheck(!ok);
        }];
    }
    
    
    // if you want to see a deadlock, just uncomment this line and run:
    //#define ONLY_USE_THE_POOL_IF_YOU_ARE_DOING_READS_OTHERWISE_YOULL_DEADLOCK_USE_FMDATABASEQUEUE_INSTEAD 1
#ifdef ONLY_USE_THE_POOL_IF_YOU_ARE_DOING_READS_OTHERWISE_YOULL_DEADLOCK_USE_FMDATABASEQUEUE_INSTEAD
    {
        
        int ops = 16;
        
        dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
        
        dispatch_apply(ops, dqueue, ^(size_t nby) {
            
            // just mix things up a bit for demonstration purposes.
            if (nby % 2 == 1) {
                [NSThread sleepForTimeInterval:.1];
                
                [dbPool inTransaction:^(FMDBDatabase *db, BOOL *rollback) {
                    NSLog(@"Starting query  %ld", nby);
                    
                    FMDBResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
                    while ([rsl fmdb_next]) {
                        ;// whatever.
                    }
                    
                    NSLog(@"Ending query    %ld", nby);
                }];
                
            }
            
            if (nby % 3 == 1) {
                [NSThread sleepForTimeInterval:.1];
            }
            
            [dbPool inTransaction:^(FMDBDatabase *db, BOOL *rollback) {
                NSLog(@"Starting update %ld", nby);
                [db fmdb_executeUpdate:@"insert into likefoo values ('1')"];
                [db fmdb_executeUpdate:@"insert into likefoo values ('2')"];
                [db fmdb_executeUpdate:@"insert into likefoo values ('3')"];
                NSLog(@"Ending update   %ld", nby);
            }];
        });
        
        [dbPool releaseAllDatabases];
        
        [dbPool inDatabase:^(FMDBDatabase *db) {
            FMDBQuickCheck([db fmdb_executeUpdate:@"insert into likefoo values ('1')"]);
        }];
    }
#endif

}


/*
 Test the date format
 */

void testOneDateFormat( FMDBDatabase *db, NSDate *testDate ) {
    [db fmdb_executeUpdate:@"DROP TABLE IF EXISTS test_format"];
    [db fmdb_executeUpdate:@"CREATE TABLE test_format ( test TEXT )"];
    [db fmdb_executeUpdate:@"INSERT INTO test_format(test) VALUES (?)", testDate];
    FMDBResultSet *rs = [db executeQuery:@"SELECT test FROM test_format"];
    if ([rs fmdb_next]) {
        NSDate *found = [rs dateForColumnIndex:0];
        if (NSOrderedSame != [testDate compare:found]) {
            NSLog(@"Did not get back what we stored.");
        }
    }
    else {
        NSLog(@"Insertion borked");
    }
    [rs close];
}

void testDateFormat() {
    
    FMDBDatabase *db = [FMDBDatabase databaseWithPath:nil]; // use in-memory DB
    [db open];
    
    NSDateFormatter *fmt = [FMDBDatabase storeableDateFormat:@"yyyy-MM-dd HH:mm:ss"];
    
    NSDate *testDate = [fmt dateFromString:@"2013-02-20 12:00:00"];
    
    // test timestamp dates (ensuring our change does not break those)
    testOneDateFormat(db,testDate);
    
    // now test the string-based timestamp
    [db setDateFormat:fmt];
    testOneDateFormat(db, testDate);
    
    [db close];
}


/*
 What is this function for?  Think of it as a template which a developer can use
 to report bugs.
 
 If you have a bug, make it reproduce in this function and then let the
 developer(s) know either via the github bug reporter or the mailing list.
 */

void FMDBReportABugFunction() {
    
    NSString *dbPath = @"/tmp/bugreportsample.db";
    
    // delete the old db if it exists
    NSFileManager *fileManager = [NSFileManager defaultManager];
    [fileManager removeItemAtPath:dbPath error:nil];
    
    FMDBDatabaseQueue *queue = [FMDBDatabaseQueue databaseQueueWithPath:dbPath];
    
    [queue inDatabase:^(FMDBDatabase *db) {
        
        /*
         Change the contents of this block to suit your needs.
         */
        
        BOOL worked = [db fmdb_executeUpdate:@"create table test (a text, b text, c integer, d double, e double)"];
        FMDBQuickCheck(worked);
        
        
        worked = [db fmdb_executeUpdate:@"insert into test values ('a', 'b', 1, 2.2, 2.3)"];
        FMDBQuickCheck(worked);
        
        FMDBResultSet *rs = [db executeQuery:@"select * from test"];
        FMDBQuickCheck([rs fmdb_next]);
        [rs close];
        
    }];
    
    
    [queue close];
    
    
    // uncomment the following line if you don't want to run through all the other tests.
    //exit(0);
    
}





