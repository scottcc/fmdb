//
//  Tests.m
//  Tests
//
//  Created by Graham Dennis on 24/11/2013.
//
//

#import "FMDBTempDBTests.h"
#import "FMDBDatabase.h"
#import "FMDBDatabaseAdditions.h"

#if FMDB_SQLITE_STANDALONE
#import <sqlite3/sqlite3.h>
#else
#import <sqlite3.h>
#endif


@interface FMDatabaseTests : FMDBTempDBTests

@end

@implementation FMDatabaseTests

+ (void)populateDatabase:(FMDBDatabase *)db
{
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
    
    [db fmdb_executeUpdate:@"create table t3 (a somevalue)"];
    
    [db fmdb_beginTransaction];
    for (int i=0; i < 20; i++) {
        [db fmdb_executeUpdate:@"insert into t3 (a) values (?)", [NSNumber numberWithInt:i]];
    }
    [db commit];
}

- (void)setUp
{
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
    
}

- (void)tearDown
{
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
    
}

- (void)testOpenWithVFS
{
    // create custom vfs
    sqlite3_vfs vfs = *sqlite3_vfs_find(NULL);
    vfs.zName = "MyCustomVFS";
    XCTAssertEqual(SQLITE_OK, sqlite3_vfs_register(&vfs, 0));
    // use custom vfs to open a in memory database
    FMDBDatabase *db = [[FMDBDatabase alloc] initWithPath:@":memory:"];
    [db openWithFlags:SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE vfs:@"MyCustomVFS"];
    XCTAssertFalse([db hadError], @"Open with a custom VFS should have succeeded");
}

- (void)testFailOnOpenWithUnknownVFS
{
    FMDBDatabase *db = [[FMDBDatabase alloc] initWithPath:@":memory:"];
    [db openWithFlags:SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE vfs:@"UnknownVFS"];
    XCTAssertTrue([db hadError], @"Should have failed");    
}

- (void)testFailOnUnopenedDatabase
{
    [self.db close];
    
    XCTAssertNil([self.db executeQuery:@"select * from table"], @"Shouldn't get results from an empty table");
    XCTAssertTrue([self.db hadError], @"Should have failed");
}

- (void)testFailOnBadStatement
{
    XCTAssertFalse([self.db fmdb_executeUpdate:@"blah blah blah"], @"Invalid statement should fail");
    XCTAssertTrue([self.db hadError], @"Should have failed");
}

- (void)testFailOnBadStatementWithError
{
    NSError *error = nil;
    XCTAssertFalse([self.db fmdb_executeUpdate:@"blah blah blah" withErrorAndBindings:&error], @"Invalid statement should fail");
    XCTAssertNotNil(error, @"Should have a non-nil NSError");
    XCTAssertEqual([error code], (NSInteger)SQLITE_ERROR, @"Error should be SQLITE_ERROR");
}

- (void)testPragmaJournalMode
{
    FMDBResultSet *ps = [self.db executeQuery:@"pragma journal_mode=delete"];
    XCTAssertFalse([self.db hadError], @"pragma should have succeeded");
    XCTAssertNotNil(ps, @"Result set should be non-nil");
    XCTAssertTrue([ps fmdb_next], @"Result set should have a fmdb_next result");
    [ps close];
}

- (void)testPragmaPageSize
{
    [self.db fmdb_executeUpdate:@"PRAGMA page_size=2048"];
    XCTAssertFalse([self.db hadError], @"pragma should have succeeded");
}

- (void)testVacuum
{
    [self.db fmdb_executeUpdate:@"VACUUM"];
    XCTAssertFalse([self.db hadError], @"VACUUM should have succeeded");
}

- (void)testSelectULL
{
    // Unsigned long long
    [self.db fmdb_executeUpdate:@"create table ull (a integer)"];
    
    [self.db fmdb_executeUpdate:@"insert into ull (a) values (?)", [NSNumber numberWithUnsignedLongLong:ULLONG_MAX]];
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
    
    FMDBResultSet *rs = [self.db executeQuery:@"select a from ull"];
    while ([rs fmdb_next]) {
        XCTAssertEqual([rs unsignedLongLongIntForColumnIndex:0], ULLONG_MAX, @"Result should be ULLONG_MAX");
        XCTAssertEqual([rs unsignedLongLongIntForColumn:@"a"],   ULLONG_MAX, @"Result should be ULLONG_MAX");
    }
    
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testSelectByColumnName
{
    FMDBResultSet *rs = [self.db executeQuery:@"select rowid,* from test where a = ?", @"hi"];
    
    XCTAssertNotNil(rs, @"Should have a non-nil result set");
    
    while ([rs fmdb_next]) {
        [rs intForColumn:@"c"];
        XCTAssertNotNil([rs stringForColumn:@"b"], @"Should have non-nil string for 'b'");
        XCTAssertNotNil([rs stringForColumn:@"a"], @"Should have non-nil string for 'a'");
        XCTAssertNotNil([rs stringForColumn:@"rowid"], @"Should have non-nil string for 'rowid'");
        XCTAssertNotNil([rs dateForColumn:@"d"], @"Should have non-nil date for 'd'");
        [rs doubleForColumn:@"d"];
        [rs doubleForColumn:@"e"];
        
        XCTAssertEqualObjects([rs columnNameForIndex:0], @"rowid",  @"Wrong column name for result set column number");
        XCTAssertEqualObjects([rs columnNameForIndex:1], @"a",      @"Wrong column name for result set column number");
    }
    
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testSelectWithIndexedAndKeyedSubscript
{
    FMDBResultSet *rs = [self.db executeQuery:@"select rowid, a, b, c from test"];
    
    XCTAssertNotNil(rs, @"Should have a non-nil result set");
    
    while ([rs fmdb_next]) {
        XCTAssertEqualObjects(rs[0], rs[@"rowid"], @"Column zero should be equal to 'rowid'");
        XCTAssertEqualObjects(rs[1], rs[@"a"], @"Column 1 should be equal to 'a'");
        XCTAssertEqualObjects(rs[2], rs[@"b"], @"Column 2 should be equal to 'b'");
        XCTAssertEqualObjects(rs[3], rs[@"c"], @"Column 3 should be equal to 'c'");
    }
    
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testBusyRetryTimeout
{
    [self.db fmdb_executeUpdate:@"create table t1 (a integer)"];
    [self.db fmdb_executeUpdate:@"insert into t1 values (?)", [NSNumber numberWithInt:5]];
    
    [self.db setMaxBusyRetryTimeInterval:2];
    
    FMDBDatabase *newDB = [FMDBDatabase databaseWithPath:self.databasePath];
    [newDB open];
    
    FMDBResultSet *rs = [newDB executeQuery:@"select rowid,* from test where a = ?", @"hi'"];
    [rs fmdb_next]; // just grab one... which will keep the db locked
    
    XCTAssertFalse([self.db fmdb_executeUpdate:@"insert into t1 values (5)"], @"Insert should fail because the db is locked by a read");
    XCTAssertEqual([self.db lastErrorCode], SQLITE_BUSY, @"SQLITE_BUSY should be the last error");
    
    [rs close];
    [newDB close];
    
    XCTAssertTrue([self.db fmdb_executeUpdate:@"insert into t1 values (5)"], @"The database shouldn't be locked at this point");
}

- (void)testCaseSensitiveResultDictionary
{
    // case sensitive result dictionary test
    [self.db fmdb_executeUpdate:@"create table cs (aRowName integer, bRowName text)"];
    [self.db fmdb_executeUpdate:@"insert into cs (aRowName, bRowName) values (?, ?)", [NSNumber numberWithBool:1], @"hello"];

    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");

    FMDBResultSet *rs = [self.db executeQuery:@"select * from cs"];
    while ([rs fmdb_next]) {
        NSDictionary *d = [rs resultDictionary];
        
        XCTAssertNotNil([d objectForKey:@"aRowName"], @"aRowName should be non-nil");
        XCTAssertNil([d objectForKey:@"arowname"], @"arowname should be nil");
        XCTAssertNotNil([d objectForKey:@"bRowName"], @"bRowName should be non-nil");
        XCTAssertNil([d objectForKey:@"browname"], @"browname should be nil");
    }
    
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testBoolInsert
{
    [self.db fmdb_executeUpdate:@"create table btest (aRowName integer)"];
    [self.db fmdb_executeUpdate:@"insert into btest (aRowName) values (?)", [NSNumber numberWithBool:12]];
    
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from btest"];
    while ([rs fmdb_next]) {
        
        XCTAssertTrue([rs boolForColumnIndex:0], @"first column should be true.");
        XCTAssertTrue([rs fmdb_intForColumnIndex:0] == 1, @"first column should be equal to 1 - it was %d.", [rs fmdb_intForColumnIndex:0]);
    }
    
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testNamedParametersCount
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table namedparamcounttest (a text, b text, c integer, d double)"]);

    NSMutableDictionary *dictionaryArgs = [NSMutableDictionary dictionary];
    [dictionaryArgs setObject:@"Text1" forKey:@"a"];
    [dictionaryArgs setObject:@"Text2" forKey:@"b"];
    [dictionaryArgs setObject:[NSNumber numberWithInt:1] forKey:@"c"];
    [dictionaryArgs setObject:[NSNumber numberWithDouble:2.0] forKey:@"d"];
    XCTAssertTrue([self.db fmdb_executeUpdate:@"insert into namedparamcounttest values (:a, :b, :c, :d)" withParameterDictionary:dictionaryArgs]);
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from namedparamcounttest"];
    
    XCTAssertNotNil(rs);
    
    [rs fmdb_next];
    
    XCTAssertEqualObjects([rs stringForColumn:@"a"], @"Text1");
    XCTAssertEqualObjects([rs stringForColumn:@"b"], @"Text2");
    XCTAssertEqual([rs intForColumn:@"c"], 1);
    XCTAssertEqual([rs doubleForColumn:@"d"], 2.0);
    
    [rs close];
    
    // note that at this point, dictionaryArgs has way more values than we need, but the query should still work since
    // a is in there, and that's all we need.
    rs = [self.db executeQuery:@"select * from namedparamcounttest where a = :a" withParameterDictionary:dictionaryArgs];
    
    XCTAssertNotNil(rs);
    XCTAssertTrue([rs fmdb_next]);
    [rs close];
    
    // ***** Please note the following codes *****
    
    dictionaryArgs = [NSMutableDictionary dictionary];
    
    [dictionaryArgs setObject:@"NewText1" forKey:@"a"];
    [dictionaryArgs setObject:@"NewText2" forKey:@"b"];
    [dictionaryArgs setObject:@"OneMoreText" forKey:@"OneMore"];
    
    XCTAssertTrue([self.db fmdb_executeUpdate:@"update namedparamcounttest set a = :a, b = :b where b = 'Text2'" withParameterDictionary:dictionaryArgs]);
    
}

- (void)testBlobs
{
    [self.db fmdb_executeUpdate:@"create table blobTable (a text, b blob)"];
    
    // let's read an image from safari's app bundle.
    NSData *safariCompass = [NSData dataWithContentsOfFile:@"/Applications/Safari.app/Contents/Resources/compass.icns"];
    if (safariCompass) {
        [self.db fmdb_executeUpdate:@"insert into blobTable (a, b) values (?, ?)", @"safari's compass", safariCompass];
        
        FMDBResultSet *rs = [self.db executeQuery:@"select b from blobTable where a = ?", @"safari's compass"];
        XCTAssertTrue([rs fmdb_next]);
        NSData *readData = [rs dataForColumn:@"b"];
        XCTAssertEqualObjects(readData, safariCompass);
        
        // ye shall read the header for this function, or suffer the consequences.
        NSData *readDataNoCopy = [rs dataNoCopyForColumn:@"b"];
        XCTAssertEqualObjects(readDataNoCopy, safariCompass);
        
        [rs close];
        XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
        XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
    }
}

- (void)testNullValues
{
    [self.db fmdb_executeUpdate:@"create table t2 (a integer, b integer)"];
    
    BOOL result = [self.db fmdb_executeUpdate:@"insert into t2 values (?, ?)", nil, [NSNumber numberWithInt:5]];
    XCTAssertTrue(result, @"Failed to insert a nil value");
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from t2"];
    while ([rs fmdb_next]) {
        XCTAssertNil([rs fmdb_stringForColumnIndex:0], @"Wasn't able to retrieve a null string");
        XCTAssertEqualObjects([rs fmdb_stringForColumnIndex:1], @"5");
    }
    
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testNestedResultSets
{
    FMDBResultSet *rs = [self.db executeQuery:@"select * from t3"];
    while ([rs fmdb_next]) {
        int foo = [rs fmdb_intForColumnIndex:0];
        
        int newVal = foo + 100;
        
        [self.db fmdb_executeUpdate:@"update t3 set a = ? where a = ?", [NSNumber numberWithInt:newVal], [NSNumber numberWithInt:foo]];
        
        FMDBResultSet *rs2 = [self.db executeQuery:@"select a from t3 where a = ?", [NSNumber numberWithInt:newVal]];
        [rs2 fmdb_next];
        
        XCTAssertEqual([rs2 fmdb_intForColumnIndex:0], newVal);
        
        [rs2 close];
    }
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testNSNullInsertion
{
    [self.db fmdb_executeUpdate:@"create table nulltest (a text, b text)"];
    
    [self.db fmdb_executeUpdate:@"insert into nulltest (a, b) values (?, ?)", [NSNull null], @"a"];
    [self.db fmdb_executeUpdate:@"insert into nulltest (a, b) values (?, ?)", nil, @"b"];
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from nulltest"];
    
    while ([rs fmdb_next]) {
        XCTAssertNil([rs fmdb_stringForColumnIndex:0]);
        XCTAssertNotNil([rs fmdb_stringForColumnIndex:1]);
    }
    
    [rs close];
    
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testNullDates
{
    NSDate *date = [NSDate date];
    [self.db fmdb_executeUpdate:@"create table datetest (a double, b double, c double)"];
    [self.db fmdb_executeUpdate:@"insert into datetest (a, b, c) values (?, ?, 0)" , [NSNull null], date];
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from datetest"];
    
    XCTAssertNotNil(rs);
    
    while ([rs fmdb_next]) {
        
        NSDate *b = [rs dateForColumnIndex:1];
        NSDate *c = [rs dateForColumnIndex:2];
        
        XCTAssertNil([rs dateForColumnIndex:0]);
        XCTAssertNotNil(c, @"zero date shouldn't be nil");
        
        XCTAssertEqualWithAccuracy([b timeIntervalSinceDate:date],  0.0, 1.0, @"Dates should be the same to within a second");
        XCTAssertEqualWithAccuracy([c timeIntervalSince1970],       0.0, 1.0, @"Dates should be the same to within a second");
    }
    [rs close];
    
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testLotsOfNULLs
{
    NSData *safariCompass = [NSData dataWithContentsOfFile:@"/Applications/Safari.app/Contents/Resources/compass.icns"];
    
    if (!safariCompass)
        return;
    
    [self.db fmdb_executeUpdate:@"create table nulltest2 (s text, d data, i integer, f double, b integer)"];
    
    [self.db fmdb_executeUpdate:@"insert into nulltest2 (s, d, i, f, b) values (?, ?, ?, ?, ?)" , @"Hi", safariCompass, [NSNumber numberWithInt:12], [NSNumber numberWithFloat:4.4f], [NSNumber numberWithBool:YES]];
    [self.db fmdb_executeUpdate:@"insert into nulltest2 (s, d, i, f, b) values (?, ?, ?, ?, ?)" , nil, nil, nil, nil, [NSNull null]];
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from nulltest2"];
    
    while ([rs fmdb_next]) {
        
        int i = [rs fmdb_intForColumnIndex:2];
        
        if (i == 12) {
            // it's the first row we inserted.
            XCTAssertFalse([rs columnIndexIsNull:0]);
            XCTAssertFalse([rs columnIndexIsNull:1]);
            XCTAssertFalse([rs columnIndexIsNull:2]);
            XCTAssertFalse([rs columnIndexIsNull:3]);
            XCTAssertFalse([rs columnIndexIsNull:4]);
            XCTAssertTrue( [rs columnIndexIsNull:5]);
            
            XCTAssertEqualObjects([rs dataForColumn:@"d"], safariCompass);
            XCTAssertNil([rs dataForColumn:@"notthere"]);
            XCTAssertNil([rs fmdb_stringForColumnIndex:-2], @"Negative columns should return nil results");
            XCTAssertTrue([rs boolForColumnIndex:4]);
            XCTAssertTrue([rs boolForColumn:@"b"]);
            
            XCTAssertEqualWithAccuracy(4.4, [rs doubleForColumn:@"f"], 0.0000001, @"Saving a float and returning it as a double shouldn't change the result much");
            
            XCTAssertEqual([rs intForColumn:@"i"], 12);
            XCTAssertEqual([rs fmdb_intForColumnIndex:2], 12);
            
            XCTAssertEqual([rs fmdb_intForColumnIndex:12],       0, @"Non-existent columns should return zero for ints");
            XCTAssertEqual([rs intForColumn:@"notthere"],   0, @"Non-existent columns should return zero for ints");
            
            XCTAssertEqual([rs longForColumn:@"i"], 12l);
            XCTAssertEqual([rs longLongIntForColumn:@"i"], 12ll);
        }
        else {
            // let's test various null things.
            
            XCTAssertTrue([rs columnIndexIsNull:0]);
            XCTAssertTrue([rs columnIndexIsNull:1]);
            XCTAssertTrue([rs columnIndexIsNull:2]);
            XCTAssertTrue([rs columnIndexIsNull:3]);
            XCTAssertTrue([rs columnIndexIsNull:4]);
            XCTAssertTrue([rs columnIndexIsNull:5]);
            
            
            XCTAssertNil([rs dataForColumn:@"d"]);
        }
    }
    
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testUTF8Strings
{
    [self.db fmdb_executeUpdate:@"create table utest (a text)"];
    [self.db fmdb_executeUpdate:@"insert into utest values (?)", @"/übertest"];
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from utest where a = ?", @"/übertest"];
    XCTAssertTrue([rs fmdb_next]);
    [rs close];
    XCTAssertFalse([self.db hasOpenResultSets], @"Shouldn't have any open result sets");
    XCTAssertFalse([self.db hadError], @"Shouldn't have any errors");
}

- (void)testArgumentsInArray
{
    [self.db fmdb_executeUpdate:@"create table testOneHundredTwelvePointTwo (a text, b integer)"];
    [self.db fmdb_executeUpdate:@"insert into testOneHundredTwelvePointTwo values (?, ?)" withArgumentsInArray:[NSArray arrayWithObjects:@"one", [NSNumber numberWithInteger:2], nil]];
    [self.db fmdb_executeUpdate:@"insert into testOneHundredTwelvePointTwo values (?, ?)" withArgumentsInArray:[NSArray arrayWithObjects:@"one", [NSNumber numberWithInteger:3], nil]];
    
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from testOneHundredTwelvePointTwo where b > ?" withArgumentsInArray:[NSArray arrayWithObject:[NSNumber numberWithInteger:1]]];
    
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertTrue([rs hasAnotherRow]);
    XCTAssertFalse([self.db hadError]);
    
    XCTAssertEqualObjects([rs fmdb_stringForColumnIndex:0], @"one");
    XCTAssertEqual([rs fmdb_intForColumnIndex:1], 2);
    
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqual([rs fmdb_intForColumnIndex:1], 3);
    
    XCTAssertFalse([rs fmdb_next]);
    XCTAssertFalse([rs hasAnotherRow]);
}

- (void)testColumnNamesContainingPeriods
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table t4 (a text, b text)"]);
    [self.db fmdb_executeUpdate:@"insert into t4 (a, b) values (?, ?)", @"one", @"two"];
    
    FMDBResultSet *rs = [self.db executeQuery:@"select t4.a as 't4.a', t4.b from t4;"];
    
    XCTAssertNotNil(rs);
    
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqualObjects([rs stringForColumn:@"t4.a"], @"one");
    XCTAssertEqualObjects([rs stringForColumn:@"b"], @"two");
    
    XCTAssertEqual(strcmp((const char*)[rs UTF8StringForColumnName:@"b"], "two"), 0, @"String comparison should return zero");
    
    [rs close];
    
    // let's try these again, with the withArgumentsInArray: variation
    XCTAssertTrue([self.db fmdb_executeUpdate:@"drop table t4;" withArgumentsInArray:[NSArray array]]);
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table t4 (a text, b text)" withArgumentsInArray:[NSArray array]]);
    
    [self.db fmdb_executeUpdate:@"insert into t4 (a, b) values (?, ?)" withArgumentsInArray:[NSArray arrayWithObjects:@"one", @"two", nil]];
    
    rs = [self.db executeQuery:@"select t4.a as 't4.a', t4.b from t4;" withArgumentsInArray:[NSArray array]];
    
    XCTAssertNotNil(rs);
    
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqualObjects([rs stringForColumn:@"t4.a"], @"one");
    XCTAssertEqualObjects([rs stringForColumn:@"b"], @"two");
    
    XCTAssertEqual(strcmp((const char*)[rs UTF8StringForColumnName:@"b"], "two"), 0, @"String comparison should return zero");
    
    [rs close];
}

- (void)testFormatStringParsing
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table t5 (a text, b int, c blob, d text, e text)"]);
    [self.db executeUpdateWithFormat:@"insert into t5 values (%s, %d, %@, %c, %lld)", "text", 42, @"BLOB", 'd', 12345678901234ll];
    
    FMDBResultSet *rs = [self.db executeQueryWithFormat:@"select * from t5 where a = %s and a = %@ and b = %d", "text", @"text", 42];
    XCTAssertNotNil(rs);
    
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqualObjects([rs stringForColumn:@"a"], @"text");
    XCTAssertEqual([rs intForColumn:@"b"], 42);
    XCTAssertEqualObjects([rs stringForColumn:@"c"], @"BLOB");
    XCTAssertEqualObjects([rs stringForColumn:@"d"], @"d");
    XCTAssertEqual([rs longLongIntForColumn:@"e"], 12345678901234ll);
    
    [rs close];
}

- (void)testFormatStringParsingWithSizePrefixes
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table t55 (a text, b int, c float)"]);
    short testShort = -4;
    float testFloat = 5.5;
    [self.db executeUpdateWithFormat:@"insert into t55 values (%c, %hi, %g)", 'a', testShort, testFloat];
    
    unsigned short testUShort = 6;
    [self.db executeUpdateWithFormat:@"insert into t55 values (%c, %hu, %g)", 'a', testUShort, testFloat];
    
    
    FMDBResultSet *rs = [self.db executeQueryWithFormat:@"select * from t55 where a = %s order by 2", "a"];
    XCTAssertNotNil(rs);
    
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqualObjects([rs stringForColumn:@"a"], @"a");
    XCTAssertEqual([rs intForColumn:@"b"], -4);
    XCTAssertEqualObjects([rs stringForColumn:@"c"], @"5.5");
    
    
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqualObjects([rs stringForColumn:@"a"], @"a");
    XCTAssertEqual([rs intForColumn:@"b"], 6);
    XCTAssertEqualObjects([rs stringForColumn:@"c"], @"5.5");
    
    [rs close];
}

- (void)testFormatStringParsingWithNilValue
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table tatwhat (a text)"]);
    
    BOOL worked = [self.db executeUpdateWithFormat:@"insert into tatwhat values(%@)", nil];
    
    XCTAssertTrue(worked);
    
    FMDBResultSet *rs = [self.db executeQueryWithFormat:@"select * from tatwhat"];
    XCTAssertNotNil(rs);
    XCTAssertTrue([rs fmdb_next]);
    XCTAssertTrue([rs columnIndexIsNull:0]);
    
    XCTAssertFalse([rs fmdb_next]);
}

- (void)testUpdateWithErrorAndBindings
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table t5 (a text, b int, c blob, d text, e text)"]);
    
    NSError *err = nil;
    BOOL result = [self.db fmdb_executeUpdate:@"insert into t5 values (?, ?, ?, ?, ?)" withErrorAndBindings:&err, @"text", [NSNumber numberWithInt:42], @"BLOB", @"d", [NSNumber numberWithInt:0]];
    XCTAssertTrue(result);
}

- (void)testSelectWithEmptyArgumentsArray
{
    FMDBResultSet *rs = [self.db executeQuery:@"select * from test where a=?" withArgumentsInArray:@[]];
    XCTAssertNil(rs);
}

- (void)testDatabaseAttach
{
    NSFileManager *fileManager = [NSFileManager new];
    [fileManager removeItemAtPath:@"/tmp/attachme.db" error:nil];
    
    FMDBDatabase *dbB = [FMDBDatabase databaseWithPath:@"/tmp/attachme.db"];
    XCTAssertTrue([dbB open]);
    XCTAssertTrue([dbB fmdb_executeUpdate:@"create table attached (a text)"]);
    XCTAssertTrue(([dbB fmdb_executeUpdate:@"insert into attached values (?)", @"test"]));
    XCTAssertTrue([dbB close]);
    
    [self.db fmdb_executeUpdate:@"attach database '/tmp/attachme.db' as attack"];
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from attack.attached"];
    XCTAssertNotNil(rs);
    XCTAssertTrue([rs fmdb_next]);
    [rs close];
}

- (void)testNamedParameters
{
    // -------------------------------------------------------------------------------
    // Named parameters.
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table namedparamtest (a text, b text, c integer, d double)"]);
    
    NSMutableDictionary *dictionaryArgs = [NSMutableDictionary dictionary];
    [dictionaryArgs setObject:@"Text1" forKey:@"a"];
    [dictionaryArgs setObject:@"Text2" forKey:@"b"];
    [dictionaryArgs setObject:[NSNumber numberWithInt:1] forKey:@"c"];
    [dictionaryArgs setObject:[NSNumber numberWithDouble:2.0] forKey:@"d"];
    XCTAssertTrue([self.db fmdb_executeUpdate:@"insert into namedparamtest values (:a, :b, :c, :d)" withParameterDictionary:dictionaryArgs]);
    
    FMDBResultSet *rs = [self.db executeQuery:@"select * from namedparamtest"];
    
    XCTAssertNotNil(rs);
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqualObjects([rs stringForColumn:@"a"], @"Text1");
    XCTAssertEqualObjects([rs stringForColumn:@"b"], @"Text2");
    XCTAssertEqual([rs intForColumn:@"c"], 1);
    XCTAssertEqual([rs doubleForColumn:@"d"], 2.0);
    
    [rs close];
    
    
    dictionaryArgs = [NSMutableDictionary dictionary];
    
    [dictionaryArgs setObject:@"Text2" forKey:@"blah"];
    
    rs = [self.db executeQuery:@"select * from namedparamtest where b = :blah" withParameterDictionary:dictionaryArgs];
    
    XCTAssertNotNil(rs);
    XCTAssertTrue([rs fmdb_next]);
    
    XCTAssertEqualObjects([rs stringForColumn:@"b"], @"Text2");
    
    [rs close];
}

- (void)testPragmaDatabaseList
{
    FMDBResultSet *rs = [self.db executeQuery:@"pragma database_list"];
    int counter = 0;
    while ([rs fmdb_next]) {
        counter++;
        XCTAssertEqualObjects([rs stringForColumn:@"file"], self.databasePath);
    }
    XCTAssertEqual(counter, 1, @"Only one database should be attached");
}

- (void)testCachedStatementsInUse
{
    [self.db setShouldCacheStatements:true];
    
    [self.db fmdb_executeUpdate:@"CREATE TABLE testCacheStatements(key INTEGER PRIMARY KEY, value INTEGER)"];
    [self.db fmdb_executeUpdate:@"INSERT INTO testCacheStatements (key, value) VALUES (1, 2)"];
    [self.db fmdb_executeUpdate:@"INSERT INTO testCacheStatements (key, value) VALUES (2, 4)"];
    
    XCTAssertTrue([[self.db executeQuery:@"SELECT * FROM testCacheStatements WHERE key=1"] fmdb_next]);
    XCTAssertTrue([[self.db executeQuery:@"SELECT * FROM testCacheStatements WHERE key=1"] fmdb_next]);
}

- (void)testStatementCachingWorks
{
    [self.db fmdb_executeUpdate:@"CREATE TABLE testStatementCaching ( value INTEGER )"];
    [self.db fmdb_executeUpdate:@"INSERT INTO testStatementCaching( value ) VALUES (1)"];
    [self.db fmdb_executeUpdate:@"INSERT INTO testStatementCaching( value ) VALUES (1)"];
    [self.db fmdb_executeUpdate:@"INSERT INTO testStatementCaching( value ) VALUES (2)"];
    
    [self.db setShouldCacheStatements:YES];
    
    // two iterations.
    //  the first time through no statements will be from the cache.
    //  the second time through all statements come from the cache.
    for (int i = 1; i <= 2; i++ ) {
        
        FMDBResultSet* rs1 = [self.db executeQuery: @"SELECT rowid, * FROM testStatementCaching WHERE value = ?", @1]; // results in 2 rows...
        XCTAssertNotNil(rs1);
        XCTAssertTrue([rs1 fmdb_next]);
        
        // confirm that we're seeing the benefits of caching.
        XCTAssertEqual([[rs1 statement] useCount], (long)i);
        
        FMDBResultSet* rs2 = [self.db executeQuery:@"SELECT rowid, * FROM testStatementCaching WHERE value = ?", @2]; // results in 1 row
        XCTAssertNotNil(rs2);
        XCTAssertTrue([rs2 fmdb_next]);
        XCTAssertEqual([[rs2 statement] useCount], (long)i);
        
        // This is the primary check - with the old implementation of statement caching, rs2 would have rejiggered the (cached) statement used by rs1, making this test fail to return the 2nd row in rs1.
        XCTAssertTrue([rs1 fmdb_next]);
        
        [rs1 close];
        [rs2 close];
    }
    
}

/*
 Test the date format
 */

- (void)testDateFormat
{
    void (^testOneDateFormat)(FMDBDatabase *, NSDate *) = ^( FMDBDatabase *db, NSDate *testDate ){
        [db fmdb_executeUpdate:@"DROP TABLE IF EXISTS test_format"];
        [db fmdb_executeUpdate:@"CREATE TABLE test_format ( test TEXT )"];
        [db fmdb_executeUpdate:@"INSERT INTO test_format(test) VALUES (?)", testDate];
        
        FMDBResultSet *rs = [db executeQuery:@"SELECT test FROM test_format"];
        XCTAssertNotNil(rs);
        XCTAssertTrue([rs fmdb_next]);
        
        XCTAssertEqualObjects([rs dateForColumnIndex:0], testDate);

        [rs close];
    };
    
    NSDateFormatter *fmt = [FMDBDatabase storeableDateFormat:@"yyyy-MM-dd HH:mm:ss"];
    
    NSDate *testDate = [fmt dateFromString:@"2013-02-20 12:00:00"];
    
    // test timestamp dates (ensuring our change does not break those)
    testOneDateFormat(self.db,testDate);
    
    // now test the string-based timestamp
    [self.db setDateFormat:fmt];
    testOneDateFormat(self.db, testDate);
}

- (void)testColumnNameMap
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table colNameTest (a, b, c, d)"]);
    XCTAssertTrue([self.db fmdb_executeUpdate:@"insert into colNameTest values (1, 2, 3, 4)"]);
    
    FMDBResultSet *ars = [self.db executeQuery:@"select * from colNameTest"];
    XCTAssertNotNil(ars);
    
    NSDictionary *d = [ars columnNameToIndexMap];
    XCTAssertEqual([d count], (NSUInteger)4);
    
    XCTAssertEqualObjects([d objectForKey:@"a"], @0);
    XCTAssertEqualObjects([d objectForKey:@"b"], @1);
    XCTAssertEqualObjects([d objectForKey:@"c"], @2);
    XCTAssertEqualObjects([d objectForKey:@"d"], @3);
    
}

- (void)testCustomFunction
{
    [self.db fmdb_executeUpdate:@"create table ftest (foo text)"];
    [self.db fmdb_executeUpdate:@"insert into ftest values ('hello')"];
    [self.db fmdb_executeUpdate:@"insert into ftest values ('hi')"];
    [self.db fmdb_executeUpdate:@"insert into ftest values ('not h!')"];
    [self.db fmdb_executeUpdate:@"insert into ftest values ('definitely not h!')"];
    
    [self.db makeFunctionNamed:@"StringStartsWithH" maximumArguments:1 withBlock:^(void *context, int aargc, void **aargv) {
        if (sqlite3_value_type(aargv[0]) == SQLITE_TEXT) {
            
            @autoreleasepool {
                
                const char *c = (const char *)sqlite3_value_text(aargv[0]);
                
                NSString *s = [NSString stringWithUTF8String:c];
                
                sqlite3_result_int(context, [s hasPrefix:@"h"]);
            }
        }
        else {
            XCTFail(@"Unknown format for StringStartsWithH (%d)", sqlite3_value_type(aargv[0]));
            sqlite3_result_null(context);
        }
    }];
    
    int rowCount = 0;
    FMDBResultSet *ars = [self.db executeQuery:@"select * from ftest where StringStartsWithH(foo)"];
    while ([ars fmdb_next]) {
        rowCount++;
        
    }
    XCTAssertEqual(rowCount, 2);

}

- (void)testVersionNumber {
    XCTAssertTrue([FMDBDatabase FMDBVersion] == 0x0262); // this is going to break everytime we bump it.
}

- (void)testExecuteStatements
{
    BOOL success;

    NSString *sql = @"create table bulktest1 (id integer primary key autoincrement, x text);"
                     "create table bulktest2 (id integer primary key autoincrement, y text);"
                     "create table bulktest3 (id integer primary key autoincrement, z text);"
                     "insert into bulktest1 (x) values ('XXX');"
                     "insert into bulktest2 (y) values ('YYY');"
                     "insert into bulktest3 (z) values ('ZZZ');";

    success = [self.db executeStatements:sql];

    XCTAssertTrue(success, @"bulk create");

    sql = @"select count(*) as count from bulktest1;"
           "select count(*) as count from bulktest2;"
           "select count(*) as count from bulktest3;";

    success = [self.db executeStatements:sql withResultBlock:^int(NSDictionary *dictionary) {
        NSInteger count = [dictionary[@"count"] integerValue];
        XCTAssertEqual(count, 1, @"expected one record for dictionary %@", dictionary);
        return 0;
    }];

    XCTAssertTrue(success, @"bulk select");

    sql = @"drop table bulktest1;"
           "drop table bulktest2;"
           "drop table bulktest3;";

    success = [self.db executeStatements:sql];

    XCTAssertTrue(success, @"bulk drop");
}

- (void)testCharAndBoolTypes
{
    XCTAssertTrue([self.db fmdb_executeUpdate:@"create table charBoolTest (a, b, c)"]);

    BOOL success = [self.db fmdb_executeUpdate:@"insert into charBoolTest values (?, ?, ?)", @YES, @NO, @('x')];
    XCTAssertTrue(success, @"Unable to insert values");

    FMDBResultSet *rs = [self.db executeQuery:@"select * from charBoolTest"];
    XCTAssertNotNil(rs);

    XCTAssertTrue([rs fmdb_next], @"Did not return row");

    XCTAssertEqual([rs boolForColumn:@"a"], true);
    XCTAssertEqualObjects([rs objectForColumnName:@"a"], @YES);

    XCTAssertEqual([rs boolForColumn:@"b"], false);
    XCTAssertEqualObjects([rs objectForColumnName:@"b"], @NO);

    XCTAssertEqual([rs intForColumn:@"c"], 'x');
    XCTAssertEqualObjects([rs objectForColumnName:@"c"], @('x'));

    [rs close];

    XCTAssertTrue([self.db fmdb_executeUpdate:@"drop table charBoolTest"], @"Did not drop table");

}

- (void)testSqliteLibVersion
{
    NSString *version = [FMDBDatabase sqliteLibVersion];
    XCTAssert([version compare:@"3.7" options:NSNumericSearch] == NSOrderedDescending, @"earlier than 3.7");
    XCTAssert([version compare:@"4.0" options:NSNumericSearch] == NSOrderedAscending, @"not earlier than 4.0");
}

- (void)testIsThreadSafe
{
    BOOL isThreadSafe = [FMDBDatabase isSQLiteThreadSafe];
    XCTAssert(isThreadSafe, @"not threadsafe");
}

- (void)testOpenNilPath
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (bar text)"], @"create failed");
    NSString *value = @"baz";
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (bar) values (?)" withArgumentsInArray:@[value]], @"insert failed");
    NSString *retrievedValue = [db stringForQuery:@"select bar from foo"];
    XCTAssert([value compare:retrievedValue] == NSOrderedSame, @"values didn't match");
}

- (void)testOpenZeroLengthPath
{
    FMDBDatabase *db = [[FMDBDatabase alloc] initWithPath:@""];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (bar text)"], @"create failed");
    NSString *value = @"baz";
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (bar) values (?)" withArgumentsInArray:@[value]], @"insert failed");
    NSString *retrievedValue = [db stringForQuery:@"select bar from foo"];
    XCTAssert([value compare:retrievedValue] == NSOrderedSame, @"values didn't match");
}

- (void)testOpenTwice
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    [db open];
    XCTAssert([db open], @"Double open failed");
}

- (void)testInvalid
{
    NSString *documentsPath = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES)[0];
    NSString *path          = [documentsPath stringByAppendingPathComponent:@"nonexistentfolder/test.sqlite"];

    FMDBDatabase *db = [[FMDBDatabase alloc] initWithPath:path];
    XCTAssertFalse([db open], @"open did NOT fail");
}

- (void)testChangingMaxBusyRetryTimeInterval
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");

    NSTimeInterval originalInterval = db.maxBusyRetryTimeInterval;
    NSTimeInterval updatedInterval = originalInterval > 0 ? originalInterval + 1 : 1;
    
    db.maxBusyRetryTimeInterval = updatedInterval;
    NSTimeInterval diff = fabs(db.maxBusyRetryTimeInterval - updatedInterval);
    
    XCTAssert(diff < 1e-5, @"interval should have changed %.1f", diff);
}

- (void)testChangingMaxBusyRetryTimeIntervalDatabaseNotOpened
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    // XCTAssert([db open], @"open failed");   // deliberately not opened

    NSTimeInterval originalInterval = db.maxBusyRetryTimeInterval;
    NSTimeInterval updatedInterval = originalInterval > 0 ? originalInterval + 1 : 1;
    
    db.maxBusyRetryTimeInterval = updatedInterval;
    XCTAssertNotEqual(originalInterval, db.maxBusyRetryTimeInterval, @"interval should not have changed");
}

- (void)testZeroMaxBusyRetryTimeInterval
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    
    NSTimeInterval updatedInterval = 0;
    
    db.maxBusyRetryTimeInterval = updatedInterval;
    XCTAssertEqual(db.maxBusyRetryTimeInterval, updatedInterval, @"busy handler not disabled");
}

- (void)testCloseOpenResultSets
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (bar text)"], @"create failed");
    NSString *value = @"baz";
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (bar) values (?)" withArgumentsInArray:@[value]], @"insert failed");
    FMDBResultSet *rs = [db executeQuery:@"select bar from foo"];
    [db closeOpenResultSets];
    XCTAssertFalse([rs fmdb_next], @"step should have failed");
}

- (void)testGoodConnection
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db goodConnection], @"no good connection");
}

- (void)testBadConnection
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    // XCTAssert([db open], @"open failed");  // deliberately did not open
    XCTAssertFalse([db goodConnection], @"no good connection");
}

- (void)testLastRowId
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (foo_id integer primary key autoincrement, bar text)"], @"create failed");
    
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (bar) values (?)" withArgumentsInArray:@[@"baz"]], @"insert failed");
    sqlite3_int64 firstRowId = [db lastInsertRowId];
    
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (bar) values (?)" withArgumentsInArray:@[@"qux"]], @"insert failed");
    sqlite3_int64 secondRowId = [db lastInsertRowId];
    
    XCTAssertEqual(secondRowId - firstRowId, 1, @"rowid should have incremented");
}

- (void)testChanges
{
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (foo_id integer primary key autoincrement, bar text)"], @"create failed");
    
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (bar) values (?)" withArgumentsInArray:@[@"baz"]], @"insert failed");
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (bar) values (?)" withArgumentsInArray:@[@"qux"]], @"insert failed");
    XCTAssert([db fmdb_executeUpdate:@"update foo set bar = ?" withArgumentsInArray:@[@"xxx"]], @"insert failed");
    int changes = [db changes];
    
    XCTAssertEqual(changes, 2, @"two rows should have incremented \(%ld)", (long)changes);
}

- (void)testBind {
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (id integer primary key autoincrement, a numeric)"], @"create failed");
    
    NSNumber *insertedValue;
    NSNumber *retrievedValue;
    
    insertedValue = [NSNumber numberWithChar:51];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithUnsignedChar:52];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");

    insertedValue = [NSNumber numberWithShort:53];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithUnsignedShort:54];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithInt:54];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithUnsignedInt:55];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithLong:56];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db longForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithUnsignedLong:57];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db longForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithLongLong:56];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db longForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithUnsignedLongLong:57];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db longForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithFloat:58];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db doubleForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
    
    insertedValue = [NSNumber numberWithDouble:59];
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db doubleForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");

    insertedValue = @TRUE;
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (a) values (?)" withArgumentsInArray:@[insertedValue]], @"insert failed");
    retrievedValue = @([db boolForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])]);
    XCTAssertEqualObjects(insertedValue, retrievedValue, @"values don't match");
}

- (void)testFormatStrings {
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (id integer primary key autoincrement, a numeric)"], @"create failed");
    
    BOOL success;
    
    char insertedChar = 'A';
    success = [db executeUpdateWithFormat:@"insert into foo (a) values (%c)", insertedChar];
    XCTAssert(success, @"insert failed");
    const char *retrievedChar = [[db stringForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])] UTF8String];
    XCTAssertEqual(insertedChar, retrievedChar[0], @"values don't match");
    
    const char *insertedString = "baz";
    success = [db executeUpdateWithFormat:@"insert into foo (a) values (%s)", insertedString];
    XCTAssert(success, @"insert failed");
    const char *retrievedString = [[db stringForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])] UTF8String];
    XCTAssert(strcmp(insertedString, retrievedString) == 0, @"values don't match");
    
    int insertedInt = 42;
    success = [db executeUpdateWithFormat:@"insert into foo (a) values (%d)", insertedInt];
    XCTAssert(success, @"insert failed");
    int retrievedInt = [db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])];
    XCTAssertEqual(insertedInt, retrievedInt, @"values don't match");

    char insertedUnsignedInt = 43;
    success = [db executeUpdateWithFormat:@"insert into foo (a) values (%u)", insertedUnsignedInt];
    XCTAssert(success, @"insert failed");
    char retrievedUnsignedInt = [db intForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])];
    XCTAssertEqual(insertedUnsignedInt, retrievedUnsignedInt, @"values don't match");
    
    float insertedFloat = 44;
    success = [db executeUpdateWithFormat:@"insert into foo (a) values (%f)", insertedFloat];
    XCTAssert(success, @"insert failed");
    float retrievedFloat = [db doubleForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])];
    XCTAssertEqual(insertedFloat, retrievedFloat, @"values don't match");
    
    unsigned long long insertedUnsignedLongLong = 45;
    success = [db executeUpdateWithFormat:@"insert into foo (a) values (%llu)", insertedUnsignedLongLong];
    XCTAssert(success, @"insert failed");
    unsigned long long retrievedUnsignedLongLong = [db longForQuery:@"select a from foo where id = ?", @([db lastInsertRowId])];
    XCTAssertEqual(insertedUnsignedLongLong, retrievedUnsignedLongLong, @"values don't match");
}

- (void)testStepError {
    FMDBDatabase *db = [[FMDBDatabase alloc] init];
    XCTAssert([db open], @"open failed");
    XCTAssert([db fmdb_executeUpdate:@"create table foo (id integer primary key)"], @"create failed");
    XCTAssert([db fmdb_executeUpdate:@"insert into foo (id) values (?)" values:@[@1] error:nil], @"create failed");
    
    NSError *error;
    BOOL success = [db fmdb_executeUpdate:@"insert into foo (id) values (?)" values:@[@1] error:&error];
    XCTAssertFalse(success, @"insert of duplicate key should have failed");
    XCTAssertNotNil(error, @"error object should have been generated");
    XCTAssertEqual(error.code, 19, @"error code 19 should have been generated");
}

@end
