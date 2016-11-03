//
//  FMDatabaseQueueTests.m
//  fmdb
//
//  Created by Graham Dennis on 24/11/2013.
//
//

#import <XCTest/XCTest.h>
#import "FMDBDatabaseQueue.h"

#if FMDB_SQLITE_STANDALONE
#import <sqlite3/sqlite3.h>
#else
#import <sqlite3.h>
#endif

@interface FMDatabaseQueueTests : FMDBTempDBTests

@property FMDBDatabaseQueue *queue;

@end

@implementation FMDatabaseQueueTests

+ (void)populateDatabase:(FMDBDatabase *)db
{
    [db fmdb_executeUpdate:@"create table easy (a text)"];
    
    [db fmdb_executeUpdate:@"create table qfoo (foo text)"];
    [db fmdb_executeUpdate:@"insert into qfoo values ('hi')"];
    [db fmdb_executeUpdate:@"insert into qfoo values ('hello')"];
    [db fmdb_executeUpdate:@"insert into qfoo values ('not')"];
}

- (void)setUp
{
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
    
    self.queue = [FMDBDatabaseQueue databaseQueueWithPath:self.databasePath];
}

- (void)tearDown
{
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testQueueSelect
{
    [self.queue inDatabase:^(FMDBDatabase *adb) {
        int count = 0;
        FMDBResultSet *rsl = [adb executeQuery:@"select * from qfoo where foo like 'h%'"];
        while ([rsl fmdb_next]) {
            count++;
        }
        
        XCTAssertEqual(count, 2);
        
        count = 0;
        rsl = [adb executeQuery:@"select * from qfoo where foo like ?", @"h%"];
        while ([rsl fmdb_next]) {
            count++;
        }
        
        XCTAssertEqual(count, 2);
    }];
}

- (void)testReadOnlyQueue
{
    FMDBDatabaseQueue *queue2 = [FMDBDatabaseQueue databaseQueueWithPath:self.databasePath flags:SQLITE_OPEN_READONLY];
    XCTAssertNotNil(queue2);

    {
        [queue2 inDatabase:^(FMDBDatabase *db2) {
            FMDBResultSet *rs1 = [db2 executeQuery:@"SELECT * FROM qfoo"];
            XCTAssertNotNil(rs1);

            [rs1 close];
            
            XCTAssertFalse(([db2 fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:3]]), @"Insert should fail because this is a read-only database");
        }];
        
        [queue2 close];
        
        // Check that when we re-open the database, it's still read-only
        [queue2 inDatabase:^(FMDBDatabase *db2) {
            FMDBResultSet *rs1 = [db2 executeQuery:@"SELECT * FROM qfoo"];
            XCTAssertNotNil(rs1);
            
            [rs1 close];
            
            XCTAssertFalse(([db2 fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:3]]), @"Insert should fail because this is a read-only database");
        }];
    }
}

- (void)testStressTest
{
    size_t ops = 16;
    
    dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    
    dispatch_apply(ops, dqueue, ^(size_t nby) {
        
        // just mix things up a bit for demonstration purposes.
        if (nby % 2 == 1) {
            [NSThread sleepForTimeInterval:.01];
            
            [self.queue inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
                FMDBResultSet *rsl = [adb executeQuery:@"select * from qfoo where foo like 'h%'"];
                while ([rsl fmdb_next]) {
                    ;// whatever.
                }
            }];
            
        }
        
        if (nby % 3 == 1) {
            [NSThread sleepForTimeInterval:.01];
        }
        
        [self.queue inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
            XCTAssertTrue([adb fmdb_executeUpdate:@"insert into qfoo values ('1')"]);
            XCTAssertTrue([adb fmdb_executeUpdate:@"insert into qfoo values ('2')"]);
            XCTAssertTrue([adb fmdb_executeUpdate:@"insert into qfoo values ('3')"]);
        }];
    });
    
    [self.queue close];
    
    [self.queue inDatabase:^(FMDBDatabase *adb) {
        XCTAssertTrue([adb fmdb_executeUpdate:@"insert into qfoo values ('1')"]);
    }];
}

- (void)testTransaction
{
    [self.queue inDatabase:^(FMDBDatabase *adb) {
        [adb fmdb_executeUpdate:@"create table transtest (a integer)"];
        XCTAssertTrue([adb fmdb_executeUpdate:@"insert into transtest values (1)"]);
        XCTAssertTrue([adb fmdb_executeUpdate:@"insert into transtest values (2)"]);
        
        int rowCount = 0;
        FMDBResultSet *ars = [adb executeQuery:@"select * from transtest"];
        while ([ars fmdb_next]) {
            rowCount++;
        }
        
        XCTAssertEqual(rowCount, 2);
    }];
    
    [self.queue inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
        XCTAssertTrue([adb fmdb_executeUpdate:@"insert into transtest values (3)"]);
        
        if (YES) {
            // uh oh!, something went wrong (not really, this is just a test
            *rollback = YES;
            return;
        }
        
        XCTFail(@"This shouldn't be reached");
    }];
    
    [self.queue inDatabase:^(FMDBDatabase *adb) {
        
        int rowCount = 0;
        FMDBResultSet *ars = [adb executeQuery:@"select * from transtest"];
        while ([ars fmdb_next]) {
            rowCount++;
        }
        
        XCTAssertFalse([adb hasOpenResultSets]);
        
        XCTAssertEqual(rowCount, 2);
    }];

}

@end
