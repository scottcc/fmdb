//
//  FMDatabasePoolTests.m
//  fmdb
//
//  Created by Graham Dennis on 24/11/2013.
//
//

#import <XCTest/XCTest.h>

@interface FMDatabasePoolTests : FMDBTempDBTests

@property FMDBDatabasePool *pool;

@end

@implementation FMDatabasePoolTests

+ (void)populateDatabase:(FMDBDatabase *)db
{
    [db fmdb_executeUpdate:@"create table easy (a text)"];
    [db fmdb_executeUpdate:@"create table easy2 (a text)"];

    [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1001]];
    [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1002]];
    [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1003]];

    [db fmdb_executeUpdate:@"create table likefoo (foo text)"];
    [db fmdb_executeUpdate:@"insert into likefoo values ('hi')"];
    [db fmdb_executeUpdate:@"insert into likefoo values ('hello')"];
    [db fmdb_executeUpdate:@"insert into likefoo values ('not')"];
}

- (void)setUp
{
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
    
    [self setPool:[FMDBDatabasePool databasePoolWithPath:self.databasePath]];
    
    [[self pool] setDelegate:self];
    
}

- (void)tearDown
{
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testPoolIsInitiallyEmpty
{
    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)0, @"Pool should be empty on creation");
}

- (void)testDatabaseCreation
{
    __block FMDBDatabase *db1;
    
    [self.pool inDatabase:^(FMDBDatabase *db) {
        
        XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)1, @"Should only have one database at this point");
        
        db1 = db;
        
    }];
    
    [self.pool inDatabase:^(FMDBDatabase *db) {
        XCTAssertEqualObjects(db, db1, @"We should get the same database back because there was no need to create a new one");
        
        [self.pool inDatabase:^(FMDBDatabase *db2) {
            XCTAssertNotEqualObjects(db2, db, @"We should get a different database because the first was in use.");
        }];
        
    }];
    
    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)2);
    
    [self.pool releaseAllDatabases];

    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)0, @"We should be back to zero databases again");
}

- (void)testCheckedInCheckoutOutCount
{
    [self.pool inDatabase:^(FMDBDatabase *aDb) {
        
        XCTAssertEqual([self.pool countOfCheckedInDatabases],   (NSUInteger)0);
        XCTAssertEqual([self.pool countOfCheckedOutDatabases],  (NSUInteger)1);
        
        XCTAssertTrue(([aDb fmdb_executeUpdate:@"insert into easy (a) values (?)", @"hi"]));
        
        // just for fun.
        FMDBResultSet *rs = [aDb executeQuery:@"select * from easy"];
        XCTAssertNotNil(rs);
        XCTAssertTrue([rs fmdb_next]);
        while ([rs fmdb_next]) { ; } // whatevers.
        
        XCTAssertEqual([self.pool countOfOpenDatabases],        (NSUInteger)1);
        XCTAssertEqual([self.pool countOfCheckedInDatabases],   (NSUInteger)0);
        XCTAssertEqual([self.pool countOfCheckedOutDatabases],  (NSUInteger)1);
    }];
    
    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)1);
}

- (void)testMaximumDatabaseLimit
{
    [self.pool setMaximumNumberOfDatabasesToCreate:2];
    
    [self.pool inDatabase:^(FMDBDatabase *db) {
        [self.pool inDatabase:^(FMDBDatabase *db2) {
            [self.pool inDatabase:^(FMDBDatabase *db3) {
                XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)2);
                XCTAssertNil(db3, @"The third database must be nil because we have a maximum of 2 databases in the pool");
            }];
            
        }];
    }];
}

- (void)testTransaction
{
    [self.pool inTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1001]];
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1002]];
        [adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1003]];
        
        XCTAssertEqual([self.pool countOfOpenDatabases],        (NSUInteger)1);
        XCTAssertEqual([self.pool countOfCheckedInDatabases],   (NSUInteger)0);
        XCTAssertEqual([self.pool countOfCheckedOutDatabases],  (NSUInteger)1);
    }];

    XCTAssertEqual([self.pool countOfOpenDatabases],        (NSUInteger)1);
    XCTAssertEqual([self.pool countOfCheckedInDatabases],   (NSUInteger)1);
    XCTAssertEqual([self.pool countOfCheckedOutDatabases],  (NSUInteger)0);
}

- (void)testSelect
{
    [self.pool inDatabase:^(FMDBDatabase *db) {
        FMDBResultSet *rs = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1001]];
        XCTAssertNotNil(rs);
        XCTAssertTrue ([rs fmdb_next]);
        XCTAssertFalse([rs fmdb_next]);
    }];
}

- (void)testTransactionRollback
{
    [self.pool inDeferredTransaction:^(FMDBDatabase *adb, BOOL *rollback) {
        XCTAssertTrue(([adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1004]]));
        XCTAssertTrue(([adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1005]]));
        XCTAssertTrue([[adb executeQuery:@"select * from easy where a == '1004'"] fmdb_next], @"1004 should be in database");
        
        *rollback = YES;
    }];
    
    [self.pool inDatabase:^(FMDBDatabase *db) {
        XCTAssertFalse([[db executeQuery:@"select * from easy where a == '1004'"] fmdb_next], @"1004 should not be in database");
    }];

    XCTAssertEqual([self.pool countOfOpenDatabases],        (NSUInteger)1);
    XCTAssertEqual([self.pool countOfCheckedInDatabases],   (NSUInteger)1);
    XCTAssertEqual([self.pool countOfCheckedOutDatabases],  (NSUInteger)0);
}

- (void)testSavepoint
{
    NSError *err = [self.pool inSavePoint:^(FMDBDatabase *db, BOOL *rollback) {
        [db fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1006]];
    }];
    
    XCTAssertNil(err);
}

- (void)testNestedSavepointRollback
{
    NSError *err = [self.pool inSavePoint:^(FMDBDatabase *adb, BOOL *rollback) {
        XCTAssertFalse([adb hadError]);
        XCTAssertTrue(([adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1009]]));
        
        [adb inSavePoint:^(BOOL *arollback) {
            XCTAssertTrue(([adb fmdb_executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1010]]));
            *arollback = YES;
        }];
    }];
    
    
    XCTAssertNil(err);
    
    [self.pool inDatabase:^(FMDBDatabase *db) {
        FMDBResultSet *rs = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1009]];
        XCTAssertTrue ([rs fmdb_next]);
        XCTAssertFalse([rs fmdb_next]); // close it out.
        
        rs = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1010]];
        XCTAssertFalse([rs fmdb_next]);
    }];
}

- (void)testLikeStringQuery
{
    [self.pool inDatabase:^(FMDBDatabase *db) {
        int count = 0;
        FMDBResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
        while ([rsl fmdb_next]) {
            count++;
        }
        
        XCTAssertEqual(count, 2);
        
        count = 0;
        rsl = [db executeQuery:@"select * from likefoo where foo like ?", @"h%"];
        while ([rsl fmdb_next]) {
            count++;
        }
        
        XCTAssertEqual(count, 2);
        
    }];
}

- (void)testStressTest
{
    size_t ops = 128;
    
    dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    
    dispatch_apply(ops, dqueue, ^(size_t nby) {
        
        // just mix things up a bit for demonstration purposes.
        if (nby % 2 == 1) {
            
            [NSThread sleepForTimeInterval:.001];
        }
        
        [self.pool inDatabase:^(FMDBDatabase *db) {
            FMDBResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
            XCTAssertNotNil(rsl);
            int i = 0;
            while ([rsl fmdb_next]) {
                i++;
                if (nby % 3 == 1) {
                    [NSThread sleepForTimeInterval:.0005];
                }
            }
            XCTAssertEqual(i, 2);
        }];
    });
    
    XCTAssert([self.pool countOfOpenDatabases] < 64, @"There should be significantly less than 64 databases after that stress test");
}


- (BOOL)databasePool:(FMDBDatabasePool*)pool shouldAddDatabaseToPool:(FMDBDatabase*)database {
    [database setMaxBusyRetryTimeInterval:10];
    // [database setCrashOnErrors:YES];
    return YES;
}

- (void)testReadWriteStressTest
{
    int ops = 16;
    
    dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    
    dispatch_apply(ops, dqueue, ^(size_t nby) {
        
        // just mix things up a bit for demonstration purposes.
        if (nby % 2 == 1) {
            [NSThread sleepForTimeInterval:.01];
            
            [self.pool inTransaction:^(FMDBDatabase *db, BOOL *rollback) {
                FMDBResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
                XCTAssertNotNil(rsl);
                while ([rsl fmdb_next]) {
                    ;// whatever.
                }
                
            }];
            
        }
        
        if (nby % 3 == 1) {
            [NSThread sleepForTimeInterval:.01];
        }
        
        [self.pool inTransaction:^(FMDBDatabase *db, BOOL *rollback) {
            XCTAssertTrue([db fmdb_executeUpdate:@"insert into likefoo values ('1')"]);
            XCTAssertTrue([db fmdb_executeUpdate:@"insert into likefoo values ('2')"]);
            XCTAssertTrue([db fmdb_executeUpdate:@"insert into likefoo values ('3')"]);
        }];
    });
    
    [self.pool releaseAllDatabases];
    
    [self.pool inDatabase:^(FMDBDatabase *db) {
        XCTAssertTrue([db fmdb_executeUpdate:@"insert into likefoo values ('1')"]);
    }];
}

@end
