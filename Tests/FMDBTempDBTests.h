//
//  FMDBTempDBTests.h
//  fmdb
//
//  Created by Graham Dennis on 24/11/2013.
//
//

#import <XCTest/XCTest.h>
#import "FMDBDatabase.h"

@protocol FMDBTempDBTests <NSObject>

@optional
+ (void)populateDatabase:(FMDBDatabase *)database;

@end

@interface FMDBTempDBTests : XCTestCase <FMDBTempDBTests>

@property FMDBDatabase *db;
@property (readonly) NSString *databasePath;

@end
