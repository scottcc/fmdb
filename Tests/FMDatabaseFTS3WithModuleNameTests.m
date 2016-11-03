//
//  FMDatabaseFTS3WithKeyTests.m
//  fmdb
//
//  Created by Stephan Heilner on 1/21/15.
//
//

#import "FMDBTempDBTests.h"
#import "FMDBDatabase+FTS3.h"
#import "FMDBTokenizers.h"

@interface FMDatabaseFTS3WithModuleNameTests : FMDBTempDBTests

@end

static id<FMTokenizerDelegate> g_testTok = nil;

@implementation FMDatabaseFTS3WithModuleNameTests

+ (void)populateDatabase:(FMDBDatabase *)db
{
    [db fmdb_executeUpdate:@"CREATE VIRTUAL TABLE mail USING fts3(subject, body)"];
    
    [db fmdb_executeUpdate:@"INSERT INTO mail VALUES('hello world', 'This message is a hello world message.')"];
    [db fmdb_executeUpdate:@"INSERT INTO mail VALUES('urgent: serious', 'This mail is seen as a more serious mail')"];
    
    // Create a tokenizer instance that will not be de-allocated when the method finishes.
    g_testTok = [[FMDBSimpleTokenizer alloc] initWithLocale:NULL];
    [FMDBDatabase registerTokenizer:g_testTok];
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

- (void)testOffsets
{
    FMDBResultSet *results = [self.db executeQuery:@"SELECT offsets(mail) FROM mail WHERE mail MATCH 'world'"];
    
    if ([results fmdb_next]) {
        FMTextOffsets *offsets = [results offsetsForColumnIndex:0];
        
        [offsets enumerateWithBlock:^(NSInteger columnNumber, NSInteger termNumber, NSRange matchRange) {
            if (columnNumber == 0) {
                XCTAssertEqual(termNumber, 0L);
                XCTAssertEqual(matchRange.location, 6UL);
                XCTAssertEqual(matchRange.length, 5UL);
            } else if (columnNumber == 1) {
                XCTAssertEqual(termNumber, 0L);
                XCTAssertEqual(matchRange.location, 24UL);
                XCTAssertEqual(matchRange.length, 5UL);
            }
        }];
    }
}

- (void)testTokenizer
{
    [self.db installTokenizerModuleWithName:@"TestModuleName"];
    
    BOOL ok = [self.db fmdb_executeUpdate:@"CREATE VIRTUAL TABLE simple_fts USING fts3(tokenize=TestModuleName)"];
    XCTAssertTrue(ok, @"Failed to create virtual table: %@", [self.db fmdb_lastErrorMessage]);
    
    // The FMDBSimpleTokenizer handles non-ASCII characters well, since it's based on CFStringTokenizer.
    NSString *text = @"I like the band Queensrÿche. They are really great.";
    
    ok = [self.db fmdb_executeUpdate:@"INSERT INTO simple_fts VALUES(?)", text];
    XCTAssertTrue(ok, @"Failed to insert data: %@", [self.db fmdb_lastErrorMessage]);
    
    FMDBResultSet *results = [self.db executeQuery:@"SELECT * FROM simple_fts WHERE simple_fts MATCH ?", @"Queensrÿche"];
    XCTAssertTrue([results fmdb_next], @"Failed to find result");
}

@end
