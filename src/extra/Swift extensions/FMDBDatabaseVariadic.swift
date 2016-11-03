//
//  FMDatabaseVariadic.swift
//  FMDB
//


//  This extension inspired by http://stackoverflow.com/a/24187932/1271826

import Foundation

extension FMDBDatabase {
    
    /// This is a rendition of executeQuery that handles Swift variadic parameters
    /// for the values to be bound to the ? placeholders in the SQL.
    ///
    /// This throws any error that occurs.
    ///
    /// - parameter sql:     The SQL statement to be used.
    /// - parameter values:  The values to be bound to the ? placeholders
    ///
    /// - returns:           This returns FMDBResultSet if successful. If unsuccessful, it throws an error.
    
    func executeQuery(sql:String, _ values: AnyObject...) throws -> FMDBResultSet {
        return try executeQuery(sql, values: values as [AnyObject]);
    }
    
    /// This is a rendition of fmdb_executeUpdate that handles Swift variadic parameters
    /// for the values to be bound to the ? placeholders in the SQL.
    ///
    /// This throws any error that occurs.
    ///
    /// - parameter sql:     The SQL statement to be used.
    /// - parameter values:  The values to be bound to the ? placeholders
    
    func fmdb_executeUpdate(sql:String, _ values: AnyObject...) throws {
        try fmdb_executeUpdate(sql, values: values as [AnyObject]);
    }
}
