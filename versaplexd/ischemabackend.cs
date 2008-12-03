using System;
using System.Collections.Generic;

// An interface to a Schemamatic schema backend.
internal interface ISchemaBackend : IDisposable
{
    // Update the backing store with all current elements.
    // If an element's text is empty, it will be deleted.
    VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, VxPutOpts opts);

    // Get elements from the backing store.
    // If keys is non-empty, only returns the schema for listed keys.
    // If keys is empty, returns all schema elements.
    VxSchema Get(IEnumerable<string> keys);

    // Gets the checksums for all elements from the backing store.
    VxSchemaChecksums GetChecksums();

    // Removes the given elements from the schema.
    VxSchemaErrors DropSchema(IEnumerable<string> keys);

    // Returns a blob of text that can be used with PutSchemaData to fill 
    // the given table.
    // "seqnum" provides a hint about the priority of the table when batch
    // processing, and is used to locate the file on disk.
    // "where" is the body of a SQL "WHERE" clause, to limit the data 
    // returned by the database, if applicable.
    // "replaces" is the list of replacements to be made on a field
    // "skipfields" is the list of fields to skip during export
    string GetSchemaData(string tablename, int seqnum, string where,
                         Dictionary<string,string> replaces, List<string> skipfields);

    // Delete all rows from the given table and replace them with the given
    // data.  text is an opaque hunk of text returned from GetSchemaData.
    // Seqnum provides a hint about the priority of the table when batch
    // processing, and is used to locate the file on disk.
    void PutSchemaData(string tablename, string text, int seqnum);
}

[Flags]
public enum VxPutOpts : int
{
    None = 0,
    // If set, PutSchema will do potentially destructive things like
    // dropping a table in order to re-add it.
    Destructive = 0x1,
    // If set, PutSchema will not attempt to do any retries.
    NoRetry = 0x2,
    // If set and the element already exists, will create the new element
    // with a different name that doesn't already exist.
    // Currently only applies to the disk backend; other backends ignore this.
    IsBackup = 0x4,
}

