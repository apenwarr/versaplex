using System;
using System.Collections.Generic;

// An interface to a Schemamatic schema backend.
internal interface ISchemaBackend  
{
    // Update the backing store with all current elements.
    VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, VxPutOpts opts);

    // Get elements from the backing store.
    // If keys is non-empty, only returns the schema for listed keys.
    // If keys is empty, returns all schema elements.
    VxSchema Get(IEnumerable<string> keys);

    // Gets the checksums for all elements from the backing store.
    VxSchemaChecksums GetChecksums();

    // Removes the given element from the schema.
    void DropSchema(string type, string name);
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
    // If set, will make a backup copy instead of overwriting existing data,
    // if possible.
    IsBackup = 0x4,
}

