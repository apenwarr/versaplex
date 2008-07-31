using System.Collections.Generic;

// An interface to a Schemamatic schema backend.
internal interface ISchemaBackend  
{
    // Update the backing store with all current elements.
    VxSchemaErrors Put(VxSchema schema, VxSchemaChecksums sums, 
        VxPutSchemaOpts opts);

    // Get elements from the backing store.
    // If keys is non-empty, only returns the schema for listed keys.
    // If keys is empty, returns all schema elements.
    VxSchema Get(IEnumerable<string> keys);

    // Gets the checksums for all elements from the backing store.
    VxSchemaChecksums GetChecksums();
}

