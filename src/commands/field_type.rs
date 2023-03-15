// FieldType signifies the database operation error codes.
// The positive numbers align with the server side file proto.h.

#[derive(Clone, Copy, Debug)]
pub enum FieldType {
    Namespace = 0,
    Table = 1,
    Key = 2,
    // BIN  = 3,
    DigestRipe = 4,
    // GUID  = 5,
    // DigestRipeArray = 6,
    TranId = 7, // user supplied transaction id, which is simply passed back,
    // ScanOptions = 8,
    ScanTimeout = 9,
    PidArray = 11,
    // IndexName = 21,
    // IndexRange = 22,
    // IndexFilter = 23,
    // IndexLimit = 24,
    // IndexOrderBy = 25,
    // IndexType = 26,
    // UdfPackageName = 30,
    // UdfFunction = 31,
    // UdfArgList = 32,
    // UdfOp = 33,
    // QueryBinList = 40,
    BatchIndex = 41,
    BatchIndexWithSet = 42,
    // FilterExp = 43,
}
