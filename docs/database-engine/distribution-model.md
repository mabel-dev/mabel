
// co-ordinator

- Parse
- Plan
- Optimize
- Schedule -> [workers] -> Assemble

// worker

- Read (PartitionScan, DatasetScan, Statistics) <- depending on the count of records and what's available
- Project (remove unwanted columns, do renames)
- Select (filter the records based on predicate)
- Evaluate (with functions)
- CrossJoin (with selector)
- Aggregate (with functions)