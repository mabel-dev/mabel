
// implemented queries
SELECT ...
ANALYZE dataset -> creates and/or returns profile information for a dataset
PLAN query -> returns the optimized plan for a query
DESCRIBE dataset -> creates and/or returns schema information for a dataset
CREATE INDEX index_name ON dataset (attribute1, attribute2, ...) -> creates an index



// sql parser -> relational algebra

// relational algebra -> graph (query planning)
- union the partitions

// query optimizer
Define static rules that transform logical operators to a physical plan.
→ Perform most restrictive selection early
→ Perform all selections before joins
→ Predicate/Limit/Projection pushdowns
→ Join ordering based on cardinality
