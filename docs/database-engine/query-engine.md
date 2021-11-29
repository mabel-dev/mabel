
// sql parser -> relational algebra

// relational algebra -> graph (query planning)
- union the partitions

// query optimizer
Define static rules that transform logical operators to a physical plan.
→ Perform most restrictive selection early
→ Perform all selections before joins
→ Predicate/Limit/Projection pushdowns
→ Join ordering based on cardinality