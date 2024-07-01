# architect 0.1.2.4300

## Updates

- `join_mapper()`:
   - Renamed from `join.mapper`: an alias of `join.mapper` is provided and points to `join_mapper`
   - Introduced inverse-weighting on columns based on unique counts per dataset
   - The output of a call to `join_mapper()` is now a slotted class as opposed to a `data.table` object with additional attributes provided. This makes referencing the map more reliable from a programming perspective.
   - Removed support for `DBOE` objects to be passed to argument `env`: this was too specialized of a use case.
- `join_reduce()`:
   - Renamed from `join.reduce`: an alias of `join.reduce` is provided and points to `join_reduce`
   - Replaced the join execution to be more procedural and to use cosine-similarity on column weights across datasets
   
# architect 0.1.2.4200

## Bug Fixes

- `define`: Corrected class check code for argument `blueprint`.

## Documentation 

- `define`: Added clarification of how argument `blueprint` is handled when combined with other operation expressions in argument `...`.

# architect 0.1.2.4100, 0.1.2.4110

## Bug Fixes

- `define`: Corrected a syntax error in the operation processing function.

# architect 0.1.2.4

## General Updates

- Minor code readability changes.
