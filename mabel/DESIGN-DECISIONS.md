
## Use of CityHash

CityHash is used for non-cryptographic hashing primarily due to the
speed, MurmurHash was originally used.

## Used of LZ4

LZ4 is used for in-memory compression, particularly by the DICTSET's persistence
capability. LZ4 appears to perform better in a tight loop than zStandard but does
not compress as well. Given the use case is likely to be that a dataset is
larger than memory but is expensive to recollect.

The compression needs to balance compression speed with compression performance,
weighted toward speed - there is no benefit to doing this if it's quicker to save
the results to disk.

Zstandard compresses anout twice as much but in benchmarks was about 1.5x the time
to run.

A large chunk of the time is serialization and deserialization of the data - so
the performance difference between the algo is much greater than that.

## Use of zStandard

zStandard is used for on-disk compression.

zStandard was chosen over LZMA which gives better compression and LZ4 which is faster
to provide balanced performance.

LZMA is orders of magnitude slower and not feasible as a realtime compression
algorithm.

The difference between LZ4 and zStandard is less pronounced when used as part of a
data processing test however LZ4 is faster, but zStandard's compression performance
is nearly double that of LZ4's.

The expected use case is systems where storage is the greatest cost so the algorithm
which reduces storage, even if a little slower, was chosen.

## Use of orjson

## Use of JSON

