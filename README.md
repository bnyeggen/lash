lash is a disk-backed hashmap for Java.  It prioritizes:

- The ability to handle large amounts of data
- Performance, especially for high volumes of random insertions, when a B-tree would suffer degraded performance
- Concurrency

This is a hashmap, not a full-on durable database.  Journaling will not be implemented, which means that if your machine loses power it is highly likely your map will be corrupted.  However, if you are able to successfully close() your hashmap, you will be able to reload the map.

For performance reasons, lash uses native byte order for storage of metadata like record lengths and hash values.  This means that if you plan on shipping lash maps across machines, you should verify they have the same native byte order.  This is a work in progress; there are no guarantees about format compatibility between release versions, or within the same snapshot version.

lash uses mmap() heavily for storage access, which means that insertion performance is highly dependent on your system's tuning parameters (eg, sysctl's vm.dirty_background_ratio and vm.dirty_ratio).  It lurves RAM, but memory usage is managed by the operating system via the page cache and not the JVM.  For this reason it has excellent garbage collection performance and is nice for memory-constrained environments.  The random access inherent in hashmaps also means that you will be throwing a lot of random IOPs - SSDs are helpful.

See the source for implementation details.  In general, two implementations are provided, and both can be wrapped in a DiskMap<K,V> that satisfies java.util.ConcurrentMap.

# VarSizeDiskMap
This implementation stores chains of variable-sized records in a secondary file, with an array of pointers to the heads of chains stored in the primary file.  This is basically ye olde standard chained hash table, but with an exotic rehash strategy (see below) and backed by two mmap'd files.

# BucketDiskMap
This implementation groups pointers to variable-sized key/value pairs into "buckets", which are roughly page-table sized (4096 bytes - enough for 170 record pointers per bucket).  When a bucket reaches capacity, we chain to a new bucket allocated in a secondary file.  That secondary file is also where we store the underlying data.  Within a bucket, we treat it as a mini hash table, using the top N bits of the record's hash for placement within the bucket (and the bottom N bits to choose the bucket itself).  In general this should be faster than VarSizeDiskMap and should generally be preferred.

# Commonalities
- Instead of rehashing all at once, like a traditional in-memory hash table, we incrementally rehash one "stripe" (every Nth record / bucket) at a time.  This is a similar idea to [linear hashing](http://202.120.223.158/Download/119b1d2b-1b2a-49ae-8597-2ff17bb529b4.pdf).  Sparse files and mmap make this much easier than it was in the 80s.  The advantage of this is lower worst-case latency than a full rehash (particularly for BucketDiskMap) and the ability to access records in other stripes concurrently with a rehash.
- The size of the underlying table is a power of two, so for a size T, a record will rehash to its present position, or the position + T.  This is unavoidable if we want the ability incrementally rehash, but does mean that hash collisions in the bottom N bits, where N > log2 (table size), will continue colliding after rehash.  This is relatively unlikely; we use a pretty good hash function (murmurhash3).
- Locks are also on a per-stripe basis, and enforce strict mututal exclusion (even for multiple readers).  Because we're using off-heap data structures, we can't depend on the tricks that eg ConcurrentHashMap uses to avoid blocking readers, and read/write locks are slower in a medium-contention scenario than synchronizing.

We're not presently on Maven Central.  If you want to use lash, simply 
`git clone https://github.com/bnyeggen/lash.git && cd lash && mvn install`.
