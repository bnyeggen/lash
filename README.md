lash is a disk-backed hashmap for Java.  It prioritizes:

- The ability to handle large amounts of data
- Performance, especially for high volumes of random insertions, when a B-tree would suffer degraded performance
- Concurrency

This is a hashmap, not a full-on durable database.  Journaling will not be implemented, which means that if your machine loses power it is highly likely your map will be corrupted.  However, if you are able to successfully close() your hashmap, you will be able to reload the map.

For performance reasons, lash uses native byte order for storage of metadata like record lengths and hash values.  This means that if you plan on shipping lash maps across machines, you should verify they have the same native byte order.  This is a work in progress; there are no guarantees about format compatibility between release versions, or within the same snapshot version.

lash uses mmap() heavily for storage access, which means that insertion performance is highly dependent on your system's tuning parameters (eg, sysctl's vm.dirty_background_ratio and vm.dirty_ratio).  It lurves RAM, but memory usage is managed by the operating system via the page cache and not the JVM.  For this reason it has excellent garbage collection performance and is nice for memory-constrained environments.  The random access inherent in hashmaps also means that you will be throwing a lot of random IOPs - SSDs are helpful.

See the source for implementation details.

ROADMAP:

- More tests!
- Support for fixed-length records and fixed-key, variable-value records.  Refactor the Record & WritethruRecord classes to support them.
- API for record -> record pointers to support multiple indexes over a primary map
- Make more configurable, specifically for:
  - Lock striping
  - Load targets
  - Rehashing scheme; r/w vs. mutex locks
- Write caching improvements (seems to be particularly necessary w/ default Linux FS settings)
- Test avoiding some locks via volatile read/writes, a la ConcurrentHashMap
