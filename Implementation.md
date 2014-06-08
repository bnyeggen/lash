The underlying hash table specifications are: 

- 64-bit murmurhash2 over key bytes for hash codes
- Power-of-two hash table size
- Collision resolution via chaining
- Rehashing in-place, incrementally via a technique known as [linear hashing](http://users.eecs.northwestern.edu/~peters/references/Linearhash80.pdf).  This results in a more-or-less constant load factor, effectively built-in concurrent rehashing, and minimal rehashing pauses.

Relatively "vanilla" linear hashing was chosen instead of its more exotic variants from later on (eg, [linear hashing with partial expansions](http://dl.acm.org/citation.cfm?id=1286912)) for a variety of reasons.  The vanilla version has an advantage of not requiring complicated address calculations, and having relatively straightforward concurrency implications.  Additionally, we trust our hash function to distribute keys randomly enough that we're not concerned with longer chains in the "back" of the map than in the front (under LHPE, the same length chains exist, but distributed throughout the table - the particular location doesn't concern us).  Optimizations involving tracking chain length and rehashing in a priority queue would reduce chain length, but we expect to be dealing with large volumes of data in a context where we'd rather have the heap overhead available for other stuff.  Linear-probing variants have greater disk cache friendliness, but don't play well with variable-size records, have somewhat complex locking schemes, and have pretty complex and duplicative incremental rehashing schemes.

Unlike vanilla linear hashing and its variants, we don't put a large amount of time into consolidating multiple records into a particular block (usually something like a disk sector).  It's difficult to verify definitively, but based on the common architectures of the day (eg, PDP-11 and VAX) it's very likely that read() calls were basically the only practical method for reading data from storage - given mmap, we don't have to put as much effort into getting the maximum throughput per syscall, since the OS manages the actual paging and caching for us.

The locking / CAS scheme to coordinate the linear hashing (in AbstractDiskMap.rehash()) is pretty nifty.
