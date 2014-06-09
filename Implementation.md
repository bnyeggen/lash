The underlying hash table specifications are: 

- 64-bit murmurhash2 over key bytes for hash codes
- Power-of-two hash table size
- Collision resolution via chaining (in a second file)
- Concurrency support via mutex striping by index
- Incremental rehashing in-place, one stripe worth of bucket's at a time (eg, if stripe width is 32, buckets 0, 32, 64, 96...)

This is similar to a technique known as [linear hashing](http://users.eecs.northwestern.edu/~peters/references/Linearhash80.pdf), in that we incrementally rehash buckets to their position or (position + table size).  Instead of doing this one bucket at a time, though, we handle it by stripe.  We can get away with this because with modern filesystems and 64-bit address spaces, expanding a file is fun and easy, and given a file we can do lots of random IO via mmap without a huge performance hit.  This also means that unlike some heavier-weight databases, we don't prioritize chunking records into sectors (which usually correspond to disk sectors) and instead just access everything as directly as possible, relying on the page cache to make it efficient.

If you're running into performance problems, especially with seeming disk thrashing under Linux, consider tuning your vm.dirty_bytes and vm.dirty_background_bytes in your /etc/sysctl.conf.  It makes a dramatic difference, especially if you have memory to spare.  We make no guarantees about uncorruptible durability in the event of a crash anyway, so you might as well let writes mostly hit cache if you can absorb them.
