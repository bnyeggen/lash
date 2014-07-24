package com.nyeggen.lash;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.nyeggen.lash.util.MMapper;

public abstract class AbstractDiskMap implements Closeable {
	/**Number of lock stripes. Always a power of 2*/
	static final int nLocks = 256;
	/**We attempt to keep load below this value.*/
	static final double loadRehashThreshold = 0.75;
	//28 -> 268,435,456; equivalent to 33,554,432 longs
	static final long defaultFileLength = 1L << 28;
	static final String primaryFileName = "primary.hash";
	static final String secondaryFileName = "secondary.hash";
	
	final MMapper primaryMapper, secondaryMapper;
	final String baseFolderLoc;

	/**Allocations in secondary increment from this point*/
	final AtomicLong secondaryWritePos = new AtomicLong(0);
	/**Enforces exclusive access to the secondary in the event of a reallocation.*/
	final ReentrantReadWriteLock secondaryLock = new ReentrantReadWriteLock();
	
	final Object[] locks = new Object[nLocks];
	{ for(int i=0;i<nLocks;i++) locks[i] = new Object(); }
	
	/**Number of records inserted.*/
	final AtomicLong size = new AtomicLong(0);
	/**Number of buckets in the table, always a power of 2.*/
	long tableLength;
	
	//Denotes the index of the next stripe to be rehashed
	final AtomicLong rehashComplete = new AtomicLong(0);
	
	public AbstractDiskMap(String baseFolderLoc, long primaryFileLen){
		try {
			final File baseFolder = new File(baseFolderLoc);
			baseFolder.mkdirs();
			this.baseFolderLoc = baseFolder.getCanonicalPath();
			
			final String primaryLoc = this.baseFolderLoc + File.separator + primaryFileName;
			final String secondaryLoc = this.baseFolderLoc + File.separator + secondaryFileName;
			final File primFile = new File(primaryLoc);
			final File secFile = new File(secondaryLoc);
			final long primFileLen = Math.max(primaryFileLen, primFile.length());
			final long secFileLen = Math.max(defaultFileLength, secFile.length());
			
			primaryMapper = new MMapper(primaryLoc, primFileLen);
			secondaryMapper = new MMapper(secondaryLoc, secFileLen);
			readHeader();
			primaryMapper.doubleLength();
		} catch (Exception e){
			throw new RuntimeException(e);
		}
	}
		
	/**Should only be called in constructor. Loads table metadata from the secondary, or
	 * initializes to default state if metadata is blank.
	 * Attempting to read a header from a different subclass of DiskMap will cause
	 * undefined behavior, corruption, and bad times.*/
	protected abstract void readHeader();

	protected long getHeaderSize() { return 32; }

	/**Embeds table metadata in the secondary to enable persistent tables.
	 * Typically called via close() method.*/
	protected void writeHeader(){
		secondaryLock.writeLock().lock();
		try {
			secondaryMapper.putLong(0, size());
			secondaryMapper.putLong(8, tableLength);
			secondaryMapper.putLong(16, secondaryWritePos.get());
			secondaryMapper.putLong(24, rehashComplete.get());
		} finally {
			secondaryLock.writeLock().lock();
		}
	}
	

	
	protected static long nextPowerOf2(long i){
		if(i < (1<<28)) return (1<<28);
		if((i & (i-1))==0) return i;
		return (1 << (64 - (Long.numberOfLeadingZeros(i))));
	}

	
	protected Object lockForHash(long hash){
		return locks[(int)(hash & (nLocks - 1))];	
	}
	
	//This doesn't lock - because it depends on tableLength, callers should
	//establish some lock that precludes a full rehash (read or write lock on
	//any of the locks).
	protected long idxForHash(long hash){
		return (hash & (nLocks - 1)) < rehashComplete.get()
				? hash & (tableLength + tableLength - 1) 
				: hash & (tableLength - 1);
	}
	
	//Recursively locks all available locks
	protected void completeExpansion(int idx){
		if(idx == nLocks){
			try {
				primaryMapper.doubleLength();
				rehashComplete.set(0);
				tableLength *= 2;
			} catch(Exception e){
				throw new RuntimeException(e);
			}
		} else {
			synchronized(locks[idx]){
				completeExpansion(idx+1);
			}
		}
	}
	
	/**Perform incremental rehashing to keep the load under the threshold.*/
	protected void rehash(){
		while(load() > loadRehashThreshold) {
			//If we've completed all rehashing, we need to expand the table & reset
			//the counters.
			if(rehashComplete.compareAndSet(nLocks, nLocks+1)){
				completeExpansion(0);
				return;
			}
			
			//Otherwise, we attempt to grab the next index to process
			long stripeToRehash;
			while(true){
				stripeToRehash = rehashComplete.getAndIncrement();
				//If it's in the valid table range, we conceptually acquired a valid ticket
				if(stripeToRehash < nLocks) break;
				//Otherwise we're in the middle of a reset - spin until it has completed.
				while(rehashComplete.get() >= nLocks) {
					Thread.yield();
					if(load() < loadRehashThreshold) return;
				}
			}
			//We now have a valid ticket - we rehash all the indexes in the given stripe
			synchronized(lockForHash(stripeToRehash)){
				for(long idx = stripeToRehash; idx < tableLength; idx+=nLocks){
					rehashIdx(idx);
				}
			}
		}		
	}
	
	/**Allocates the given amount of space in secondary storage, and returns a
	 * pointer to it.  Expands secondary storage if necessary.*/
	protected long allocateSecondary(long size){
		secondaryLock.readLock().lock();
		try {
			while(true){
				final long out = secondaryWritePos.get();
				final long newSecondaryPos = out + size;
				if(newSecondaryPos >= secondaryMapper.size()){
					//Goes to reallocation section
					break;
				} else {
					if(secondaryWritePos.compareAndSet(out, newSecondaryPos)) return out;
				}
			}
		} finally {
			secondaryLock.readLock().unlock();
		}
		
		secondaryLock.writeLock().lock();
		try {
			if(secondaryWritePos.get() + size >= secondaryMapper.size()) 
				secondaryMapper.doubleLength();
		} catch(Exception e){
			throw new RuntimeException(e);
		} finally {
			secondaryLock.writeLock().unlock();
		}
		return allocateSecondary(size);
	}
	
	/**Because all records in a bucket hash to their position or position + tableLength,
	 * we can incrementally rehash one bucket at a time.
	 * This does not need to acquire a lock; the calling rehash() method handles it.*/
	protected abstract void rehashIdx(long idx);
	
	private void clear(int i){
		if(i==nLocks){
			this.secondaryLock.writeLock().lock();
			try {
				this.primaryMapper.clear();
				this.secondaryWritePos.set(getHeaderSize());
				this.size.set(0);
				this.rehashComplete.set(0);
			} finally {
				this.secondaryLock.writeLock().unlock();
			}
		} else {
			synchronized (locks[i]) {
				clear(i+1);
			}
		}
	}
	/**Removes all entries from the map.*/
	public void clear(){ clear(0); }
	
	/**Writes all header metadata and unmaps the backing mmap'd files.*/
	@Override
	public void close() throws IOException {
		writeHeader();
		primaryMapper.close();
		secondaryMapper.close();
	}
	
	/**Removes the associated data files, and base folder if it is empty.*/
	public void delete() throws IOException {
		primaryMapper.close();
		secondaryMapper.close();
		
		new File(primaryMapper.getLocation()).delete();
		new File(secondaryMapper.getLocation()).delete();
		new File(this.baseFolderLoc).delete();
	}
	
	/**Number of inserted records.  O(1).*/
	public long size(){
		return size.get();
	}
	
	/**Average number of records per bucket. O(1).*/
	public double load(){
		return size.doubleValue() / (tableLength + (tableLength/nLocks)*rehashComplete.get());
	}
	
	/**Returns the value corresponding to the given key, or null if it is not
	 * present.  Zero-width values (ie, a hash set) are supported.*/
	public abstract byte[] get(byte[] k);
	/**Inserts the given record in the map, and returns the previous value associated
	 * with the given key, or null if there was none.*/
	public abstract byte[] put(byte[] k, byte[] v);
	/**Inserts the given record in the map, only if there was no previous value associated
	 * with the key.  Returns null in the case of a successful insertion, or the value
	 * previously (and currently) associated with the map.*/
	public abstract byte[] putIfAbsent(byte[] k, byte[] v);
	/**Remove the record associated with the given key, returning the previous value, or
	 * null if there was none.*/
	public abstract byte[] remove(byte[] k);
	
	public abstract boolean remove(byte[] k, byte[] v);
	public abstract byte[] replace(byte[] k, byte[] v);
	public abstract boolean replace(byte[] k, byte[] prevVal, byte[] newVal);
	public boolean containsKey(byte[] k){
		return get(k) != null;
	};
	public abstract Iterator<Map.Entry<byte[],byte[]>> iterator();
}
