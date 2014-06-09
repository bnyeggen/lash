package com.nyeggen.lash;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.nyeggen.lash.bucket.WritethruRecord;
import com.nyeggen.lash.util.MMapper;

public abstract class AbstractDiskMap implements Closeable {
	/**Number of lock stripes. Always a power of 2*/
	static final int nLocks = 256;
	/**We attempt to keep load below this value.*/
	static final double loadRehashThreshold = 0.75;
	//28 -> 268,435,456; equivalent to 33,554,432 longs
	static final long defaultFileLength = 1L << 28;
	
	final MMapper primaryMapper, secondaryMapper;
	final String baseFolderLoc;

	/**Allocations in secondary increment from this point*/
	AtomicLong secondaryWritePos;
	/**Enforces exclusive access to the secondary in the event of a reallocation.*/
	final ReentrantReadWriteLock secondaryLock = new ReentrantReadWriteLock();
	
	final Object[] locks = new Object[nLocks];
	{ for(int i=0;i<nLocks;i++) locks[i] = new Object(); }
	
	/**Number of records inserted.*/
	final AtomicLong size = new AtomicLong(0);
	/**Number of buckets in the table, always a power of 2.*/
	long tableLength;
	
	final AtomicLong rehashCompleteIdx = new AtomicLong(-1);
	final AtomicLong rehashProcessingIdx = new AtomicLong(0);
	
	public AbstractDiskMap(String baseFolderLoc){
		try {
			final File baseFolder = new File(baseFolderLoc);
			baseFolder.mkdirs();
			this.baseFolderLoc = baseFolder.getCanonicalPath();
			
			final String primaryLoc = this.baseFolderLoc + File.separator + "primary.hash";
			final String secondaryLoc = this.baseFolderLoc + File.separator + "secondary.hash";
			final File primFile = new File(primaryLoc);
			final File secFile = new File(secondaryLoc);
			final long primFileLen = Math.max(defaultFileLength, primFile.length());
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

	/**Embeds table metadata in the secondary to enable persistent tables.
	 * Typically called via close() method.*/
	protected abstract void writeHeader();
	
	protected Object lockForHash(long hash){
		return locks[(int)(hash & (nLocks - 1))];	
	}
	
	//This doesn't lock - because it depends on tableLength, callers should
	//establish some lock that precludes a full rehash (read or write lock on
	//any of the locks).
	protected long idxForHash(long hash){
		final long h0 = hash & (tableLength - 1);
		if(rehashCompleteIdx.get() >= h0)
			return hash & (tableLength + tableLength - 1);
		else return h0;
	}
	
	//Recursively locks all available locks
	protected void completeExpansion(int idx){
		if(idx == nLocks){
			try {
				primaryMapper.doubleLength();
				//Processing counter is always >= complete counter, so we can reset both here
				rehashProcessingIdx.set(0);
				rehashCompleteIdx.set(-1);
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
			//the counters.  All the actual rehashing has been done, though.
			if(rehashCompleteIdx.compareAndSet(tableLength-1, tableLength)){
				completeExpansion(0);
				return;
			}
			
			//Otherwise, we attempt to grab the next index to process
			long i;
			while(true){
				i = rehashProcessingIdx.getAndIncrement();
				//If it's in the valid table range, we conceptually acquired a valid ticket
				if(i < tableLength) break;
				//Otherwise we're in the middle of a reset - spin until it has completed.
				//TODO: Maybe do a wait() instead, depending on efficiency vs. spin
				while(i >= tableLength) {
					Thread.yield();
					if(load() < loadRehashThreshold) return;
					i = rehashProcessingIdx.get();
				}
			}
			//We now have a valid ticket - we rehash the corresponding index
			synchronized(lockForHash(i)){
				rehashIdx(i);
				//Now, to ensure we have a contiguous range of complete tickets, we
				//only add it back to the complete set if we can find the prior ticket.
				//Benchmarking is difficult, but I believe this may be a hot loop.
				while(!rehashCompleteIdx.compareAndSet(i-1, i));
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
		return size.doubleValue() / (tableLength + rehashCompleteIdx.get() + 1);
	}

	/**Interface used in lieu of an iterator for record-processing.*/
	public static interface RecordProcessor {
		/**Returns whether we should continue processing.*/
		public boolean process(WritethruRecord record);
	}
	
	/**Incrementally run the supplied RecordProcessor on each record.*/
	public abstract void processAllRecords(RecordProcessor proc);
	
	/**Returns the byte offset the given bucket number lives at.*/
	protected abstract long idxToPos(long idx);
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
}
