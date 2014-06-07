package com.nyeggen.lash;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.nyeggen.lash.bucket.WritethruRecord;
import com.nyeggen.lash.util.MMapper;

public abstract class AbstractDiskMap implements Closeable {
	/**Number of lock stripes. Always a power of 2*/
	static final int nLocks = 32;
	/**We attempt to keep load below this value.*/
	static final double loadRehashThreshold = 0.75;
	//268,435,456; equivalent to 33,554,432 longs
	static final long defaultFileLength = 1L << 28;
	
	final MMapper primaryMapper, secondaryMapper;
	final String baseFolderLoc;

	/**Allocations in secondary increment from this point*/
	AtomicLong secondaryWritePos;
	/**Enforces exclusive access to the secondary in the event of a reallocation.*/
	final ReentrantReadWriteLock secondaryLock = new ReentrantReadWriteLock();
	
	final ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[nLocks];
	{ for(int i=0;i<nLocks;i++) locks[i] = new ReentrantReadWriteLock(); }
	
	/**Number of records inserted.*/
	final AtomicLong size = new AtomicLong(0);
	/**Number of buckets in the table, always a power of 2.*/
	long tableLength;

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
	
	/**Returns the lock corresponding to the given hash or bucket index (they
	 * produce the same answer as long as we have nBuckets > nLocks).*/
	protected ReadWriteLock lockForHash(long hash){
		return locks[(int)(hash & (nLocks - 1))];
	}
	
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
		return size.doubleValue() / tableLength;
	}

	/**Interface used in lieu of an iterator for record-processing.*/
	public static interface RecordProcessor {
		/**Returns whether we should continue processing.*/
		public boolean process(WritethruRecord record);
	}
	
	/**Incrementally run the supplied RecordProcessor on each record.*/
	public abstract void processAllRecords(RecordProcessor proc);
	
	public abstract byte[] get(byte[] k);
	public abstract byte[] put(byte[] k, byte[] v);
	public abstract byte[] putIfAbsent(byte[] k, byte[] v);
	public abstract byte[] remove(byte[] k);
}
