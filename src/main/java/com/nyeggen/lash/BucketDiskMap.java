package com.nyeggen.lash;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.nyeggen.lash.bucket.RecordPtr;
import com.nyeggen.lash.util.Hash;
import com.nyeggen.lash.util.MMapper;

/**Each bucket is a multi-record mini-hash table that stores multiple pointers
 * into secondary.  If a bucket overflows, we chain to a second bucket of
 * pointers stored in secondary.*/
public class BucketDiskMap extends ADiskMap {

	protected final static int bucketByteSize = 4096;
	protected final static int bucketHeaderSize = 16;
	protected final static int recordSize = 24;
	protected final static int recordsPerBucket = 170; // 4096 / 24 + 16;
	protected final static int recordsPerBucketTarget = 128; // ~0.75 load factor

	//When we split a bucket chain, the "old" position typically loses half its
	//buckets.  They are cleared and recorded here so we can re-use them instead
	//of allocating additional buckets.
	//Alternately, we could store a pointer to a "free chain" in the header.
	protected final ConcurrentLinkedQueue<Long> freeSecondaryBuckets = new ConcurrentLinkedQueue<Long>();
	
	public BucketDiskMap(String baseFolderLoc){
		this(baseFolderLoc, 0);
	}
	public BucketDiskMap(String baseFolderLoc, long primaryFileLen){
		super(baseFolderLoc, nextPowerOf2(primaryFileLen));
	}
	
	@Override
	protected void readHeader(){
		secondaryLock.readLock().lock();
		try {
			final long size = secondaryMapper.getLong(0),
					   bucketsInMap = secondaryMapper.getLong(8),
					   lastSecondaryPos = secondaryMapper.getLong(16),
					   rehashComplete = secondaryMapper.getLong(24);
			this.size.set(size);
			this.tableLength = bucketsInMap == 0 ? (primaryMapper.size() / bucketByteSize) : bucketsInMap;
			this.secondaryWritePos.set(lastSecondaryPos == 0 ? getHeaderSize() : lastSecondaryPos);
			this.rehashComplete.set(rehashComplete);
		} finally {
			secondaryLock.readLock().unlock();
		}
	}
	
	@Override
	public double load() {
		return super.load() / recordsPerBucket;
	}

	protected long idxToPos(long idx){
		return idx * bucketByteSize;
	}
	
	/**Represents the result of a search in a bucket chain for a particular
	 * key.  If the search was successful, we do not need to record the
	 * information about free positions or the last bucket in the chain.
	 * If we do not find the key, and there are no free positions, we may
	 * only set the last bucket information.*/
	protected static class SearchResult {
		/**Bucket in which the key was found, or null if it was not.*/
		BucketView foundBucket = null;
		/**Bucket sub-index at which the key was found, or -1 if it was not.*/
		int foundSubIdx = -1;
		/**Bucket in which the first writable slot was found, or null if it was not.
		 * We insert to bucket capacity, not load target.*/
		BucketView freeBucket = null;
		/**Bucket sub-index at which there is a writable slot, or -1 if there is none.*/
		int freeSubIdx = -1;
		/**The final bucket in the searched chain, or null if we found the key.*/
		BucketView lastBucket;
		/**The val associated with the supplied key, or null if there was none.*/
		byte[] val = null;
	}
	
	private byte[] getValIfMatch(RecordPtr recPtr, byte[] k){
		final byte[] prospectiveK = recPtr.getKey(secondaryMapper);
		return Arrays.equals(prospectiveK, k) ? recPtr.getVal(secondaryMapper) : null;
	}
	
	/**Runs a search over all buckets in a chain.*/
	private SearchResult locateRecord(byte[] k, long hash){
		final SearchResult out = new SearchResult();
		final long idx = idxForHash(hash);
		BucketView bucket = new BucketView(idx);
		
		//Chain to next bucket
		while(true){
			if(bucket.findInBucket(hash, k, out)){
				return out;
			}
			final BucketView nextBucket = bucket.nextBucket();
			if(nextBucket == null){
				out.lastBucket = bucket;
				return out;
			} else bucket = nextBucket;
		}
	}
	
	/**Used for rehashing. Splits data into groups of at max recordsPerBucketTarget.
	 * If data is empty, returns the List equivalent of [[]]*/
	private static List<List<RecordPtr>> splitToBuckets(List<RecordPtr> data){
		final List<List<RecordPtr>> out = new ArrayList<List<RecordPtr>>();
		if(data.size() == 0) {
			out.add(new ArrayList<RecordPtr>(0));
		} else if(data.size() <= recordsPerBucketTarget){
			out.add(data);
		} else for(int i=0; i<data.size(); i+= recordsPerBucketTarget){
			final List<RecordPtr> these = data.subList(i, Math.min(i+recordsPerBucketTarget, data.size()));
			out.add(these);
		}
		return out;
	}

	/**Overwrites the existing contents of the bucket chain starting at bucket
	 * with the given record pointers.  If there are "left over" buckets, they
	 * are added to the free list.*/
	private void overwriteChain(BucketView bucket, List<RecordPtr> ptrs){
		final Iterator<List<RecordPtr>> it = splitToBuckets(ptrs).iterator();
		while(it.hasNext()){
			final List<RecordPtr> sublist = it.next();
			bucket.overwritePointers(sublist);
			if(it.hasNext()){
				if (bucket.nextBucket() == null) bucket = bucket.allocateNextBucket();
				else bucket = bucket.nextBucket();
			} else break;
		}
		final BucketView endBucket = bucket;
		
		//Add tail to the free list
		bucket = bucket.nextBucket();
		while(bucket != null){
			final BucketView nextBucket = bucket.nextBucket();
			bucket.clearAll();
			freeSecondaryBuckets.add(bucket.pos);
			bucket = nextBucket;
		}
		endBucket.setNextBucketPos(0);
	}
	
	@Override
	protected void rehashIdx(long idx) {
		final long keepIdx = idx, moveIdx = idx + tableLength;
		
		final ArrayList<RecordPtr> keepBuckets = new ArrayList<RecordPtr>();
		final ArrayList<RecordPtr> moveBuckets = new ArrayList<RecordPtr>();

		//Accumulate all the old pointer records
		{
			final BucketView keepBucket = new BucketView(keepIdx);
			final List<RecordPtr> allBuckets = keepBucket.getAllPointersInChain();
			//And filter
			for(final RecordPtr recPtr : allBuckets){
				final long newIdx = recPtr.hash & (tableLength + tableLength - 1L);
				if(newIdx == keepIdx) keepBuckets.add(recPtr);
				else if (newIdx == moveIdx) moveBuckets.add(recPtr);
				else throw new IllegalStateException("Should rehash to idx or idx+tableLength");
			}
		}
		
		overwriteChain(new BucketView(keepIdx), keepBuckets);
		overwriteChain(new BucketView(moveIdx), moveBuckets);
	}

	@Override
	public byte[] get(byte[] k) {
		final long hash = Hash.murmurHash(k);
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			return sr.val;
		}
	}
	
	@Override
	public byte[] put(byte[] k, byte[] v) {
		if(load() > loadRehashThreshold) rehash();
		
		final long hash = Hash.murmurHash(k);
		final long dataPtr = writeKeyVal(k, v);
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			final RecordPtr toWrite = new RecordPtr(hash, dataPtr, k.length, v.length);
			if(sr.val != null){
				//Overwrite existing
				sr.foundBucket.writeRecord(toWrite, sr.foundSubIdx);
			} else if(sr.freeBucket != null){
				//Write new, in the free position
				sr.freeBucket.writeRecord(toWrite, sr.freeSubIdx);
				size.incrementAndGet();
			} else {
				//Write new, in a new bucket
				final RecordPtr recPtr = new RecordPtr(hash, dataPtr, k.length, v.length);
				sr.lastBucket.allocateNextBucket().writeRecord(recPtr);
				size.incrementAndGet();
			}
			return sr.val;
		}
	}

	@Override
	public byte[] putIfAbsent(byte[] k, byte[] v) {
		if(load() > loadRehashThreshold) rehash();
		
		final long hash = Hash.murmurHash(k);
		
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			if(sr.val != null) return sr.val;
			else if(sr.freeBucket != null){
				//Write new, in the free position
				final long dataPtr = writeKeyVal(k, v);
				final RecordPtr recPtr = new RecordPtr(hash, dataPtr, k.length, v.length);
				sr.freeBucket.writeRecord(recPtr, sr.freeSubIdx);
				size.incrementAndGet();
			} else {
				//Write new, in a new bucket
				final long dataPtr = writeKeyVal(k, v);
				final RecordPtr recPtr = new RecordPtr(hash, dataPtr, k.length, v.length);
				sr.lastBucket.allocateNextBucket().writeRecord(recPtr);
				size.incrementAndGet();
			}
			return sr.val;
		}
	}

	@Override
	public byte[] remove(byte[] k) {
		final long hash = Hash.murmurHash(k);
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			if(sr.val != null){
				sr.foundBucket.writeRecord(RecordPtr.DELETED, sr.foundSubIdx);
				size.decrementAndGet();
			}
			return sr.val;
		}
	}
	
	@Override
	public boolean remove(byte[] k, byte[] v) {
		final long hash = Hash.murmurHash(k);
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			if(Arrays.equals(sr.val, v)){
				sr.foundBucket.writeRecord(RecordPtr.DELETED, sr.foundSubIdx);
				size.decrementAndGet();
				return true;
			}
			return false;
		}
	}
	
	@Override
	public byte[] replace(byte[] k, byte[] v) {
		final long hash = Hash.murmurHash(k);
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			if(sr.val != null){
				//We could be optimistic and write this outside the lock.
				final long dataPtr = writeKeyVal(k, v);
				final RecordPtr toWrite = new RecordPtr(hash, dataPtr, k.length, v.length);
				sr.foundBucket.writeRecord(toWrite, sr.foundSubIdx);
			}
			return sr.val;
		}
	}
	
	@Override
	public boolean replace(byte[] k, byte[] prevVal, byte[] newVal) {
		final long hash = Hash.murmurHash(k);
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			if(Arrays.equals(sr.val, prevVal)){
				//We could be optimistic and write this outside the lock.
				final long dataPtr = writeKeyVal(k, newVal);
				final RecordPtr toWrite = new RecordPtr(hash, dataPtr, k.length, newVal.length);
				sr.foundBucket.writeRecord(toWrite, sr.foundSubIdx);
				return true;
			}
			return false;
		}
	}
	
	private long writeKeyVal(byte[] k, byte[] v){
		final long out = allocateSecondary(k.length + v.length);
		secondaryMapper.putBytes(out, k);
		secondaryMapper.putBytes(out + k.length, v);
		return out;
	}
	
	@Override
	public Iterator<Map.Entry<byte[], byte[]>> iterator(){
		return new Iterator<Map.Entry<byte[],byte[]>>() {
			BucketView nextBucket = new BucketView(0);
			int nextIdx = 0, nextSubIdx = -1;
			{
				advance();
			}
			private void advance(){
				for(; nextIdx < tableLength; ){
					for(; nextBucket != null; nextBucket = nextBucket.nextBucket()){
						for(nextSubIdx = nextSubIdx+1; nextSubIdx < recordsPerBucket; nextSubIdx++){
							if(!nextBucket.getPointer(nextSubIdx).isWritable()) return;
						}
						nextSubIdx = 0;
					}
					nextBucket = (++nextIdx < tableLength) ? new BucketView(nextIdx) : null;
				}
			}
			@Override
			public boolean hasNext() {
				return nextBucket != null;
			}
			@Override
			public Entry<byte[], byte[]> next() {
				final RecordPtr ptr = nextBucket.getPointer(nextSubIdx);
				final byte[] k = ptr.getKey(secondaryMapper);
				final byte[] v = ptr.getVal(secondaryMapper);
				return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(k, v);
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	/**Returns position between 0 and recordsPerBucket, based on top bits*/
	protected static int subIdxForHash(long hash){
		return (int)((hash >>> (Long.numberOfLeadingZeros(recordsPerBucket) - 1)) % recordsPerBucket);
	}
	/**Absolute position of the nth record in the bucket at the given pos*/
	protected static long subPosForSubIdx(long bucketPos, long subIdx){
		return bucketPos + bucketHeaderSize + subIdx*recordSize;
	}
	
	/**Object corresponding to a particular bucket.*/
	protected class BucketView {
		final long idx, pos;
		final MMapper mapper;
		long nextBucketPos = -1;
		
		private BucketView(long idx, long pos, MMapper mapper){
			this.idx = idx;
			this.pos = pos;
			this.mapper = mapper;
		}
		
		/**Zeroes the entire bucket.*/
		public void clearAll(){
			mapper.putBytes(pos, new byte[bucketByteSize]);
		}
		
		/**Clears all embedded RecordPtrs, retaining header*/
		public void clearEntries(){
			mapper.putBytes(pos+bucketHeaderSize, new byte[bucketByteSize-bucketHeaderSize]);
		}
		
		//For primary.
		public BucketView(long idx){
			this(idx, idxToPos(idx), primaryMapper);
		}
		
		/**Returns the next bucket in the chain, or null if there is none.*/
		public BucketView nextBucket(){
			if(nextBucketPos == -1) loadNextBucketPos();
			if(nextBucketPos == 0) return null;
			return new BucketView(this.idx, nextBucketPos, secondaryMapper);
		}
		
		/**Inserts the given record at the optimal position in the bucket.
		 * Throws IllegalStateException if you insert into a full bucket.*/
		public void writeRecord(RecordPtr ptr){
			int subIdx = subIdxForHash(ptr.hash);
			for(int offset=0; offset<recordsPerBucket; offset++){
				final long subPos = subPosForSubIdx(pos, subIdx);
				if(RecordPtr.writableAt(mapper, subPos)){
					ptr.writeToPos(subPos, mapper);
					return;
				}
				subIdx = (subIdx+1) % recordsPerBucket;
			}
			throw new IllegalStateException("Writing to full bucket");
		}
		
		/**Writes the given record pointer to the given index in the bucket.
		 * Overwrites existing data.*/
		public void writeRecord(RecordPtr ptr, int subIdx){
			final long writePos = subPosForSubIdx(pos, subIdx);
			ptr.writeToPos(writePos, mapper);
		}
		
		/**Searches this bucket for the given key, mutating the given SearchResult
		 * as needed, and returning true if it was found.*/
		public boolean findInBucket(long hash, byte[] k, SearchResult out){
			final int startSubIdx = subIdxForHash(hash);
			for(int offset = 0; offset < recordsPerBucket; offset++){
				final int subIdx = (startSubIdx + offset) % recordsPerBucket;
				final long subPos = subPosForSubIdx(pos, subIdx);
				
				final RecordPtr recPtr = new RecordPtr(mapper, subPos);
				
				if(recPtr.maybeMatches(hash, k)){
					//Check for "real" match
					final byte[] prospectiveV = getValIfMatch(recPtr, k);
					if(prospectiveV != null){
						out.val = prospectiveV;
						out.foundBucket = this;
						out.foundSubIdx = subIdx;
						return true;
					}
					else continue;
				} else if(recPtr.isFree()){
					if(out.freeBucket == null){
						out.freeBucket = this;
						out.freeSubIdx = subIdx;
					}
					break;
				} else if (recPtr.isDeleted()){
					if(out.freeBucket == null){
						out.freeBucket = this;
						out.freeSubIdx = subIdx;
					}
					continue;
				} else {
					//Mismatched non-writable key.  Keep traversing bucket
					continue;
				}
			}
			return false;
		}
		
		/**Sets the nextBucketPos to a new empty bucket, either freshly
		 * allocated from secondary or pulled from the free list.
		 * Calling this on a bucket with an existing valid next bucket throws
		 * an IllegalStateException.*/
		public BucketView allocateNextBucket(){
			if(nextBucketPos != 0) throw new IllegalStateException();
			final Long prospective = freeSecondaryBuckets.poll();
			if(prospective != null) nextBucketPos = prospective.longValue();
			else nextBucketPos = BucketDiskMap.this.allocateSecondary(bucketByteSize);
			setNextBucketPos(nextBucketPos);
			return nextBucket();
		}
		
		/**Returns the (possibly free or deleted) record pointer at the given
		 * index in this bucket.*/
		public RecordPtr getPointer(int subIdx){
			final long subPos = subPosForSubIdx(pos, subIdx);
			return new RecordPtr(mapper, subPos);
		}
		
		/**Adds all valid RecordPtrs in this bucket to the given list.*/
		private void getAllPointers(List<RecordPtr> accum){
			for(int i=0; i<recordsPerBucket; i++){
				final RecordPtr recPtr = getPointer(i);
				if(!recPtr.isWritable()) accum.add(recPtr);
			}
		}
		
		/**Returns a List containing all valid RecordPtrs in this chain.*/
		public List<RecordPtr> getAllPointersInChain(){
			final ArrayList<RecordPtr> out = new ArrayList<RecordPtr>();
			getAllPointers(out);
			BucketView child = this.nextBucket();
			while(child != null){
				child.getAllPointers(out);
				child = child.nextBucket();
			}
			return out;
		}
		
		/**Removes all existing RecordPtrs from this bucket, and populates with
		 * those in the passed list.  Throws IllegalArgumentException if the
		 * passed list contains too many to write.  Removes all existing pointers
		 * even if the passed list is empty.*/
		public void overwritePointers(List<RecordPtr> ptrs){
			if(ptrs.size() > recordsPerBucket) 
				throw new IllegalArgumentException("Number of pointers over capacity");
			clearEntries();
			for(final RecordPtr recPtr : ptrs) writeRecord(recPtr);
		}
				
		private void loadNextBucketPos(){
			nextBucketPos = mapper.getLong(pos);
		}
		public void setNextBucketPos(long v){
			nextBucketPos = v;
			mapper.putLong(pos, v);
		}
	}
}
