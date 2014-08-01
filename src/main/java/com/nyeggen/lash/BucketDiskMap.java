package com.nyeggen.lash;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import com.nyeggen.lash.bucket.RecordPtr;
import com.nyeggen.lash.util.Hash;
import com.nyeggen.lash.util.MMapper;

/**Each bucket is a multi-record mini-hash table that stores multiple pointers
 * into secondary.  If a bucket overflows, we chain to a second bucket of
 * pointers stored in secondary.*/
public class BucketDiskMap extends AbstractDiskMap {

	protected final static int bucketByteSize = 4096;
	protected final static int bucketHeaderSize = 16;
	protected final static int recordSize = 24;
	protected final static int recordsPerBucket = 170; // 4096 / 24 + 16;
	protected final static int recordsPerBucketTarget = 128; // ~0.75 load factor
	
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

	protected long idxToPos(long idx){
		return idx * bucketByteSize;
	}
	/**Returns position between 0 and recordsPerBucket, based on top bits*/
	protected int subIdxForHash(long hash){
		return (int)((hash >>> (Long.numberOfLeadingZeros(recordsPerBucket) - 1)) % recordsPerBucket);
	}
	protected long subPosForSubIdx(long bucketPos, long subIdx){
		return bucketPos + bucketHeaderSize + subIdx*recordSize;
	}
	protected static long getNextBucketPos(long bucketPos, MMapper mapper){
		return mapper.getLong(bucketPos);
	}
	protected static void setNextBucketPos(long bucketPos, MMapper mapper, long val){
		mapper.putLong(bucketPos, val);
	}
	
	protected static class SearchResult {
		//If finding is successful, we do not need to find the free position or last bucket.
		//If we do not find k, and there are no free positions, we may only set the last
		//bucket.
		
		/**MMapper in which the key was found, or null if it was not.*/
		MMapper foundMapper = null;
		/**Byte position at which the key was found, or -1 if it was not.*/
		long foundPos = -1;
		/**MMapper in which the first writable slot was found, or null if it was not.*/
		MMapper freeMapper = null;
		/**Byte position at which there is a writable slot, or null if there is none.*/
		long freePos = -1;
		/**MMapper containing the last bucket in the chain, or null if it was not located.*/
		MMapper lastBucketMapper = null;
		/**Position of the last bucket in the chain, or -1 if it was not located.*/
		long lastBucketPos = -1;
		/**The val associated with the supplied key, or null if there was none.*/
		byte[] val = null;
	}
	
	private byte[] getValIfMatch(RecordPtr recPtr, byte[] k){
		final byte[] prospectiveK = new byte[k.length];
		final byte[] prospectiveV = new byte[recPtr.vLength];
		final long dataPtr = recPtr.dataPtr;
		secondaryMapper.getBytes(dataPtr, prospectiveK);
		secondaryMapper.getBytes(dataPtr + k.length, prospectiveV);
		return Arrays.equals(prospectiveK, k) ? prospectiveV : null;
	}
	
	/**@param mapper The mapper in which to search
	 * @param hash The hash of the key to be searched for
	 * @param k The key to be searched for
	 * @param bucketPos The byte position of the bucket to be searched
	 * @param startSubIdx The index within the bucket at which to start searching
	 * @param out The search results to be mutated
	 * @return Whether or not the value was found*/
	private boolean findInBucket(MMapper mapper, long hash, byte[] k, long bucketPos, int startSubIdx, SearchResult out){
		for(int offset = 0; offset < recordsPerBucket; offset++){
			final int subIdx = (startSubIdx + offset) % recordsPerBucket;
			final long subPos = subPosForSubIdx(bucketPos, subIdx);
			
			final RecordPtr recPtr = new RecordPtr(mapper, subPos);
			
			if(recPtr.matchesData(hash, k.length)){
				//Check for "real" match
				final byte[] prospectiveV = getValIfMatch(recPtr, k);
				if(prospectiveV != null){
					out.val = prospectiveV;
					out.foundMapper = mapper;
					out.foundPos = subPos;
					return true;
				}
				else continue;
			} else if(recPtr.isFree()){
				if(out.freeMapper == null){
					out.freeMapper = mapper;
					out.freePos = subPos;
				}
				break;
			} else if (recPtr.isDeleted()){
				if(out.freeMapper == null){
					out.freeMapper = mapper;
					out.freePos = subPos;
				}
				continue;
			} else {
				//Mismatched non-writable key.  Keep traversing bucket
				continue;
			}
		}
		return false;
	}
	
	private SearchResult locateRecord(byte[] k, long hash){
		final SearchResult out = new SearchResult();
		final long idx = idxForHash(hash);
		final int startSubIdx = subIdxForHash(hash);
		
		MMapper mapper = primaryMapper;

		long bucketPos = idxToPos(idx);
		long nextBucketPos = getNextBucketPos(bucketPos, mapper);
		
		if(findInBucket(mapper, hash, k, bucketPos, startSubIdx, out)) 
			return out;
		
		//Chain to next bucket
		while(nextBucketPos != 0){
			mapper = secondaryMapper;
			bucketPos = nextBucketPos;
			nextBucketPos = getNextBucketPos(bucketPos, mapper);
			if(findInBucket(mapper, hash, k, bucketPos, startSubIdx, out))
				return out;
		}
		//Not found.  Record last bucket.
		out.lastBucketMapper = mapper;
		out.lastBucketPos = bucketPos;
		return out;
	}

	/**Accumulates a list of RecordPtrs in that bucket, and any of its child buckets.*/
	private void accumRecordsInBucketChain(long bucketPos, MMapper mapper, List<RecordPtr> accum){
		long nextBucketPos = getNextBucketPos(bucketPos, mapper);
		for(int subIdx=0; subIdx<recordsPerBucket; subIdx++){
			final long subPos = subPosForSubIdx(bucketPos, subIdx);
			final RecordPtr recPtr = new RecordPtr(mapper, subPos);
			if(!recPtr.isWritable()){
				accum.add(recPtr);
			}
		}
		if(nextBucketPos != 0){
			accumRecordsInBucketChain(nextBucketPos, secondaryMapper, accum);
		}
	}
	
	/**Overwrites the given bucket with the supplied records, attempting to maintain
	 * load, overwriting down to child buckets, and allocating if necessary*/
	private void writeRecordsToBucketChain(long bucketPos, MMapper mapper, List<RecordPtr> toWrite){
		final RecordPtr[] writeLayout = new RecordPtr[recordsPerBucket];
		final List<RecordPtr> subList = toWrite.subList(0, Math.min(toWrite.size(), recordsPerBucketTarget));
		
		for(final RecordPtr recPtr : subList){
			int subIdx = subIdxForHash(recPtr.hash);
			for(int offset=0; offset<recordsPerBucket; offset++){
				if(writeLayout[subIdx] == null) {
					writeLayout[subIdx] = recPtr;
					break;					
				}
				subIdx = (subIdx + 1) % recordsPerBucket;
			}
		}
		
		for(int subIdx = 0; subIdx < recordsPerBucket; subIdx++){
			final RecordPtr recPtr = writeLayout[subIdx];
			if(recPtr != null) {
				recPtr.writeToPos(subPosForSubIdx(bucketPos, subIdx), mapper);
			} else {
				RecordPtr.writeFree(mapper, subPosForSubIdx(bucketPos, subIdx));
			}
		}
		if(toWrite.size() <= recordsPerBucketTarget) {
			//Nuke next bucket pos
			setNextBucketPos(bucketPos, mapper, 0);
			return;
		}
		long nextBucketPos = getNextBucketPos(bucketPos, mapper);
		if(nextBucketPos == 0){
			nextBucketPos = allocateSecondaryBucket(mapper, bucketPos);
		}
		writeRecordsToBucketChain(nextBucketPos, secondaryMapper, toWrite.subList(recordsPerBucketTarget, toWrite.size()));
	}
	
	@Override
	protected void rehashIdx(long idx) {
		final long keepIdx = idx, moveIdx = idx + tableLength;
		
		final ArrayList<RecordPtr> keepBuckets = new ArrayList<RecordPtr>();
		final ArrayList<RecordPtr> moveBuckets = new ArrayList<RecordPtr>();

		long keepBucketPos = idxToPos(idx);
		long moveBucketPos = idxToPos(moveIdx);

		//Accumulate all the old pointer records
		{
			final ArrayList<RecordPtr> allBuckets  = new ArrayList<RecordPtr>();
			accumRecordsInBucketChain(keepBucketPos, primaryMapper, allBuckets);
			//And filter
			for(final RecordPtr recPtr : allBuckets){
				final long newIdx = recPtr.hash & (tableLength + tableLength - 1L);
				if(newIdx == keepIdx) keepBuckets.add(recPtr);
				else if (newIdx == moveIdx) moveBuckets.add(recPtr);
				else throw new IllegalStateException("Should rehash to idx or idx+tableLength");
			}
		}
		
		writeRecordsToBucketChain(keepBucketPos, primaryMapper, keepBuckets);
		writeRecordsToBucketChain(moveBucketPos, primaryMapper, moveBuckets);
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
		final long hash = Hash.murmurHash(k);
		final long dataPtr = writeKeyVal(k, v);
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			if(sr.val != null){
				//Overwrite existing
				RecordPtr.overwrite(sr.foundMapper, sr.foundPos, hash, dataPtr, k.length, v.length);
			} else if(sr.freeMapper != null){
				//Write new, in the free position
				RecordPtr.overwrite(sr.freeMapper, sr.freePos, hash, dataPtr, k.length, v.length);
				size.incrementAndGet();
			} else {
				//Write new, in a new bucket
				final RecordPtr recPtr = new RecordPtr(hash, dataPtr, k.length, v.length);
				final long bucketPos = allocateSecondaryBucket(sr.lastBucketMapper, sr.lastBucketPos);
				writeRecordsToBucketChain(bucketPos, secondaryMapper, Arrays.asList(new RecordPtr[]{recPtr}));
				size.incrementAndGet();
			}
			return sr.val;
		}
	}

	@Override
	public byte[] putIfAbsent(byte[] k, byte[] v) {
		final long hash = Hash.murmurHash(k);
		
		synchronized(lockForHash(hash)){
			final SearchResult sr = locateRecord(k, hash);
			if(sr.val != null){}
			else if(sr.freeMapper != null){
				//Write new, in the free position
				final long dataPtr = writeKeyVal(k, v);
				RecordPtr.overwrite(sr.freeMapper, sr.freePos, hash, dataPtr, k.length, v.length);
				size.incrementAndGet();
			} else {
				//Write new, in a new bucket
				final long dataPtr = writeKeyVal(k, v);
				final RecordPtr recPtr = new RecordPtr(hash, dataPtr, k.length, v.length);
				final long bucketPos = allocateSecondaryBucket(sr.lastBucketMapper, sr.lastBucketPos);
				writeRecordsToBucketChain(bucketPos, sr.lastBucketMapper, Arrays.asList(new RecordPtr[]{recPtr}));
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
				RecordPtr.writeDeleted(sr.foundMapper, sr.foundPos);
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
				RecordPtr.writeDeleted(sr.foundMapper, sr.foundPos);
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
				RecordPtr.overwrite(sr.foundMapper, sr.foundPos, hash, dataPtr, k.length, v.length);
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
				RecordPtr.overwrite(sr.foundMapper, sr.foundPos, hash, dataPtr, k.length, newVal.length);
				return true;
			}
			return false;
		}
	}
	
	private long allocateSecondaryBucket(MMapper parentMapper, long parentBucketPos){
		final long childBucketPos = allocateSecondary(bucketByteSize);
		parentMapper.putLong(parentBucketPos, childBucketPos);
		return childBucketPos;
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
			MMapper nextMapper;
			long nextBucketIdx;
			long nextBucketPos;
			int nextSubIdx;
			boolean finished;

			{
				finished = true;
				nextMapper = primaryMapper;
				nextBucketIdx = 0;
				nextBucketPos = idxToPos(nextBucketIdx);
				nextSubIdx = -1; //Necessary because we increment on first loop
				advance();
			}
			
			@Override
			public boolean hasNext() { return !finished; }
			private void advance(){
				//Yes, this is ugly; we have to jump into the middle of a 3-nested
				//loop.  Makes ya wish for coroutines.  firstItComplete tells us
				//when we can start "from scratch" in a bucket in primary
				boolean firstItComplete = false;
				finished = true;
				//Loops over all bucket chains in table
				for(; nextBucketIdx<tableLength; nextBucketIdx++){
					if(firstItComplete) {
						nextMapper = primaryMapper;
						nextBucketPos = idxToPos(nextBucketIdx);
					}
					//Loops over a chain of buckets
					while(true){
						//Loops over record pointers in bucket
						//Can we avoid this conditional?
						for(nextSubIdx = firstItComplete ? 0 : nextSubIdx+1
								; nextSubIdx<recordsPerBucket
								; nextSubIdx++){
							final long subPos = subPosForSubIdx(nextBucketPos, nextSubIdx);
							if(!new RecordPtr(nextMapper, subPos).isWritable()){
								finished = false;
								return;
							}
						}
						firstItComplete = true;
						nextBucketPos = getNextBucketPos(nextBucketPos, nextMapper);
						if(nextBucketPos == 0) break;
						else nextMapper = secondaryMapper;						
					}
				}
			}
			@Override
			public Entry<byte[], byte[]> next() {
				if(finished) throw new NoSuchElementException();
				final long subPos = subPosForSubIdx(nextBucketPos, nextSubIdx);
				final RecordPtr recPtr = new RecordPtr(nextMapper, subPos);
				advance();
				final byte[] k = recPtr.getKey(secondaryMapper);
				final byte[] v = recPtr.getVal(secondaryMapper);
				return new AbstractMap.SimpleEntry<byte[],byte[]>(k, v);
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
