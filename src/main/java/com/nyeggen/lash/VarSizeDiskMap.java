package com.nyeggen.lash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.nyeggen.lash.bucket.RecordChainNode;
import com.nyeggen.lash.bucket.WritethruRecordChainNode;
import com.nyeggen.lash.util.Hash;

public class VarSizeDiskMap extends AbstractDiskMap {

	/* In this implementation, the primary mapper just stores long pointers
	 * into the secondary mapper.*/
	
	private static final int PRIMARY_REC_SIZE = 8;
	
	public VarSizeDiskMap(String baseFolderLoc){
		this(baseFolderLoc, 0);
	}
	public VarSizeDiskMap(String baseFolderLoc, long primaryFileLen){
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
			this.tableLength = bucketsInMap == 0 ? (primaryMapper.size() / PRIMARY_REC_SIZE) : bucketsInMap;
			this.secondaryWritePos.set(lastSecondaryPos == 0 ? getHeaderSize() : lastSecondaryPos);
			this.rehashComplete.set(rehashComplete);
		} finally {
			secondaryLock.readLock().unlock();
		}
	}

	protected long idxToPos(long idx){
		return idx * PRIMARY_REC_SIZE;
	}
	/**Retrieves a record at the given position from the secondary. Does not
	 * validate the correctness of the position.*/
	protected WritethruRecordChainNode getSecondaryRecord(long pos){
		secondaryLock.readLock().lock();
		try {
			return WritethruRecordChainNode.readRecord(secondaryMapper, pos);
		} finally {
			secondaryLock.readLock().unlock();
		}
	}
	
	/**Allocates sufficient space for the record to be written to secondary
	/* at the returned position.*/
	protected long allocateForRecord(final RecordChainNode record){
		final long recordSize = record.size();
		return allocateSecondary(recordSize);
	}
	
	@Override
	public byte[] get(byte[] k){
		final long hash = Hash.murmurHash(k);
		synchronized(lockForHash(hash)){
			final long idx = idxForHash(hash);
			final long pos = idxToPos(idx);
			final long adr = primaryMapper.getLong(pos);
			if(adr == 0) return null;
			
			WritethruRecordChainNode record = getSecondaryRecord(adr);
			while(true){
				if(record.keyEquals(hash, k)) {
					return record.getVal();
				} else if(record.getNextRecordPos() != 0) {
					record = getSecondaryRecord(record.getNextRecordPos());
				} else return null;
			}
		}
	}
	
	//This is the primary use case for a r/w lock
	@Override
	public byte[] putIfAbsent(byte[] k, byte[] v){
		if(load() > loadRehashThreshold) rehash();

		final long hash = Hash.murmurHash(k);
		final RecordChainNode toWriteBucket = new RecordChainNode(hash, k, v);
		
		synchronized(lockForHash(hash)){
			final long idx = idxForHash(hash);
			final long pos = idxToPos(idx);
			final long adr = primaryMapper.getLong(pos);
			if(adr == 0){
				final long insertPos = allocateForRecord(toWriteBucket);
				WritethruRecordChainNode.writeRecord(toWriteBucket, secondaryMapper, insertPos);
				primaryMapper.putLong(pos, insertPos);
				size.incrementAndGet();
				return null;
			}
			WritethruRecordChainNode bucket = getSecondaryRecord(adr);
			while(true){
				if(bucket.keyEquals(hash, k)) return bucket.getVal();
				else if(bucket.getNextRecordPos() != 0) bucket = getSecondaryRecord(bucket.getNextRecordPos());
				else {
					final long insertPos = allocateForRecord(toWriteBucket);
					WritethruRecordChainNode.writeRecord(toWriteBucket, secondaryMapper, insertPos);
					bucket.setNextRecordPos(insertPos);
					size.incrementAndGet();
					return null;
				}
			}
		}
	}
	
	@Override
	public byte[] put(byte[] k, byte[] v){
		if(load() > loadRehashThreshold) rehash();
		
		final long hash = Hash.murmurHash(k);
		final RecordChainNode toWriteBucket = new RecordChainNode(hash, k, v);
		//We'll be inserting somewhere
		final long insertPos = allocateForRecord(toWriteBucket);
		
		synchronized(lockForHash(hash)){
			final long idx = idxForHash(hash);
			final long pos = idxToPos(idx);
			
			final long adr = primaryMapper.getLong(pos);
			if(adr == 0) {
				WritethruRecordChainNode.writeRecord(toWriteBucket, secondaryMapper, insertPos);
				primaryMapper.putLong(pos, insertPos);
				return null;
			}
			
			WritethruRecordChainNode bucket = getSecondaryRecord(adr);
			WritethruRecordChainNode prev = null;
			while(true){
				if(bucket.keyEquals(hash, k)) {
					toWriteBucket.setNextRecordPos(bucket.getNextRecordPos());
					WritethruRecordChainNode.writeRecord(toWriteBucket, secondaryMapper, insertPos);
					if(prev == null) {
						primaryMapper.putLong(pos, insertPos);
					} else {
						prev.setNextRecordPos(insertPos);						
					}
					return bucket.getVal();
				}
				else if(bucket.getNextRecordPos() != 0) {
					prev = bucket;
					bucket = getSecondaryRecord(bucket.getNextRecordPos());
				}
				else {
					WritethruRecordChainNode.writeRecord(toWriteBucket, secondaryMapper, insertPos);
					bucket.setNextRecordPos(insertPos);
					size.incrementAndGet();
					return null;
				}
			}
		}
	}
	
	@Override
	public byte[] remove(byte[] k){
		final long hash = Hash.murmurHash(k);

		synchronized(lockForHash(hash)){
			final long idx = idxForHash(hash);
			final long pos = idxToPos(idx);
			final long adr = primaryMapper.getLong(pos);
			if(adr == 0) return null;
			
			WritethruRecordChainNode bucket = getSecondaryRecord(adr);
			WritethruRecordChainNode prev = null;
			while(true){
				if(bucket.keyEquals(hash, k)) {
					if(prev == null) primaryMapper.putLong(pos, bucket.getNextRecordPos());
					else prev.setNextRecordPos(bucket.getNextRecordPos());
					size.decrementAndGet();
					return bucket.getVal();
				}
				else if(bucket.getNextRecordPos() != 0) {
					prev = bucket;
					bucket = getSecondaryRecord(bucket.getNextRecordPos());
				}
				else return null;
			}
		}
	}
	
	@Override
	protected void rehashIdx(long idx){
		final ArrayList<WritethruRecordChainNode> keepBuckets = new ArrayList<WritethruRecordChainNode>();
		final ArrayList<WritethruRecordChainNode> moveBuckets = new ArrayList<WritethruRecordChainNode>();
		
		final long keepIdx = idx, moveIdx = idx + tableLength;
		
		final long addr = primaryMapper.getLong(idxToPos(idx));
		if(addr == 0) return;

		WritethruRecordChainNode bucket = getSecondaryRecord(addr);
		while(true){
			final long newIdx = bucket.getHash() & (tableLength + tableLength - 1L);
			if(newIdx == keepIdx) keepBuckets.add(bucket);
			else if(newIdx == moveIdx) moveBuckets.add(bucket);
			else throw new IllegalStateException("hash:" + bucket.getHash() + ", idx:" + keepIdx + ", newIdx:" + newIdx + ", tableLength:" + tableLength);
			if(bucket.getNextRecordPos() != 0) bucket = getSecondaryRecord(bucket.getNextRecordPos());
			else break;
		}
		//Adjust chains
		primaryMapper.putLong(idxToPos(keepIdx), rewriteChain(keepBuckets));
		primaryMapper.putLong(idxToPos(moveIdx), rewriteChain(moveBuckets));
	}
		
	/**Cause each bucket to point to the subsequent one.  Returns address of original,
	 * or 0 if the list was empty.*/
	protected long rewriteChain(List<WritethruRecordChainNode> buckets){
		if(buckets.isEmpty()) return 0;
		buckets.get(buckets.size() - 1).setNextRecordPos(0);
		for(int i=0; i<buckets.size()-1; i++){
			buckets.get(i).setNextRecordPos(buckets.get(i+1).getPos());
		}
		return buckets.get(0).getPos();
	}
	
	/**Incrementally run the supplied RecordProcessor on each record.  Locks
	 * incrementally as well, so does not ensure total consistency unless you
	 * synchronize externally - particularly given a rehash.*/
	@Override
	public void processAllRecords(RecordProcessor proc){
		for(long idx = 0; idx < tableLength; idx++){
			final long pos = idxToPos(idx);
			synchronized(lockForHash(idx)){
				final long adr = primaryMapper.getLong(pos);
				if(adr == 0) continue;
				WritethruRecordChainNode bucket = getSecondaryRecord(adr);
				while(true){
					if(!proc.process(bucket)) return;
					if(bucket.getNextRecordPos() != 0) {
						bucket = getSecondaryRecord(bucket.getNextRecordPos());
					} else break;
				}
			}
		}
	}
	
	/**Average size in bytes of written records. Requires scan, and incrementally
	 * locks & unlocks each bucket - if you want total consistency you must synchronize
	 * externally.*/
	public double avgRecordSize(){
		final AtomicLong recordCtr = new AtomicLong(0), recordSizeCtr = new AtomicLong(0);
		processAllRecords(new RecordProcessor() {
			public boolean process(RecordChainNode record) {
				recordCtr.incrementAndGet();
				recordSizeCtr.addAndGet(record.size());
				return true;
			}
		});
		return recordSizeCtr.doubleValue() / recordCtr.doubleValue();
	}
}
