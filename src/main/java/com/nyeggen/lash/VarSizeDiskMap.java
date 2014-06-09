package com.nyeggen.lash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.nyeggen.lash.bucket.Record;
import com.nyeggen.lash.bucket.WritethruRecord;
import com.nyeggen.lash.util.Hash;

public class VarSizeDiskMap extends AbstractDiskMap {

	/* In this implementation, the primary mapper just stores long pointers
	 * into the secondary mapper.*/
	
	public VarSizeDiskMap(String baseFolderLoc){
		super(baseFolderLoc);
	}
	
	protected void readHeader(){
		secondaryLock.readLock().lock();
		try {
			final long size = secondaryMapper.getLong(0),
					   bucketsInMap = secondaryMapper.getLong(8),
					   lastSecondaryPos = secondaryMapper.getLong(16);			
			this.size.set(size);
			this.tableLength = bucketsInMap == 0 ? (primaryMapper.size() / 8) : bucketsInMap;
			this.secondaryWritePos = new AtomicLong(lastSecondaryPos == 0 ? 24 : lastSecondaryPos);
		} finally {
			secondaryLock.readLock().unlock();
		}
	}

	protected void writeHeader(){
		secondaryLock.writeLock().lock();
		try {
			secondaryMapper.putLong(0, size());
			secondaryMapper.putLong(8, tableLength);
			secondaryMapper.putLong(16, secondaryWritePos.get());
		} finally {
			secondaryLock.writeLock().lock();
		}
	}
	@Override
	protected long idxToPos(long idx){
		return idx * 8;
	}
	/**Retrieves a record at the given position from the secondary. Does not
	 * validate the correctness of the position.*/
	protected WritethruRecord getSecondaryRecord(long pos){
		secondaryLock.readLock().lock();
		try {
			return new WritethruRecord(secondaryMapper, pos);
		} finally {
			secondaryLock.readLock().unlock();
		}
	}
	
	/**Allocates sufficient space for the record to be written to secondary
	/* at the returned position.*/
	protected long allocateForRecord(final Record record){
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
			
			WritethruRecord record = getSecondaryRecord(adr);
			while(true){
				if(record.keyEquals(hash, k)) {
					return record.getVal();
				} else if(record.getNextRecordPos() != 0) {
					record = getSecondaryRecord(record.getNextRecordPos());
				} else return null;
			}
		}
	}
	
	@Override
	public byte[] putIfAbsent(byte[] k, byte[] v){
		if(load() > loadRehashThreshold) rehash();

		final long hash = Hash.murmurHash(k);
		final Record toWriteBucket = new Record(hash, k, v);
		
		synchronized(lockForHash(hash)){
			final long idx = idxForHash(hash);
			final long pos = idxToPos(idx);
			final long adr = primaryMapper.getLong(pos);
			if(adr == 0){
				final long insertPos = allocateForRecord(toWriteBucket);
				toWriteBucket.writeAt(secondaryMapper, insertPos);
				primaryMapper.putLong(pos, insertPos);
				size.incrementAndGet();
				return null;
			}
			WritethruRecord bucket = getSecondaryRecord(adr);
			while(true){
				if(bucket.keyEquals(hash, k)) return bucket.getVal();
				else if(bucket.getNextRecordPos() != 0) bucket = getSecondaryRecord(bucket.getNextRecordPos());
				else {
					final long insertPos = allocateForRecord(toWriteBucket);
					toWriteBucket.writeAt(secondaryMapper, insertPos);
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
		final Record toWriteBucket = new Record(hash, k, v);
		//We'll be inserting somewhere
		final long insertPos = allocateForRecord(toWriteBucket);
		
		synchronized(lockForHash(hash)){
			final long idx = idxForHash(hash);
			final long pos = idxToPos(idx);
			
			final long adr = primaryMapper.getLong(pos);
			if(adr == 0) {
				toWriteBucket.writeAt(secondaryMapper, insertPos);
				primaryMapper.putLong(pos, insertPos);
				return null;
			}
			
			WritethruRecord bucket = getSecondaryRecord(adr);
			WritethruRecord prev = null;
			while(true){
				if(bucket.keyEquals(hash, k)) {
					toWriteBucket.setNextRecordPos(bucket.getNextRecordPos());
					toWriteBucket.writeAt(secondaryMapper, insertPos);
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
					toWriteBucket.writeAt(secondaryMapper, insertPos);
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
			
			WritethruRecord bucket = getSecondaryRecord(adr);
			WritethruRecord prev = null;
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
		final ArrayList<WritethruRecord> keepBuckets = new ArrayList<WritethruRecord>();
		final ArrayList<WritethruRecord> moveBuckets = new ArrayList<WritethruRecord>();
		
		final long keepIdx = idx, moveIdx = idx + tableLength;
		
		final long addr = primaryMapper.getLong(idxToPos(idx));
		if(addr == 0) return;

		WritethruRecord bucket = getSecondaryRecord(addr);
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
	protected long rewriteChain(List<WritethruRecord> buckets){
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
				WritethruRecord bucket = getSecondaryRecord(adr);
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
			public boolean process(WritethruRecord bucket) {
				recordCtr.incrementAndGet();
				recordSizeCtr.addAndGet(bucket.size());
				return true;
			}
		});
		return recordSizeCtr.doubleValue() / recordCtr.doubleValue();
	}
}
