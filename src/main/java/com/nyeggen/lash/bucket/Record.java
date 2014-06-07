package com.nyeggen.lash.bucket;

import java.util.Arrays;

import com.nyeggen.lash.util.MMapper;

/**This represents a chunk o' bytes that does not have a correspondence (yet)
 * to a particular storage location.*/
public class Record {
	long hash;
	long nextRecordPos;
	byte[] key;
	byte[] val;
	
	protected Record(){}
	
	public Record(long hash, byte[] key, byte[] val){
		this(hash, 0, key, val);
	}
	
	public Record(long hash, long nextBucket, byte[] key, byte[] val){
		this.hash = hash;
		this.nextRecordPos = nextBucket;
		this.key = key;
		this.val = val;
	}
	
	public WritethruRecord writeAt(MMapper m, long pos){
		m.putLong(pos, hash);
		m.putLong(pos + 8, nextRecordPos);
		m.putInt(pos + 16, key.length);
		m.putInt(pos + 20, val.length);
		m.putBytes(pos + 24, key);
		m.putBytes(pos + 24 + key.length, val);

		return new WritethruRecord(m, pos, hash, nextRecordPos, key, val);
	}
	
	public long size(){ return 8 + 8 + 4 + 4 + key.length + val.length; }
	
	public long getNextRecordPos(){ return this.nextRecordPos; }
	public void setNextRecordPos(long nRecPos){ this.nextRecordPos = nRecPos; }
	public long getHash(){ return hash; }
	public byte[] getKey(){ return key; }
	public byte[] getVal(){ return val; }
	
	public boolean keyEquals(long oHash, byte[] oKey){
		return oHash == this.hash && Arrays.equals(oKey, this.key);
	}
}
