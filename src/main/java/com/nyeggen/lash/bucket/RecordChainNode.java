package com.nyeggen.lash.bucket;

import java.util.Arrays;

/**This represents a chunk o' bytes that does not have a correspondence (yet)
 * to a particular storage location.*/
public class RecordChainNode {
	long hash;
	long nextRecordPos;
	byte[] key;
	byte[] val;
	
	protected RecordChainNode(){}
	
	public RecordChainNode(long hash, byte[] key, byte[] val){
		this(hash, 0, key, val);
	}
	
	public RecordChainNode(long hash, long nextBucket, byte[] key, byte[] val){
		this.hash = hash;
		this.nextRecordPos = nextBucket;
		this.key = key;
		this.val = val;
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
