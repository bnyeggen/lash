package com.nyeggen.lash.bucket;

import com.nyeggen.lash.util.MMapper;

/**This represents a bucket that exists at a particular location in an underlying
 * file.  Mutator methods will propagate through to the underlying storage.
 * However, if another writer overwrites data logically corresponding to this
 * bucket, the change will not be reflected in the Java object.*/
public class VarSizeWritethruRecord extends Record {
	final MMapper m;
	final long pos;
	
	public static VarSizeWritethruRecord readRecord(MMapper m, long pos){
		return new VarSizeWritethruRecord(m, pos);
	}
	
	/**Used for reads*/
	protected VarSizeWritethruRecord(MMapper m, long pos){
		super();
		this.m = m;
		this.pos = pos;
		
		this.hash = m.getLong(pos);
		this.nextRecordPos = m.getLong(pos + 8);
		final int keyLen = m.getInt(pos + 16);
		final int valLen = m.getInt(pos + 20);
		this.key = new byte[keyLen];
		this.val = new byte[valLen];
		
		m.getBytes(pos + 24, key);
		m.getBytes(pos + 24 + keyLen, val);		
	}
	
	/**Used for writes*/
	protected VarSizeWritethruRecord(MMapper m, long pos, long hash, long nextRecordPos, byte[] key, byte[] val){
		this.m = m;
		this.pos = pos;
		this.hash = hash;
		this.nextRecordPos = nextRecordPos;
		this.key = key;
		this.val = val;
	}
	
	@Override
	public void setNextRecordPos(long nRecPos){
		this.nextRecordPos = nRecPos;
		m.putLong(pos + 8, nRecPos);
	}

	public long getPos(){
		return pos;
	}

	@Override
	public String toString(){
		return "Record:{Position:" + pos  + ", Hash: " + hash + ", NextRecPos:" + nextRecordPos 
				+ ", KeySize:" + key.length + ", ValSize: " + val.length + "}";
	}
	
	public static VarSizeWritethruRecord writeRecord(Record rec, MMapper m, long pos){
		m.putLong(pos, rec.hash);
		m.putLong(pos + 8, rec.nextRecordPos);
		m.putInt(pos + 16, rec.key.length);
		m.putInt(pos + 20, rec.val.length);
		m.putBytes(pos + 24, rec.key);
		m.putBytes(pos + 24 + rec.key.length, rec.val);

		return new VarSizeWritethruRecord(m, pos, rec.hash, rec.nextRecordPos, rec.key, rec.val);
	}
}
