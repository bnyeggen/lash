package com.nyeggen.lash.serde;

import java.util.UUID;

import com.nyeggen.lash.util.MMapper;

@SuppressWarnings("restriction")
public class UUIDSerde implements Serde<UUID> {
	private UUIDSerde(){}
	private static final UUIDSerde inst = new UUIDSerde();
	public static UUIDSerde getInstance(){ return inst; }
	
	@Override
	public UUID fromBytes(byte[] d) {
		if(d == null || d.length == 0) return null;
		final long baos = MMapper.getByteArrayOffset();
		final long mostSig = MMapper.getUnsafe().getLong(d, baos);
		final long leastSig = MMapper.getUnsafe().getLong(d, baos+8);
		return new UUID(mostSig, leastSig);
	}
	@Override
	public byte[] toBytes(UUID t) {
		if(t == null) return new byte[]{};
		final long baos = MMapper.getByteArrayOffset();
		final long mostSig = t.getMostSignificantBits();
		final long leastSig = t.getLeastSignificantBits();
		final byte[] out = new byte[16];
		
		MMapper.getUnsafe().putLong(out, baos, mostSig);
		MMapper.getUnsafe().putLong(out, baos+8, leastSig);

		return out;
	}
}
