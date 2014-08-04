package com.nyeggen.lash.serde;

import com.nyeggen.lash.util.MMapper;

@SuppressWarnings("restriction")
public class LongSerde implements Serde<Long> {
	private LongSerde(){}
	private static final LongSerde inst = new LongSerde();
	public static LongSerde getInstance(){ return inst; }

	@Override
	public Long fromBytes(byte[] b) {
		if (b == null || b.length == 0) return null;
		return MMapper.getUnsafe().getLong(b, MMapper.getByteArrayOffset());
	}
	@Override
	public byte[] toBytes(Long t) {
		if(t == null) return new byte[]{};
		final byte[] out = new byte[8];
		MMapper.getUnsafe().putLong(out, MMapper.getByteArrayOffset(), t.longValue());
		return out;
	}
}
