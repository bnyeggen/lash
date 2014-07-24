package com.nyeggen.lash.serde;

public class RawByteSerde implements Serde<byte[]> {
	@Override
	public byte[] fromBytes(byte[] d) { return d; }
	@Override
	public byte[] toBytes(byte[] t) { return t; }
}
