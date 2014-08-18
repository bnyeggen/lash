package com.nyeggen.lash.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import sun.nio.ch.FileChannelImpl;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class MMapper implements Closeable{

	private static final Unsafe unsafe;
	private static final Method mmap;
	private static final Method unmmap;
	private static final int BYTE_ARRAY_OFFSET;
	private static final int DOUBLE_ARRAY_OFFSET;
	private static final int INT_ARRAY_OFFSET;
	private static final int LONG_ARRAY_OFFSET;

	private long addr=0, size=0;
	private final String loc;

	static {
		try {
			Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
			singleoneInstanceField.setAccessible(true);
			unsafe = (Unsafe) singleoneInstanceField.get(null);

			mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
			unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);

			BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
			DOUBLE_ARRAY_OFFSET = unsafe.arrayBaseOffset(double[].class);
			INT_ARRAY_OFFSET = unsafe.arrayBaseOffset(int[].class);
			LONG_ARRAY_OFFSET = unsafe.arrayBaseOffset(long[].class);
		} catch (Exception e){
			throw new RuntimeException(e);
		}
	}
	
	private class MMapIndexOOBException extends IndexOutOfBoundsException{
		private static final long serialVersionUID = 5471155580682412381L;
		final long pos;
		public MMapIndexOOBException(long pos) {
			this.pos = pos;
		}
		@Override
		public String toString() {
			return "Access: " + pos + ". Map covers [" + 0 +", " + size + "]";
		}
	}

	//Bundle reflection calls to get access to the given method
	private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
		Method m = cls.getDeclaredMethod(name, params);
		m.setAccessible(true);
		return m;
	}

	//Round to next 4096 bytes
	private static long roundTo4096(long i) {
		return (i + 0xfffL) & ~0xfffL;
	}

	//Given that the location and size have been set, map that location
	//for the given length and set this.addr to the returned offset
	private void mapAndSetOffset() throws Exception{
		if(loc == null){
			addr = (addr == 0) ? allocateDirect(size) : reAllocateDirect(addr, size);
			return;
		}
		
		final RandomAccessFile backingFile = new RandomAccessFile(loc, "rw");
		backingFile.setLength(size);

		final FileChannel ch = backingFile.getChannel();
		addr = (Long) mmap.invoke(ch, 1, 0L, size);

		ch.close();
		backingFile.close();
	}

	/**MMaps a file at the given location, creating the file if it does not
	 * exist.  If the location is null, allocates memory in an anonymous
	 * mapping not backed by any file.*/
	public MMapper(final String loc, long len) throws Exception {
		this.loc = loc;
		this.size = roundTo4096(len);
		mapAndSetOffset();
	}
	public long size(){
		return this.size;
	}

	//Callers should synchronize to avoid calls in the middle of this, but
	//it is undesirable to synchronize w/ all access methods.
	public void remap(long nLen) throws Exception{
		if(loc != null) unmmap.invoke(null, addr, this.size);
		this.size = roundTo4096(nLen);
		mapAndSetOffset();
	}
	
	public void doubleLength() throws Exception{
		remap(this.size * 2);
	}
	
	@Override
	public void close() throws IOException {
		try {
			if(loc != null) unmmap.invoke(null, addr, size);
			else deallocateDirect(addr);
		} catch (Exception e){
			throw new RuntimeException(e);
		}
	}
	
	public byte getByte(long pos){
		if(pos>=size) throw new MMapIndexOOBException(pos);
		return unsafe.getByte(pos + addr);
	}
	
	public int getInt(long pos){
		if(pos+4>size) throw new MMapIndexOOBException(pos);
		return unsafe.getInt(pos + addr);
	}

	public long getLong(long pos){
		if(pos+8>size) throw new MMapIndexOOBException(pos);
		return unsafe.getLong(pos + addr);
	}

	public void putByte(long pos, byte val){
		if(pos>=size) throw new MMapIndexOOBException(pos);
		unsafe.putByte(pos + addr, val);
	}
	
	public void setBit(long bitN, boolean val){
		final long bytePos = bitN >> 3;
		if(bytePos>=size) throw new MMapIndexOOBException(bytePos);
		final long bitBytePos = bitN & 7;
		final byte curByte = unsafe.getByte(addr + bytePos);
		final byte newByte = val 
				? (byte)(curByte | (1 << bitBytePos))
				: (byte)(curByte & ~(1 << bitBytePos));
		unsafe.putByte(addr+bytePos, newByte);
	}
	
	public boolean getBit(long bitN){
		final long bytePos = bitN >> 3;
		if(bytePos>=size) throw new MMapIndexOOBException(bytePos);
		final long bitBytePos = bitN & 7;
		final byte curByte = unsafe.getByte(addr + bytePos);
		return (curByte & (1 << bitBytePos)) != 0;
	}

	public void putInt(long pos, int val){
		if(pos+4>size) throw new MMapIndexOOBException(pos);
		unsafe.putInt(pos + addr, val);
	}

	public void putLong(long pos, long val){
		if(pos+8>size) throw new MMapIndexOOBException(pos);
		unsafe.putLong(pos + addr, val);
	}
	
	//May want to have offset & length within data as well, for both of these
	public void getBytes(long pos, byte[] data){
		if(pos+data.length>size) throw new MMapIndexOOBException(pos);
		unsafe.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET, data.length);
	}

	public void putBytes(long pos, byte[] data){
		if(pos+data.length>size) throw new MMapIndexOOBException(pos);
		unsafe.copyMemory(data, BYTE_ARRAY_OFFSET, null, pos + addr, data.length);
	}
	
	public String getLocation(){
		return loc;
	}
	
	public void clear(){
		unsafe.setMemory(addr, size, (byte)0);
	}
	
	public static long allocateDirect(long size){
		return unsafe.allocateMemory(size);
	}
	public static void deallocateDirect(long addr){
		unsafe.freeMemory(addr);
	}
	public static long reAllocateDirect(long addr, long size){
		return unsafe.reallocateMemory(addr, size);
	}
	
	/**After Java 7, this is a built-in.  We put it here so it's available
	 * for testing.*/
	public static File createTempDir() {
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		String baseName = System.currentTimeMillis() + "-";

		for (int counter = 0; counter < 1024; counter++) {
			File tempDir = new File(baseDir, baseName + counter);
			if (tempDir.mkdir()) {
				return tempDir;
			}
		}
		throw new IllegalStateException("Failed to create directory within "
				+ 1024 + " attempts (tried " + baseName + "0 to " + baseName
				+ (1024 - 1) + ')');
	}
	
	public static Unsafe getUnsafe(){
		return unsafe;
	}
	public static long getByteArrayOffset(){
		return BYTE_ARRAY_OFFSET;
	}
	public static long getDoubleArrayOffset(){
		return DOUBLE_ARRAY_OFFSET;
	}
	public static long getIntArrayOffset(){
		return INT_ARRAY_OFFSET;
	}
	public static long getLongArrayOffset(){
		return LONG_ARRAY_OFFSET;
	}
}
