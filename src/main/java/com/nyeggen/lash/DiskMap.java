package com.nyeggen.lash;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.nyeggen.lash.serde.Serde;


@SuppressWarnings("unchecked")
public class DiskMap<K,V> implements ConcurrentMap<K, V> {
	private final IDiskMap backingMap;
	private final Serde<K> keySerde;
	private final Serde<V> valSerde;
	
	public DiskMap(Serde<K> keySerde, Serde<V> valSerde, ADiskMap backingMap){
		this.keySerde = keySerde;
		this.valSerde = valSerde;
		this.backingMap = backingMap;
	}
	
	@Override
	public V get(Object key) {
		final K asK = (K)key;
		final byte[] kBytes = keySerde.toBytes(asK);
		final byte[] out = backingMap.get(kBytes);
		return valSerde.fromBytes(out);
	}
	
	@Override
	public void clear() {
		backingMap.clear();
	}
	
	@Override
	public boolean containsKey(Object key) {
		//The rationale for these parameters to be Object rather than K,V is
		//that side-effect checks of something impossible maintains validity.
		//However, we need some way to get the bytes out, which forces us to 
		//constrain further than the interface, possibly leading to a 
		//ClassCastException.  We could catch the CCE & return false, but that
		//would crater performance, and the caller has the option of doing so 
		//if they want.
		final K asK = (K)key;
		final byte[] kBytes = keySerde.toBytes(asK);
		return backingMap.containsKey(kBytes);
	}
	
	/**Returns the number of key-value mappings in this map. If the map
	 * contains more than Integer.MAX_VALUE elements, returns Integer.MAX_VALUE.
	 * Use longSize() in preference to this.*/
	@Override
	public int size() {
		return (int)backingMap.size();
	}
	/**Returns the number of key-value mappings in this map.*/
	public long longSize(){
		return backingMap.size();
	}
	@Override
	public boolean isEmpty() {
		return size() == 0;
	}
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		final Iterator<?> it = m.entrySet().iterator();
		while(it.hasNext()){
			final Entry<K, V> entry = (Entry<K, V>)it.next();
			put(entry.getKey(), entry.getValue());
		}
	}
	@Override
	public V remove(Object key) {
		final K asK = (K)key;
		final byte[] kBytes = keySerde.toBytes(asK);
		final byte[] out = backingMap.remove(kBytes);
		return valSerde.fromBytes(out);
	}
	
	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}
	@Override
	public Set<K> keySet() {
		return new KeySet();
	}

	@Override
	public boolean containsValue(Object value) {
		final Iterator<Map.Entry<K, V>> it = entrySet().iterator();
		if (value == null) {
			while (it.hasNext()) {
				if (it.next().getValue() == null) return true;
			}
		} else {
			while (it.hasNext()) {
				if (value.equals(it.next().getValue())) return true;
			}
		}
		return false;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == this) return true;
		if(! (obj instanceof Map)) return false;
		final Map<K,V> asM = (Map<K,V>) obj;
		if(asM.size() != size()) return false;

		final Iterator<Map.Entry<K, V>> it = asM.entrySet().iterator();
		try {
			while(it.hasNext()){
				final Map.Entry<K, V> e = it.next();
				final K k = e.getKey();
				final V thatV = e.getValue();
				final V thisV = get(k);
				if(thisV == null){
					if(thatV != null) return false;
				} else {
					if(!thisV.equals(thatV)) return false;
				}
			}
		} catch(ClassCastException e) { return false; }
		return true;
	}
	
	@Override
	public Collection<V> values() {
		return new ValCollection();
	}
	
	protected class ValCollection extends AbstractCollection<V>{
		@Override
		public int size() {
			return DiskMap.this.size();
		}
		@Override
		public Iterator<V> iterator() {
			return new Iterator<V>() {
				final Iterator<Map.Entry<K, V>> backingIt = DiskMap.this.entrySet().iterator();
				@Override
				public boolean hasNext() {
					return backingIt.hasNext();
				}
				@Override
				public V next() {
					return backingIt.next().getValue();
				}
				@Override
				public void remove() {
					backingIt.remove();
				}
			};
		}
		@Override
		public boolean contains(Object o) {
			return DiskMap.this.containsValue(o);
		}
		@Override
		public void clear() {
			DiskMap.this.clear();
		}
		@Override
		public boolean isEmpty() {
			return DiskMap.this.isEmpty();
		}
	}
	
	protected class KeySet extends AbstractSet<K>{
		@Override
		public int size() {
			return DiskMap.this.size();
		}
		@Override
		public boolean contains(Object o) {
			return DiskMap.this.containsKey(o);
		}
		@Override
		public Iterator<K> iterator() {
			return new Iterator<K>() {
				final Iterator<Map.Entry<K, V>> backingIt = DiskMap.this.entrySet().iterator();
				@Override
				public boolean hasNext() {
					return backingIt.hasNext();
				}
				@Override
				public K next() {
					return backingIt.next().getKey();
				}
				@Override
				public void remove() {
					backingIt.remove();
				}
			};
		}
		@Override
		public void clear() {
			DiskMap.this.clear();
		}
		@Override
		public boolean remove(Object o) {
			return DiskMap.this.remove(o) != null;
		}
	}
	
	protected class EntrySet extends AbstractSet<Map.Entry<K, V>> {
		@Override
		public int size() {
			return DiskMap.this.size();
		}

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new Iterator<Map.Entry<K,V>>() {
				final Iterator<Map.Entry<byte[], byte[]>> backingIt = backingMap.iterator();
				@Override
				public boolean hasNext() {
					return backingIt.hasNext();
				}
				@Override
				public java.util.Map.Entry<K, V> next() {
					final Map.Entry<byte[], byte[]> e = backingIt.next();
					final K k = keySerde.fromBytes(e.getKey());
					final V v = valSerde.fromBytes(e.getValue());
					return new AbstractMap.SimpleImmutableEntry<K,V>(k, v);
				}
				@Override
				public void remove() { backingIt.remove(); }
			};
		}

		@Override
		public boolean contains(Object o) {
			if (!(o instanceof Map.Entry)) return false;
			Map.Entry<K, V> e = (Map.Entry<K, V>) o;
			final V v = get(e.getKey());
			return v == null ? e.getValue() == null : v.equals(e.getValue());
		}

		@Override
		public void clear() {
			DiskMap.this.clear();
		}

		@Override
		public boolean remove(Object o) {
			if (!(o instanceof Map.Entry)) return false;
			Map.Entry<K, V> e = (Map.Entry<K, V>) o;
			return DiskMap.this.remove(e.getKey(), e.getValue());
		}
	}
	
	@Override
	public V put(K key, V value) {
		final byte[] kBytes = keySerde.toBytes(key);
		final byte[] vBytes = valSerde.toBytes(value);
		final byte[] out = backingMap.put(kBytes, vBytes);
		return valSerde.fromBytes(out);
	}
	
	@Override
	public V putIfAbsent(K key, V value) {
		final byte[] kBytes = keySerde.toBytes(key);
		final byte[] vBytes = valSerde.toBytes(value);
		final byte[] out = backingMap.putIfAbsent(kBytes, vBytes);
		return valSerde.fromBytes(out);
	}
	
	@Override
	public boolean remove(Object key, Object value) {
		final K asK = (K)key;
		final V asV = (V)value;
		
		final byte[] kBytes = keySerde.toBytes(asK);
		final byte[] vBytes = valSerde.toBytes(asV);
		return backingMap.remove(kBytes, vBytes);
	}
	
	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		final byte[] kBytes = keySerde.toBytes(key);
		final byte[] oldValBytes = valSerde.toBytes(oldValue);
		final byte[] newValBytes = valSerde.toBytes(newValue);
		return backingMap.replace(kBytes, oldValBytes, newValBytes);
	}
	@Override
	public V replace(K key, V value) {
		final byte[] kBytes = keySerde.toBytes(key);
		final byte[] vBytes = valSerde.toBytes(value);
		final byte[] out =  backingMap.replace(kBytes, vBytes);
		return valSerde.fromBytes(out);
	}
	
}
