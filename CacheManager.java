package com.jnetzhou.utils.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author jnetzhou
 * @description 缓存管理组件
 * @param <K>
 * @param <T>
 * @param <CacheObject>
 */
public class CacheManager<K,V> implements Runnable{
	
	protected final static int DEFAULT_INITAL_CAPACITY = 500;
	protected final static int DEFAULT_MAX_CAPACITY = 2000;
	/**
	 * 扫描周期为1000MS
	 */
	protected final static int DEFAULT_SCHEDULE_TIME = 1000;
	
	protected int initialCapacity = DEFAULT_INITAL_CAPACITY;
	protected int maxCapacity = DEFAULT_MAX_CAPACITY;
	protected long period;
	protected TimeUnit tmUnit;

	@SuppressWarnings("rawtypes")
	protected ConcurrentHashMap<K,WeakReference<CacheObject>> cacheMap = null;
	@SuppressWarnings("rawtypes")
	protected ReferenceQueue<? super CacheObject> refQueue = null;
	protected ScheduledExecutorService scheduledExecutorService;
	@SuppressWarnings("rawtypes")
	protected CacheStateChangeListener cacheStateChangeListener;

	/**
	 * 默认按使用频率最少的清理LRU
	 */
	protected CleanStrategy cleanStrategy = CleanStrategy.USE_FREQUENCY; 
	protected Object lock = new Object();
	protected boolean isRunning = false;
	
	/**
	 * 
	 * @author jnetzhou
	 * @description 缓存状态listener
	 * @param <O>
	 */
	public static interface CacheStateChangeListener<V> {
		public void onAdd(V v);
		public void onDeleteByUser(V v);
		public void onDeleteBySystem(V v);
		public void onDeleteAll();
	}
	
	/**
	 * 
	 * @author jnetzhou
	 * @description 清理策略
	 */
	public static enum CleanStrategy {
		ALIVE_TIME_LIMIT,		//按限定存活时间清理
		USE_FREQUENCY			//按使用频率清理
	}
	
	/**
	 * 
	 * @author jnetzhou
	 * @description 缓存对象封装类
	 * @param <O>
	 */
	public static class CacheObject<O>{
		
		/**
		 * 一直存活
		 */
		public static final int IMMORTAL = -1;
		/**
		 * 进入缓存时间
		 */
		long enterTime;
		/**
		 * 允许存活的时间
		 */
		long allowAliveTime;
		/**
		 * 当前活跃时间
		 */
		long activeTime;
		/**
		 * 缓存对象
		 */
		O o;
		
		public CacheObject() {
			
		}
		
		public CacheObject(O o) {
			this.o = o;
		}

		public CacheObject(O o,boolean setEnterTime) {
			this.o = o;
			if(setEnterTime) {
				enterTime = System.currentTimeMillis();
			}
		}
		
		public CacheObject(O o,boolean setEnterTime,long allowAliveTime) {
			this.o = o;
			long currentTime = System.currentTimeMillis();
			if(setEnterTime) {
				enterTime = currentTime;
			}
			this.allowAliveTime = allowAliveTime;
			this.activeTime = currentTime;
		}
		
		public long getEnterTime() {
			return enterTime;
		}

		public void setEnterTime(long enterTime) {
			this.enterTime = enterTime;
		}

		public long getAllowALiveTime() {
			return allowAliveTime;
		}

		public void setAllowALiveTime(long allowAliveTime) {
			this.allowAliveTime = allowAliveTime;
		}

		public O getO() {
			return o;
		}

		public void setO(O o) {
			this.o = o;
		}
		
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return String.format("Object=%s\n enterTime=%s\n allowAliveTime=%s\n activeTime=%s\n",Objects.isNull(o) ? "null" : o.getClass().getName(),enterTime,allowAliveTime,activeTime);
		}
	}
	
	public void setCacheStateChangeListener(@SuppressWarnings("rawtypes") CacheStateChangeListener cacheStateChangeListener) {
		this.cacheStateChangeListener = cacheStateChangeListener;
	}
	
	
	public CacheManager() {
		this(DEFAULT_INITAL_CAPACITY,DEFAULT_MAX_CAPACITY,DEFAULT_SCHEDULE_TIME,TimeUnit.MILLISECONDS,CleanStrategy.USE_FREQUENCY);
	}
	
	public CacheManager(long period) {
		this(DEFAULT_INITAL_CAPACITY,DEFAULT_MAX_CAPACITY,period,TimeUnit.MILLISECONDS,CleanStrategy.USE_FREQUENCY);
	}
	
	public CacheManager(long period,CleanStrategy cleanStrategy) {
		this(DEFAULT_INITAL_CAPACITY,DEFAULT_MAX_CAPACITY,period,TimeUnit.MILLISECONDS,cleanStrategy);
	}
	
	public CacheManager(long period,TimeUnit tmUnit,CleanStrategy cleanStrategy) {
		this(DEFAULT_INITAL_CAPACITY,DEFAULT_MAX_CAPACITY,period,tmUnit,cleanStrategy);
	}
	
	public CacheManager(int initialCapacity,int maxCapacity,long period,TimeUnit tmUnit) {
		this(initialCapacity,maxCapacity,period,tmUnit,CleanStrategy.USE_FREQUENCY);
	}
	
	@SuppressWarnings("rawtypes")
	public CacheManager(int initialCapacity,int maxCapacity,long period,TimeUnit tmUnit,CleanStrategy cleanStrategy) {
		this.initialCapacity = initialCapacity;
		this.maxCapacity = maxCapacity;
		this.cleanStrategy = cleanStrategy;
		this.period = period;
		this.tmUnit = tmUnit;
		this.refQueue = new ReferenceQueue<CacheObject>();
		this.cacheMap = new ConcurrentHashMap<K,WeakReference<CacheObject>>(initialCapacity);
	}
	
	public void start() {
		if(isRunning) {
			return;
		}
		if(scheduledExecutorService == null || scheduledExecutorService.isShutdown()) {
			scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
			scheduledExecutorService.scheduleAtFixedRate(this, 1000, period, tmUnit);
			isRunning = true;
		}
	}
	
	public void stop() {
		if(isRunning) {
			isRunning = false;
			scheduledExecutorService.shutdownNow();
			scheduledExecutorService = null;
		}
	}
	
	@SuppressWarnings({ "rawtypes" })
	private synchronized List<Map.Entry<K,WeakReference<CacheObject>>> sort() {
		Set<Entry<K,WeakReference<CacheObject>>> set = cacheMap.entrySet();
		List<Map.Entry<K,WeakReference<CacheObject>>> list = new LinkedList<Entry<K,WeakReference<CacheObject>>>(set);
		Collections.sort(list,new Comparator<Entry<K,WeakReference<CacheObject>>>(){
			@Override
			public int compare(Entry<K, WeakReference<CacheObject>> o1, Entry<K, WeakReference<CacheObject>> o2) {
				// TODO Auto-generated method stub
				CacheObject cacheObject1 = o1.getValue().get();
				CacheObject cacheObject2 = o2.getValue().get();
				if(cleanStrategy == CleanStrategy.ALIVE_TIME_LIMIT) {
					return cacheObject1.enterTime < cacheObject2.enterTime ? 1 : -1;
				} else if(cleanStrategy == CleanStrategy.USE_FREQUENCY){
					return cacheObject1.activeTime > cacheObject2.activeTime ? 1 : -1;
				}
				return 0;
			}
			});
		return list;
	}
	
	@SuppressWarnings({ "unchecked", "unused" })
	public synchronized void checkCapacity(int size) {
		System.out.println("maxCapacity- cacheMap.size = " + (maxCapacity - cacheMap.size()));
		if(maxCapacity - cacheMap.size() < size) { //清除足够的缓存空间
			int cleanSize = size - (maxCapacity - cacheMap.size());		                         
			@SuppressWarnings("rawtypes")
			List<Map.Entry<K,WeakReference<CacheObject>>> list = sort();
			int index = 0;
			Set<K> keySet = cacheMap.keySet();
			if(Objects.nonNull(keySet) && keySet.size() > 0) {
				for(K key : keySet) {
					K k = list.get(0).getKey();
					CacheObject<K> cacheObject = cacheMap.remove(k).get();
					if(cacheStateChangeListener != null) {
						cacheStateChangeListener.onDeleteBySystem(cacheObject.o);
					}
					cacheObject.o = null;
					cacheObject = null;
					++index;
					if(index == cleanSize) {
						return;
					}
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public ReferenceQueue getReleaseCaches() {
		return refQueue;
	}
	
	public void add(Map<K,V> kvs) {
		if(Objects.nonNull(kvs)) {
			checkCapacity(kvs.size());
			Set<K> keySet = kvs.keySet();
			for(K key : keySet) {
				add(key,kvs.get(key));
			}
		}
	}
	
	public void add(K k,V v) {
		add(k,v,CacheObject.IMMORTAL);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public V get(K k) {
		V v = null;
		if(cacheMap.containsKey(k)) {
			WeakReference<CacheObject> cacheObjectRef = cacheMap.get(k);
			cacheObjectRef.get().activeTime = System.currentTimeMillis();
			cacheMap.put(k,cacheObjectRef);
			v = (V)(cacheObjectRef.get().o);
		}
		return v;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void add(K k,V v,long allowAliveTime) {
		checkCapacity(1);
		cacheMap.put(k, new WeakReference<CacheObject>(new CacheObject(v,true,allowAliveTime),refQueue));
		if(cacheStateChangeListener != null) {
			cacheStateChangeListener.onAdd(v);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void remove(K k) {
		if(cacheMap.containsKey(k)) {
			WeakReference<CacheObject> cacheObjectRef = cacheMap.get(k);
			if(cacheStateChangeListener != null) {
				cacheStateChangeListener.onDeleteBySystem(cacheObjectRef.get().o);
			}
			cacheMap.remove(k);
		}
	}
	
	public synchronized void removeAll() {
		if(cacheStateChangeListener != null) {
			cacheStateChangeListener.onDeleteAll();
		}
		cacheMap.clear();
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
		Set<K> keySet = cacheMap.keySet();
		if(Objects.nonNull(keySet) && keySet.size() > 0) {
			for(K key : keySet) {
				CacheObject<V> cacheObjectTemp = cacheMap.get(key).get();
				if(Objects.nonNull(cacheObjectTemp) && cacheObjectTemp.allowAliveTime!=CacheObject.IMMORTAL && System.currentTimeMillis() - cacheObjectTemp.enterTime > cacheObjectTemp.allowAliveTime) {
					CacheObject<V> cacheObject = cacheMap.remove(key).get();
					if(cacheStateChangeListener != null) {
						cacheStateChangeListener.onDeleteBySystem(cacheObject.o);
					}
					cacheObject = null;
				}
			}
		} else {
			System.out.println("keyset size ===> null" );
		}
		}catch(Exception e) {
			e.printStackTrace();
		}

	}
	
	public static void sleep(long m) {
		try {
			Thread.sleep(m);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
//		CacheManager<String,String> cacheManager = new CacheManager<String,String>(5000,TimeUnit.MILLISECONDS,CleanStrategy.USE_FREQUENCY);
		CacheManager<String,String> cacheManager = new CacheManager<String,String>(2,2,5000,TimeUnit.MILLISECONDS);
		
		cacheManager.start();
		cacheManager.add("测试key1", "测试value1");
		sleep(2000);
		cacheManager.add("测试key2", "测试value2");
		sleep(2000);
		cacheManager.add("测试key3", "测试value3");
		sleep(2000);
		cacheManager.add("测试key4", "测试value4");
		sleep(2000);
//		cacheManager.add("测试key5", "测试value5",2356);
//		sleep(2000);
//		cacheManager.add("测试key6", "测试value6",1234);

	}

}
