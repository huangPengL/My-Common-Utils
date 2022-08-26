package com.hpl.lock;

import com.hpl.redis.Redis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.params.SetParams;

import java.util.concurrent.TimeUnit;

/**
 *
 * 基于 setnx 实现的 redis 锁, 只实现了tryLock, unlock
 * @Author: huangpenglong
 * @Date: 2022/8/24 18:42
 */
public class SetnxLock implements MyLock{

    private static final Logger log = LoggerFactory.getLogger(SetnxLock.class);

    private static final String DEFAULT_VALUE = "0";
    private static final long DEFAULT_TTL = 3L;         // 超时时间3秒钟

    private final Redis redis;
    private final String key;
    private final String value;
    private final long ttl;

    private boolean locked;

    private SetnxLock(Redis redis, String key, String value, long ttl) {
        this.redis = redis;
        this.key = key;
        this.value = value;
        this.ttl = ttl;
        this.locked = false;
    }

    // 获取一个默认的锁
    public static final SetnxLock defaultLock(Redis redis, String key){
        return new SetnxLock(redis, key, DEFAULT_VALUE, DEFAULT_TTL);
    }

    // 获取自定义的锁
    public static final SetnxLock getLock(Redis redis, String key, String value, long ttl){
        return new SetnxLock(redis, key, value, ttl);
    }

    @Override
    public boolean tryLock() {
        try {
            locked = redis.set(key, value, SetParams.setParams().nx().ex(ttl)) != null;
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }
        return locked;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long end = System.currentTimeMillis() + unit.toMillis(time);
        do{
            if(tryLock()){
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(1L);
        }while (System.currentTimeMillis() > end);
        return false;
    }

    @Override
    public void unLock() {
        if (!locked) {
            return;
        }

        try {
            redis.del(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
