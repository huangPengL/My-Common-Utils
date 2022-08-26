package com.hpl.lock;

import java.util.concurrent.TimeUnit;

/**
 * @Author: huangpenglong
 * @Date: 2022/8/24 18:43
 */
public interface MyLock {

    // 获取锁，若获取成果则返回true, 否则返回false
    boolean tryLock();
    boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

    // 释放锁
    void unLock();
}
