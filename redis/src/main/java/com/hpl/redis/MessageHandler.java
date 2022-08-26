package com.hpl.redis;

/**
 * @Author: huangpenglong
 * @Date: 2022/8/23 15:24
 */
public interface MessageHandler {

    /** 处理接收到的消息 **/
    void handle(String message);
}
