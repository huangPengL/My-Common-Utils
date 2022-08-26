package com.hpl.redis;

import com.alibaba.fastjson.JSON;

/**
 * @Author: huangpenglong
 * @Date: 2022/8/23 16:36
 */
public class Publisher {

    private final String channel;
    private final Redis redis;

    private Publisher(String channel, Redis redis) {
        this.channel = channel;
        this.redis = redis;
    }


    /** 返回一个指定channel的发布者 **/
    public static final Publisher of(String channel, Redis redis){
        return new Publisher(channel, redis);
    }

    /* 发布文本消息 */
    public Long publish(String message){
        return redis.publish(channel, message);
    }

    /* 发布json消息 */
    public Long publishJson(Object obj){
        return redis.publish(channel, JSON.toJSONString(obj));
    }

}
