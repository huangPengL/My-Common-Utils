package com.hpl.redis;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * 消息订阅者
 * 只能接收到订阅后发布的消息, 离线的消息无法接收
 *
 * @Author: huangpenglong
 * @Date: 2022/8/23 15:16
 */
public class Subscriber extends Thread {

    private static final Logger log = LoggerFactory.getLogger(Subscriber.class);

    private final Jedis jedis;
    private final JedisPubSub jedisPubSub;

    /* 频道对应的消息处理器 */
    private ListMultimap<String, MessageHandler> handlersMap;

    public Subscriber(Redis redis, final ExecutorService threadPool) {
        this.jedis = redis.getJedis();

        // 当频道消息到达之后，系统自动调用监听器的onMessage方法
        this.jedisPubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                List<MessageHandler> messageHandlers = handlersMap.get(channel);
                if(CollectionUtils.isEmpty(messageHandlers)){
                    return;
                }
                messageHandlers.forEach(handler -> threadPool.execute(() -> handler.handle(message)));
            }
        };
        this.handlersMap = LinkedListMultimap.create(4);
    }

    /* 添加一个消息处理器，如果已经启动该线程会抛出异常 */
    public void addMessageHandler(String channel, MessageHandler messageHandler){
        if(isAlive()){
            throw new UnsupportedOperationException("can't add message handler after subscribe already started.");
        }
        handlersMap.put(channel, messageHandler);
    }

    @Override
    /* 开启所有订阅 */
    public void run() {
        handlersMap = ImmutableListMultimap.copyOf(handlersMap);

        try {
            jedis.subscribe(jedisPubSub, handlersMap.keySet().toArray(new String[0]));
        }catch (JedisConnectionException e) {
            log.warn("{}", new StringBuilder(64).append("ex=").append(e.getClass().getSimpleName()).append(", errmsg=")
                    .append(e.getMessage()));
            jedis.close();
        }
    }

    /* 取消所有订阅，在退出的时候调用 */
    public void unSubscribe(){
        jedisPubSub.unsubscribe(handlersMap.keySet().toArray(new String[0]));
    }
}
