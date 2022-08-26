package com.hpl.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.SetParams;

public class Redis {

    private final JedisPool jedispool;

    public Redis(GenericObjectPoolConfig<Jedis> redisPoolConfig, String ip, int port, String auth, int timeout) {
        this.jedispool = new JedisPool(redisPoolConfig, ip, port, timeout, auth);
    }

    /**
     * 一次业务需要多次访问 redis 时, 可以取一个 Jedis 来操作, 减少每次从 jedispool 里取 Jedis 的消耗
     * <p>
     * 务必在使用完后调用 <code>close() </code> 方法, 使 jedispool 回收 Jedis
     **/
    public Jedis getJedis() {
        return jedispool.getResource();
    }

    /**
     * Test if the specified key exists. The command returns true if the key exists,
     * otherwise false is returned. Note that even keys set with an empty string as
     * value will return true. Time complexity: O(1)
     *
     * @param key
     * @return Boolean reply, true if the key exists, otherwise false
     */
    public Boolean exists(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.exists(key);
        }
    }

    /**
     * Get the value of the specified key. If the key does not exist the special
     * value 'nil' is returned. If the value stored at key is not a string an error
     * is returned because GET can only handle string values.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @return Bulk reply
     */
    public String get(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.get(key);
        }
    }

    /**
     * Set the string value as value of the key. The string can't be longer than
     * 1073741824 bytes (1 GB).
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param value
     * @return Status code reply
     */
    public String set(final String key, final String value) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.set(key, value);
        }
    }

    /**
     * Set the the respective keys to the respective values. MSET will replace old
     * values with new values, while {@link #msetnx(String...) MSETNX} will not
     * perform any operation at all even if just a single key already exists.
     * <p>
     * Because of this semantic MSETNX can be used in order to set different keys
     * representing different fields of an unique logic object in a way that ensures
     * that either all the fields or none at all are set.
     * <p>
     * Both MSET and MSETNX are atomic operations. This means that for instance if
     * the keys A and B are modified, another client talking to Redis can either see
     * the changes to both A and B at once, or no modification at all.
     *
     * @param keysvalues
     * @return Status code reply Basically +OK as MSET can't fail
     * @see #msetnx(String...)
     */
    public String mset(final String... keysvalues) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.mset(keysvalues);
        }
    }

    /**
     * Get the values of all the specified keys. If one or more keys don't exist or
     * is not of type String, a 'nil' value is returned instead of the value of the
     * specified key, but the operation never fails.
     * <p>
     * Time complexity: O(1) for every key
     *
     * @param keys
     * @return Multi bulk reply
     */
    public List<String> mget(final String... keys) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.mget(keys);
        }
    }

    /**
     * When a key to remove holds a value other than a string, the individual
     * complexity for this key is O(M) where M is the number of elements in the
     * list, set, sorted set or hash. Removing a single key that holds a string
     * value is O(1). Removes the specified keys. A key is ignored if it does not
     * exist. *
     * <p>
     * Time complexity: O(N) where N is the number of keys that will be removed.
     *
     * @param key
     * @return Return value Integer reply: The number of keys that were removed.
     */
    public Long del(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.del(key);
        }
    }

    /**
     * Remove the specified keys. If a given key does not exist no operation is
     * performed for this key. The command returns the number of keys removed. Time
     * complexity: O(1)
     *
     * @param keys
     * @return Integer reply, specifically: an integer greater than 0 if one or more
     *         keys were removed 0 if none of the specified key existed
     */
    public Long del(final String... keys) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.del(keys);
        }
    }

    /**
     * Return the score of the specified element of the sorted set at key. If the
     * specified element does not exist in the sorted set, or the key does not exist
     * at all, a special 'nil' value is returned.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param member
     * @return the score
     */
    public Double zscore(final String key, final String member) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zscore(key, member);
        }
    }

    /**
     * If key holds a hash, retrieve the value associated to the specified field.
     * <p>
     * If the field is not found or the key does not exist, a special 'nil' value is
     * returned.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param field
     * @return Bulk reply
     */
    public String hget(final String key, final String field) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hget(key, field);
        }
    }

    /**
     * Return all the fields and associated values in a hash.
     * <p>
     * <b>Time complexity:</b> O(N), where N is the total number of entries
     *
     * @param key
     * @return All the fields and values contained into a hash.
     */
    public Map<String, String> hgetAll(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hgetAll(key);
        }
    }

    /**
     * Set the specified hash field to the specified value.
     * <p>
     * If key does not exist, a new key holding a hash is created.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param field
     * @param value
     * @return If the field already exists, and the HSET just produced an update of
     *         the value, 0 is returned, otherwise if a new field is created 1 is
     *         returned.
     */
    public Long hset(final String key, final String field, final String value) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hset(key, field, value);
        }
    }

    /**
     * Add the specified member having the specified score to the sorted set stored
     * at key. If member is already a member of the sorted set the score is updated,
     * and the element reinserted in the right position to ensure sorting. If key
     * does not exist a new sorted set with the specified member as sole member is
     * created. If the key exists but does not hold a sorted set value an error is
     * returned.
     * <p>
     * The score value can be the string representation of a double precision
     * floating point number.
     * <p>
     * Time complexity O(log(N)) with N being the number of elements in the sorted
     * set
     *
     * @param key
     * @param score
     * @param member
     * @return Integer reply, specifically: 1 if the new element was added 0 if the
     *         element was already a member of the sorted set and the score was
     *         updated
     */
    public Long zadd(final String key, final double score, final String member) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zadd(key, score, member);
        }
    }

    public Long zadd(final String key, final Map<String, Double> map) {

        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zadd(key, map);
        }
    }

    /**
     * Increment the number stored at key by one. If the key does not exist or
     * contains a value of a wrong type, set the key to the value of "0" before to
     * perform the increment operation.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are not
     * "integer" types. Simply the string stored at the key is parsed as a base 10
     * 64 bit signed integer, incremented, and then converted back as a string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @return Integer reply, this commands will reply with the new value of key
     *         after the increment.
     * @see #incrBy(String, long)
     * @see #decr(String)
     * @see #decrBy(String, long)
     */
    public Long incr(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.incr(key);
        }
    }

    /**
     * INCRBY work just like {@link #incr(String) INCR} but instead to increment by
     * 1 the increment is integer.
     * <p>
     * INCR commands are limited to 64 bit signed integers.
     * <p>
     * Note: this is actually a string operation, that is, in Redis there are not
     * "integer" types. Simply the string stored at the key is parsed as a base 10
     * 64 bit signed integer, incremented, and then converted back as a string.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param increment
     * @return Integer reply, this commands will reply with the new value of key
     *         after the increment.
     * @see #incr(String)
     * @see #decr(String)
     * @see #decrBy(String, long)
     */
    public Long incrBy(final String key, final long increment) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.incrBy(key, increment);
        }
    }

    /**
     * Set a timeout on the specified key. After the timeout the key will be
     * automatically deleted by the server. A key with an associated timeout is said
     * to be volatile in Redis terminology.
     * <p>
     * Volatile keys are stored on disk like the other keys, the timeout is
     * persistent too like all the other aspects of the dataset. Saving a dataset
     * containing expires and stopping the server does not stop the flow of time as
     * Redis stores on disk the time when the key will no longer be available as
     * Unix time, and not the remaining seconds.
     * <p>
     * Since Redis 2.1.3 you can update the value of the timeout of a key already
     * having an expire set. It is also possible to undo the expire at all turning
     * the key into a normal key using the {@link #persist(String) PERSIST} command.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param seconds
     * @return Integer reply, specifically: 1: the timeout was set. 0: the timeout
     *         was not set since the key already has an associated timeout (this may
     *         happen only in Redis versions &lt; 2.1.3, Redis &gt;= 2.1.3 will
     *         happily update the timeout), or the key does not exist.
     * @see <a href="http://redis.io/commands/expire">Expire Command</a>
     */
    public Long expire(final String key, final long seconds) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.expire(key, seconds);
        }

    }

    /**
     * Return the rank (or index) of member in the sorted set at key, with scores
     * being ordered from low to high.
     * <p>
     * When the given member does not exist in the sorted set, the special value
     * 'nil' is returned. The returned rank (or index) of the member is 0-based for
     * both commands.
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))
     *
     * @param key
     * @param member
     * @return Integer reply or a nil bulk reply, specifically: the rank of the
     *         element as an integer reply if the element exists. A nil bulk reply
     *         if there is no such element.
     * @see #zrevrank(String, String)
     */
    public Long zrank(final String key, final String member) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrank(key, member);
        }
    }

    /**
     * Return the rank (or index) of member in the sorted set at key, with scores
     * being ordered from high to low.
     * <p>
     * When the given member does not exist in the sorted set, the special value
     * 'nil' is returned. The returned rank (or index) of the member is 0-based for
     * both commands.
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))
     *
     * @param key
     * @param member
     * @return Integer reply or a nil bulk reply, specifically: the rank of the
     *         element as an integer reply if the element exists. A nil bulk reply
     *         if there is no such element.
     * @see #zrank(String, String)
     */
    public Long zrevrank(final String key, final String member) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrevrank(key, member);
        }
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key.
     * <p>
     * ZRANGE can perform different types of range queries: by index (rank), by the
     * score, or by lexicographical order.
     * <p>
     * Time complexity: O(log(N)+M) with N being the number of elements in the
     * sorted set and M the number of elements returned.
     *
     * @param key
     * @param start
     * @param stop
     * @return list of elements in the specified range (optionally with their
     *         scores, in case the WITHSCORES option is given).
     */
    public Set<String> zrange(final String key, final long start, final long stop) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrange(key, start, stop);
        }
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key. The
     * elements are considered to be ordered from the highest to the lowest score.
     * Descending lexicographical order is used for elements with equal score.
     * <p>
     * Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
     * <p>
     * Time complexity: O(log(N)+M) with N being the number of elements in the
     * sorted set and M the number of elements returned.
     *
     * @param key
     * @param start
     * @param stop
     * @return list of elements in the specified range (optionally with their
     *         scores).
     */
    public Set<String> zrevrange(final String key, final long start, final long stop) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrevrange(key, start, stop);
        }
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key.
     * <p>
     * ZRANGE can perform different types of range queries: by index (rank), by the
     * score, or by lexicographical order.
     * <p>
     * The optional WITHSCORES argument supplements the command's reply with the
     * scores of elements returned. The returned list contains
     * value1,score1,...,valueN,scoreN instead of value1,...,valueN. Client
     * libraries are free to return a more appropriate data type (suggestion: an
     * array with (value, score) arrays/tuples).
     * <p>
     * Time complexity: O(log(N)+M) with N being the number of elements in the
     * sorted set and M the number of elements returned.
     *
     * @param key
     * @param start
     * @param stop
     * @return list of elements in the specified range (optionally with their
     *         scores, in case the WITHSCORES option is given).
     */
    public Set<Tuple> zrangeWithScores(final String key, final long start, final long stop) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrangeWithScores(key, start, stop);
        }
    }

    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrangeByScore(key, min, max);
        }
    }

    /**
     * Return the all the elements in the sorted set at key with a score between min
     * and max (including elements with score equal to min or max).
     * <p>
     * The elements having the same score are returned sorted lexicographically as
     * ASCII strings (this follows from a property of Redis sorted sets and does not
     * involve further computation).
     * <p>
     * Using the optional {@link #zrangeByScore(String, double, double, int, int)
     * LIMIT} it's possible to get only a range of the matching elements in an
     * SQL-alike way. Note that if offset is large the commands needs to traverse
     * the list for offset elements and this adds up to the O(M) figure.
     * <p>
     * The {@link #zcount(String, double, double) ZCOUNT} command is similar to
     * {@link #zrangeByScore(String, double, double) ZRANGEBYSCORE} but instead of
     * returning the actual elements in the specified interval, it just returns the
     * number of matching elements.
     * <p>
     * <b>Exclusive intervals and infinity</b>
     * <p>
     * min and max can be -inf and +inf, so that you are not required to know what's
     * the greatest or smallest element in order to take, for instance, elements "up
     * to a given value".
     * <p>
     * Also while the interval is for default closed (inclusive) it's possible to
     * specify open intervals prefixing the score with a "(" character, so for
     * instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (1.3 5}
     * <p>
     * Will return all the values with score &gt; 1.3 and &lt;= 5, while for
     * instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (5 (10}
     * <p>
     * Will return all the values with score &gt; 5 and &lt; 10 (5 and 10 excluded).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and M
     * the number of elements returned by the command, so if M is constant (for
     * instance you always ask for the first ten elements with LIMIT) you can
     * consider it O(log(N))
     *
     * @param key
     * @param min a double or Double.NEGATIVE_INFINITY for "-inf"
     * @param max a double or Double.POSITIVE_INFINITY for "+inf"
     * @return Multi bulk reply specifically a list of elements in the specified
     *         score range.
     * @see #zrangeByScore(String, double, double)
     * @see #zrangeByScore(String, double, double, int, int)
     * @see #zrangeByScoreWithScores(String, double, double)
     * @see #zrangeByScoreWithScores(String, String, String)
     * @see #zrangeByScoreWithScores(String, double, double, int, int)
     * @see #zcount(String, double, double)
     */
    public Set<String> zrangeByScore(final String key, final long min, final long max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrangeByScore(key, min, max);
        }
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key. The
     * elements are considered to be ordered from the highest to the lowest score.
     * Descending lexicographical order is used for elements with equal score.
     * <p>
     * Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
     * <p>
     * The optional WITHSCORES argument supplements the command's reply with the
     * scores of elements returned. The returned list contains
     * value1,score1,...,valueN,scoreN instead of value1,...,valueN. Client
     * libraries are free to return a more appropriate data type (suggestion: an
     * array with (value, score) arrays/tuples).
     * <p>
     * Time complexity: O(log(N)+M) with N being the number of elements in the
     * sorted set and M the number of elements returned.
     *
     * @param key
     * @param start
     * @param stop
     * @return list of elements in the specified range (optionally with their
     *         scores).
     */
    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long stop) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrevrangeWithScores(key, start, stop);
        }
    }

    /**
     * Remove the specified member from the sorted set value stored at key. If
     * member was not a member of the set no operation is performed. If key does not
     * not hold a set value an error is returned.
     * <p>
     * Time complexity O(log(N)) with N being the number of elements in the sorted
     * set
     *
     * @param key
     * @param members
     * @return Integer reply, specifically: 1 if the new element was removed 0 if
     *         the new element was not a member of the set
     */
    public Long zrem(final String key, final String... members) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrem(key, members);
        }
    }

    /**
     * Return the sorted set cardinality (number of elements). If the key does not
     * exist 0 is returned, like for empty sorted sets.
     * <p>
     * Time complexity O(1)
     *
     * @param key
     * @return the cardinality (number of elements) of the set as an integer.
     */
    public Long zcard(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zcard(key);
        }
    }

    /**
     * Set the respective fields to the respective values. HMSET replaces old values
     * with new values.
     * <p>
     * If key does not exist, a new key holding a hash is created.
     * <p>
     * <b>Time complexity:</b> O(N) (with N being the number of fields)
     *
     * @param key
     * @param hash
     * @return Return OK or Exception if hash is empty
     */
    public String hmset(final String key, final Map<String, String> hash) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hmset(key, hash);
        }
    }

    /**
     * Retrieve the values associated to the specified fields.
     * <p>
     * If some of the specified fields do not exist, nil values are returned. Non
     * existing keys are considered like empty hashes.
     * <p>
     * <b>Time complexity:</b> O(N) (with N being the number of fields)
     *
     * @param key
     * @param fields
     * @return Multi Bulk Reply specifically a list of all the values associated
     *         with the specified fields, in the same order of the request.
     */
    public List<String> hmget(final String key, final String... fields) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hmget(key, fields);
        }
    }

    /**
     * Remove the specified field from an hash stored at key.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param fields
     * @return If the field was present in the hash it is deleted and 1 is returned,
     *         otherwise 0 is returned and no operation is performed.
     */
    public Long hdel(final String key, final String... fields) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hdel(key, fields);
        }
    }

    /**
     * Set the string value as value of the key. The string can't be longer than
     * 1073741824 bytes (1 GB).
     *
     * @param key
     * @param value
     * @param params NX|XX, NX -- Only set the key if it does not already exist. XX
     *               -- Only set the key if it already exist. EX|PX, expire time
     *               units: EX = seconds; PX = milliseconds
     * @return Status code reply: OK if set value success, and nil if use NX and key
     *         already exists
     */
    public String set(final String key, final String value, final SetParams params) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.set(key, value, params);
        }
    }

    /**
     * The TTL command returns the remaining time to live in seconds of a key that
     * has an {@link #expire(String, int) EXPIRE} set. This introspection capability
     * allows a Redis client to check how many seconds a given key will continue to
     * be part of the dataset.
     *
     * @param key
     * @return Integer reply, returns the remaining time to live in seconds of a key
     *         that has an EXPIRE. In Redis 2.6 or older, if the Key does not exists
     *         or does not have an associated expire, -1 is returned. In Redis 2.8
     *         or newer, if the Key does not have an associated expire, -1 is
     *         returned or if the Key does not exists, -2 is returned.
     */
    public Long ttl(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.ttl(key);
        }
    }

    public Set<String> keys(final String pattern) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.keys(pattern);
        }
    }

    public ScanResult<String> scan(final String cursor) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.scan(cursor);
        }
    }

    public ScanResult<String> scan(final String cursor, final ScanParams params) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.scan(cursor, params);
        }
    }

    /**
     * Return true if member is a member of the set stored at key, otherwise false
     * is returned.
     * <p>
     * Time complexity O(1)
     *
     * @param key
     * @param member
     * @return Boolean reply, specifically: true if the element is a member of the
     *         set false if the element is not a member of the set OR if the key
     *         does not exist
     */
    public Boolean sismember(final String key, final String member) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.sismember(key, member);
        }
    }

    /**
     * Add the specified member to the set value stored at key. If member is already
     * a member of the set no operation is performed. If key does not exist a new
     * set with the specified member as sole member is created. If the key exists
     * but does not hold a set value an error is returned.
     * <p>
     * Time complexity O(1)
     *
     * @param key
     * @param members
     * @return Integer reply, specifically: 1 if the new element was added 0 if the
     *         element was already a member of the set
     */
    public Long sadd(final String key, final String... members) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.sadd(key, members);
        }
    }

    /**
     * Return all the members (elements) of the set value stored at key. This is
     * just syntax glue for {@link #sinter(String...) SINTER}.
     * <p>
     * Time complexity O(N)
     *
     * @param key
     * @return Multi bulk reply
     */
    public Set<String> smembers(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.smembers(key);
        }
    }

    /**
     * Remove the specified member from the set value stored at key. If member was
     * not a member of the set no operation is performed. If key does not hold a set
     * value an error is returned.
     * <p>
     * Time complexity O(1)
     *
     * @param key
     * @param members
     * @return Integer reply, specifically: 1 if the new element was removed 0 if
     *         the new element was not a member of the set
     */
    public Long srem(final String key, final String... members) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.srem(key, members);
        }
    }

    /**
     * The command is exactly equivalent to the following group of commands:
     * {@link #set(String, String) SET} + {@link #expire(String, int) EXPIRE}. The
     * operation is atomic.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param seconds
     * @param value
     * @return Status code reply
     */
    public String setex(final String key, final long seconds, final String value) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.setex(key, seconds, value);
        }
    }

    /**
     * SETNX works exactly like {@link #set(String, String) SET} with the only
     * difference that if the key already exists no operation is performed. SETNX
     * actually means "SET if Not eXists".
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param value
     * @return Integer reply, specifically: 1 if the key was set 0 if the key was
     *         not set
     */
    public Long setnx(final String key, final String value) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.setnx(key, value);
        }
    }

    public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hscan(key, cursor);
        }
    }

    public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor, final ScanParams params) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hscan(key, cursor, params);
        }
    }

    public ScanResult<Tuple> zscan(final String key, final String cursor) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zscan(key, cursor);
        }
    }

    public ScanResult<Tuple> zscan(final String key, final String cursor, final ScanParams params) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zscan(key, cursor, params);
        }
    }

    /**
     * Return the number of items in a hash.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @return The number of entries (fields) contained in the hash stored at key.
     *         If the specified key does not exist, 0 is returned assuming an empty
     *         hash.
     */
    public Long hlen(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hlen(key);
        }
    }

    /**
     * Return the all the elements in the sorted set at key with a score between min
     * and max (including elements with score equal to min or max).
     * <p>
     * The elements having the same score are returned sorted lexicographically as
     * ASCII strings (this follows from a property of Redis sorted sets and does not
     * involve further computation).
     * <p>
     * Using the optional {@link #zrangeByScore(String, double, double, int, int)
     * LIMIT} it's possible to get only a range of the matching elements in an
     * SQL-alike way. Note that if offset is large the commands needs to traverse
     * the list for offset elements and this adds up to the O(M) figure.
     * <p>
     * The {@link #zcount(String, double, double) ZCOUNT} command is similar to
     * {@link #zrangeByScore(String, double, double) ZRANGEBYSCORE} but instead of
     * returning the actual elements in the specified interval, it just returns the
     * number of matching elements.
     * <p>
     * <b>Exclusive intervals and infinity</b>
     * <p>
     * min and max can be -inf and +inf, so that you are not required to know what's
     * the greatest or smallest element in order to take, for instance, elements "up
     * to a given value".
     * <p>
     * Also while the interval is for default closed (inclusive) it's possible to
     * specify open intervals prefixing the score with a "(" character, so for
     * instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (1.3 5}
     * <p>
     * Will return all the values with score &gt; 1.3 and &lt;= 5, while for
     * instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (5 (10}
     * <p>
     * Will return all the values with score &gt; 5 and &lt; 10 (5 and 10 excluded).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and M
     * the number of elements returned by the command, so if M is constant (for
     * instance you always ask for the first ten elements with LIMIT) you can
     * consider it O(log(N))
     *
     * @param key
     * @param min
     * @param max
     * @return Multi bulk reply specifically a list of elements in the specified
     *         score range.
     * @see #zrangeByScore(String, double, double)
     * @see #zrangeByScore(String, double, double, int, int)
     * @see #zrangeByScoreWithScores(String, double, double)
     * @see #zrangeByScoreWithScores(String, double, double, int, int)
     * @see #zcount(String, double, double)
     */
    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrangeByScoreWithScores(key, min, max);
        }
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrangeByScoreWithScores(key, min, max);
        }
    }

    /**
     * Remove all the elements in the sorted set at key with a score between min and
     * max (including elements with score equal to min or max).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and M
     * the number of elements removed by the operation
     *
     * @param key
     * @param min
     * @param max
     * @return Integer reply, specifically the number of elements removed.
     */
    public Long zremrangeByScore(final String key, final double min, final double max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zremrangeByScore(key, min, max);
        }
    }

    public Long zremrangeByScore(final String key, final String min, final String max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zremrangeByScore(key, min, max);
        }
    }

    public Long publish(final String channel, final String message) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.publish(channel, message);
        }
    }

    /**
     * Increment the number stored at field in the hash at key by value. If key does
     * not exist, a new key holding a hash is created. If field does not exist or
     * holds a string, the value is set to 0 before applying the operation. Since
     * the value argument is signed you can use this command to perform both
     * increments and decrements.
     * <p>
     * The range of values supported by HINCRBY is limited to 64 bit signed
     * integers.
     * <p>
     * <b>Time complexity:</b> O(1)
     *
     * @param key
     * @param field
     * @param value
     * @return Integer reply The new value at field after the increment operation.
     */
    public Long hincrBy(final String key, final String field, final long value) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.hincrBy(key, field, value);
        }
    }

    /**
     * Sets or clears the bit at offset in the string value stored at key
     *
     * @param key
     * @param offset
     * @param value
     * @return
     */
    public Boolean setbit(final String key, final long offset, final boolean value) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.setbit(key, offset, value);
        }
    }

    public Boolean setbit(final String key, final long offset, final String value) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.setbit(key, offset, value);
        }
    }

    /**
     * Returns the bit value at offset in the string value stored at key
     *
     * @param key
     * @param offset
     * @return
     */
    public Boolean getbit(final String key, final long offset) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.getbit(key, offset);
        }
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrevrangeByScore(key, max, min);
        }
    }

    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrevrangeByScore(key, max, min);
        }
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset,
            final int count) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrevrangeByScore(key, max, min, offset, count);
        }
    }

    /**
     * If member already exists in the sorted set adds the increment to its score
     * and updates the position of the element in the sorted set accordingly. If
     * member does not already exist in the sorted set it is added with increment as
     * score (that is, like if the previous score was virtually zero). If key does
     * not exist a new sorted set with the specified member as sole member is
     * created. If the key exists but does not hold a sorted set value an error is
     * returned.
     * <p>
     * The score value can be the string representation of a double precision
     * floating point number. It's possible to provide a negative value to perform a
     * decrement.
     * <p>
     * For an introduction to sorted sets check the Introduction to Redis data types
     * page.
     * <p>
     * Time complexity O(log(N)) with N being the number of elements in the sorted
     * set
     *
     * @param key
     * @param increment
     * @param member
     * @return The new score
     */
    public Double zincrby(final String key, final double increment, final String member) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zincrby(key, increment, member);
        }
    }

    /**
     * Add the string value to the head (LPUSH) or tail (RPUSH) of the list stored
     * at key. If the key does not exist an empty list is created just before the
     * append operation. If the key exists but is not a List an error is returned.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param strings
     * @return Integer reply, specifically, the number of elements inside the list
     *         after the push operation.
     */
    public Long rpush(final String key, final String... strings) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.rpush(key, strings);
        }
    }

    /**
     * Add the string value to the head (LPUSH) or tail (RPUSH) of the list stored
     * at key. If the key does not exist an empty list is created just before the
     * append operation. If the key exists but is not a List an error is returned.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @param strings
     * @return Integer reply, specifically, the number of elements inside the list
     *         after the push operation.
     */
    public Long lpush(final String key, final String... strings) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.lpush(key, strings);
        }
    }

    /**
     * Atomically return and remove the first (LPOP) or last (RPOP) element of the
     * list. For example if the list contains the elements "a","b","c" RPOP will
     * return "c" and the list will become "a","b".
     * <p>
     * If the key does not exist or the list is already empty the special value
     * 'nil' is returned.
     *
     * @param key
     * @return Bulk reply
     * @see #lpop(String)
     */
    public String rpop(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.rpop(key);
        }
    }

    /**
     * Atomically return and remove the first (LPOP) or last (RPOP) element of the
     * list. For example if the list contains the elements "a","b","c" LPOP will
     * return "a" and the list will become "b","c".
     * <p>
     * If the key does not exist or the list is already empty the special value
     * 'nil' is returned.
     *
     * @param key
     * @return Bulk reply
     * @see #rpop(String)
     */
    public String lpop(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.lpop(key);
        }
    }

    public Long bitcount(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.bitcount(key);
        }
    }

    public Long bitcount(final String key, final long start, final long end) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.bitcount(key, start, end);
        }
    }

    /**
     * Return the specified elements of the list stored at the specified key. Start
     * and end are zero-based indexes. 0 is the first element of the list (the list
     * head), 1 the next element and so on.
     * <p>
     * For example LRANGE foobar 0 2 will return the first three elements of the
     * list.
     * <p>
     * start and end can also be negative numbers indicating offsets from the end of
     * the list. For example -1 is the last element of the list, -2 the penultimate
     * element and so on.
     * <p>
     * <b>Consistency with range functions in various programming languages</b>
     * <p>
     * Note that if you have a list of numbers from 0 to 100, LRANGE 0 10 will
     * return 11 elements, that is, rightmost item is included. This may or may not
     * be consistent with behavior of range-related functions in your programming
     * language of choice (think Ruby's Range.new, Array#slice or Python's range()
     * function).
     * <p>
     * LRANGE behavior is consistent with one of Tcl.
     * <p>
     * <b>Out-of-range indexes</b>
     * <p>
     * Indexes out of range will not produce an error: if start is over the end of
     * the list, or start &gt; end, an empty list is returned. If end is over the
     * end of the list Redis will threat it just like the last element of the list.
     * <p>
     * Time complexity: O(start+n) (with n being the length of the range and start
     * being the start offset)
     * 
     * @param key
     * @param start
     * @param stop
     * @return Multi bulk reply, specifically a list of elements in the specified
     *         range.
     */
    public List<String> lrange(final String key, final long start, final long end) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.lrange(key, start, end);
        }
    }

    /**
     * Trim an existing list so that it will contain only the specified range of
     * elements specified. Start and end are zero-based indexes. 0 is the first
     * element of the list (the list head), 1 the next element and so on.
     * <p>
     * For example LTRIM foobar 0 2 will modify the list stored at foobar key so
     * that only the first three elements of the list will remain.
     * <p>
     * start and end can also be negative numbers indicating offsets from the end of
     * the list. For example -1 is the last element of the list, -2 the penultimate
     * element and so on.
     * <p>
     * Indexes out of range will not produce an error: if start is over the end of
     * the list, or start &gt; end, an empty list is left as value. If end over the
     * end of the list Redis will threat it just like the last element of the list.
     * <p>
     * Hint: the obvious use of LTRIM is together with LPUSH/RPUSH. For example:
     * <p>
     * {@code lpush("mylist", "someelement"); ltrim("mylist", 0, 99); * }
     * <p>
     * The above two commands will push elements in the list taking care that the
     * list will not grow without limits. This is very useful when using Redis to
     * store logs for example. It is important to note that when used in this way
     * LTRIM is an O(1) operation because in the average case just one element is
     * removed from the tail of the list.
     * <p>
     * Time complexity: O(n) (with n being len of list - len of range)
     * 
     * @param key
     * @param start
     * @param stop
     * @return Status code reply
     */
    public String ltrim(final String key, final long start, final long end) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.ltrim(key, start, end);
        }
    }

    /**
     * Return the set cardinality (number of elements). If the key does not exist 0
     * is returned, like for empty sets.
     * 
     * @param key
     * @return Integer reply, specifically: the cardinality (number of elements) of
     *         the set as an integer.
     */
    public Long scard(final String key) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.scard(key);
        }
    }

    public Long zcount(final String key, final double min, final double max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zcount(key, min, max);
        }
    }

    public Long zcount(final String key, final String min, final String max) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zcount(key, min, max);
        }
    }

    /**
     * Return the all the elements in the sorted set at key with a score between min
     * and max (including elements with score equal to min or max).
     * <p>
     * The elements having the same score are returned sorted lexicographically as
     * ASCII strings (this follows from a property of Redis sorted sets and does not
     * involve further computation).
     * <p>
     * Using the optional {@link #zrangeByScore(String, double, double, int, int)
     * LIMIT} it's possible to get only a range of the matching elements in an
     * SQL-alike way. Note that if offset is large the commands needs to traverse
     * the list for offset elements and this adds up to the O(M) figure.
     * <p>
     * The {@link #zcount(String, double, double) ZCOUNT} command is similar to
     * {@link #zrangeByScore(String, double, double) ZRANGEBYSCORE} but instead of
     * returning the actual elements in the specified interval, it just returns the
     * number of matching elements.
     * <p>
     * <b>Exclusive intervals and infinity</b>
     * <p>
     * min and max can be -inf and +inf, so that you are not required to know what's
     * the greatest or smallest element in order to take, for instance, elements "up
     * to a given value".
     * <p>
     * Also while the interval is for default closed (inclusive) it's possible to
     * specify open intervals prefixing the score with a "(" character, so for
     * instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (1.3 5}
     * <p>
     * Will return all the values with score &gt; 1.3 and &lt;= 5, while for
     * instance:
     * <p>
     * {@code ZRANGEBYSCORE zset (5 (10}
     * <p>
     * Will return all the values with score &gt; 5 and &lt; 10 (5 and 10 excluded).
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(log(N))+O(M) with N being the number of elements in the sorted set and M
     * the number of elements returned by the command, so if M is constant (for
     * instance you always ask for the first ten elements with LIMIT) you can
     * consider it O(log(N))
     * 
     * @see #zrangeByScore(String, double, double)
     * @see #zrangeByScore(String, double, double, int, int)
     * @see #zrangeByScoreWithScores(String, double, double)
     * @see #zrangeByScoreWithScores(String, double, double, int, int)
     * @see #zcount(String, double, double)
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return Multi bulk reply specifically a list of elements in the specified
     *         score range.
     */
    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset,
            final int count) {
        try (Jedis jedis = jedispool.getResource()) {
            return jedis.zrangeByScore(key, min, max, offset, count);
        }
    }

}
