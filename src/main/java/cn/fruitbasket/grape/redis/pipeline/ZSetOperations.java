package cn.fruitbasket.grape.redis.pipeline;

import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

/**
 * zSet 管道操作
 *
 * @author LiuBing
 * @date 2021/1/25
 */
public class ZSetOperations extends AbstractRedisOperations {

    public Supplier<Boolean> add(String key, String value, double score) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().add(key, value, score));
    }

    public Supplier<Long>
    add(String key, Set<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>> tuples) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().add(key, tuples));
    }

    public Supplier<Long> remove(String key, Object... values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().remove(key, values));
    }

    public Supplier<Double> incrementScore(String key, String value, double delta) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().incrementScore(key, value, delta));
    }

    public Supplier<Double> rank(String key, Object o) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().rank(key, o));
    }

    public Supplier<Long> reverseRank(String key, Object o) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().reverseRank(key, o));
    }

    public Supplier<Set<Long>> range(String key, long start, long end) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().range(key, start, end));
    }

    public Supplier<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>>
    rangeWithScores(String key, long start, long end) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().rangeWithScores(key, start, end));
    }

    public Supplier<Set<String>> rangeByScore(String key, double min, double max) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().rangeByScore(key, min, max));
    }

    public Supplier<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>>
    rangeByScoreWithScores(String key, double min, double max) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().rangeByScoreWithScores(key, min, max));
    }

    public Supplier<Set<String>> rangeByScore(String key, double min, double max, long offset, long count) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().rangeByScore(key, min, max, offset, count));
    }

    public Supplier<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>>
    rangeByScoreWithScores(String key, double min, double max, long offset, long count) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().rangeByScoreWithScores(key, min, max, offset, count));
    }

    public Supplier<Set<String>> rangeByScore(String key, long start, long end) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().reverseRange(key, start, end));
    }

    public Supplier<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>>
    reverseRangeWithScores(String key, long start, long end) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().reverseRangeWithScores(key, start, end));
    }

    public Supplier<Set<String>> reverseRangeByScore(String key, double min, double max) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().reverseRangeByScore(key, min, max));
    }

    public Supplier<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>>
    reverseRangeByScoreWithScores(String key, double min, double max) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().reverseRangeByScoreWithScores(key, min, max));
    }

    public Supplier<Set<String>> reverseRangeByScore(String key, double min, double max, long offset, long count) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().reverseRangeByScore(key, min, max, offset, count));
    }

    public Supplier<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>>
    reverseRangeByScoreWithScores(String key, double min, double max, long offset, long count) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().reverseRangeByScoreWithScores(key, min, max, offset, count));
    }

    public Supplier<Long> count(String key, double min, double max) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().count(key, min, max));
    }

    public Supplier<Long> size(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().size(key));
    }

    public Supplier<Long> zCard(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().zCard(key));
    }

    public Supplier<Double> score(String key, Object o) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().score(key, o));
    }

    public Supplier<Long> removeRange(String key, long start, long end) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().removeRange(key, start, end));
    }

    public Supplier<Long> removeRangeByScore(String key, double min, double max) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().removeRangeByScore(key, min, max));
    }

    public Supplier<Long> unionAndStore(String key, String otherKey, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().unionAndStore(key, otherKey, destKey));
    }

    public Supplier<Long> unionAndStore(String key, Collection<String> otherKeys, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().unionAndStore(key, otherKeys, destKey));
    }

    public Supplier<Long> intersectAndStore(String key, String otherKey, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().intersectAndStore(key, otherKey, destKey));
    }

    public Supplier<Long> intersectAndStore(String key, Collection<String> otherKeys, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().intersectAndStore(key, otherKeys, destKey));
    }

    public Supplier<Cursor<org.springframework.data.redis.core.ZSetOperations.TypedTuple<String>>>
    scan(String key, ScanOptions options) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().scan(key, options));
    }

    public Supplier<Set<String>> rangeByLex(String key, RedisZSetCommands.Range range) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForZSet().rangeByLex(key, range));
    }

    public Supplier<Set<String>>
    rangeByLex(String key, RedisZSetCommands.Range range, RedisZSetCommands.Limit limit) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForZSet().rangeByLex(key, range, limit));
    }

    ZSetOperations(StringRedisPipeline stringRedisPipeline) {
        super(stringRedisPipeline);
    }
}
