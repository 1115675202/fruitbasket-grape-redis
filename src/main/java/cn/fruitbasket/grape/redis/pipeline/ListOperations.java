package cn.fruitbasket.grape.redis.pipeline;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 列表 管道操作
 *
 * @author LiuBing
 * @date 2021/1/22
 */
public class ListOperations extends AbstractRedisOperations {

    public Supplier<List<String>> range(String key, long start, long end) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().range(key, start, end));
    }

    public ListOperations trim(String key, long start, long end) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForList().trim(key, start, end));
        return this;
    }

    public Supplier<Long> size(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().size(key));
    }

    public Supplier<Long> leftPush(String key, String value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().leftPush(key, value));
    }

    public Supplier<Long> leftPushAll(String key, String... values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().leftPushAll(key, values));
    }

    public Supplier<Long> leftPushAll(String key, Collection<String> values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().leftPushAll(key, values));
    }

    public Supplier<Long> leftPushIfPresent(String key, String value) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForList().leftPushIfPresent(key, value));
    }

    public Supplier<Long> leftPush(String key, String pivot, String value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().leftPush(key, pivot, value));
    }

    public Supplier<Long> rightPush(String key, String value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().rightPush(key, value));
    }

    public Supplier<Long> rightPushAll(String key, String... values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().rightPushAll(key, values));
    }

    public Supplier<Long> rightPushAll(String key, Collection<String> values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().rightPushAll(key, values));
    }

    public Supplier<Long> rightPushIfPresent(String key, String value) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForList().rightPushIfPresent(key, value));
    }

    public Supplier<Long> rightPush(String key, String pivot, String value) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForList().rightPush(key, pivot, value));
    }

    public ListOperations set(String key, long index, String value) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForList().set(key, index, value));
        return this;
    }

    public Supplier<Long> remove(String key, long count, String value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().remove(key, count, value));
    }

    public Supplier<String> index(String key, long index) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().index(key, index));
    }

    public Supplier<String> leftPop(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().leftPop(key));
    }

    public Supplier<String> leftPop(String key, long timeout, TimeUnit unit) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForList().leftPop(key, timeout, unit));
    }

    public Supplier<String> rightPop(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForList().rightPop(key));
    }

    public Supplier<String> rightPop(String key, long timeout, TimeUnit unit) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForList().rightPop(key, timeout, unit));
    }

    public Supplier<String> rightPopAndLeftPush(String sourceKey, String destinationKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForList().rightPopAndLeftPush(sourceKey, destinationKey));
    }

    public Supplier<String> rightPopAndLeftPush(String sourceKey, String destinationKey, long timeout, TimeUnit unit) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit));
    }

    ListOperations(StringRedisPipeline stringRedisPipeline) {
        super(stringRedisPipeline);
    }
}
