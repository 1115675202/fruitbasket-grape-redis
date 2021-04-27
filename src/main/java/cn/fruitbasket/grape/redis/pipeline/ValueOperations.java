package cn.fruitbasket.grape.redis.pipeline;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 字符串 管道操作
 *
 * @author LiuBing
 * @date 2021/1/22
 */
public class ValueOperations extends AbstractRedisOperations {

    public ValueOperations set(String key, String value) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().set(key, value));
        return this;
    }

    public ValueOperations set(String key, String value, long timeout, TimeUnit unit) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().set(key, value, timeout, unit));
        return this;
    }

    public Supplier<Boolean> setIfAbsent(String key, String value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().setIfAbsent(key, value));
    }

    public ValueOperations multiSet(Map<String, String> map) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().multiSet(map));
        return this;
    }

    public Supplier<Boolean> multiSetIfAbsent(Map<String, String> map) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().multiSetIfAbsent(map));
    }

    public Supplier<String> get(Object key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().get(key));
    }

    public Supplier<String> getAndSet(String key, String value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().getAndSet(key, value));
    }

    public Supplier<List<String>> multiGet(Collection<String> keys) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().multiGet(keys));
    }

    public Supplier<Long> increment(String key, long delta) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().increment(key, delta));
    }

    public Supplier<Double> increment(String key, double delta) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().increment(key, delta));
    }

    public Supplier<Integer> append(String key, String value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().append(key, value));
    }

    public Supplier<String> get(String key, long start, long end) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().get(key, start, end));
    }

    public ValueOperations set(String key, String value, long offset) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().set(key, value, offset));
        return this;
    }

    public Supplier<Long> size(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().size(key));
    }

    public Supplier<Boolean> setBit(String key, long offset, boolean value) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().setBit(key, offset, value));
    }

    public Supplier<Boolean> getBit(String key, long offset) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForValue().getBit(key, offset));
    }

    ValueOperations(StringRedisPipeline stringRedisPipeline) {
        super(stringRedisPipeline);
    }
}
