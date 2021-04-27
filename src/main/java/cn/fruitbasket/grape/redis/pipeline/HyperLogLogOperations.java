package cn.fruitbasket.grape.redis.pipeline;

import java.util.function.Supplier;

/**
 * @author LiuBing
 * @date 2021/4/27
 */
public class HyperLogLogOperations extends AbstractRedisOperations {

    public Supplier<Long> add(String key, String... values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForHyperLogLog().add(key, values));
    }

    public Supplier<Long> size(String... keys) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForHyperLogLog().size(keys));
    }

    public Supplier<Long> union(String destination, String... sourceKeys) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForHyperLogLog().union(destination, sourceKeys));
    }

    public HyperLogLogOperations delete(String key) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForHyperLogLog().delete(key));
        return this;
    }

    HyperLogLogOperations(StringRedisPipeline stringRedisPipeline) {
        super(stringRedisPipeline);
    }
}
