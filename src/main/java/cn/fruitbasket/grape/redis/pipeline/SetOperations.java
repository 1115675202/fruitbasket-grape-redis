package cn.fruitbasket.grape.redis.pipeline;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * set 管道操作
 *
 * @author LiuBing
 * @date 2021/1/25
 */
public class SetOperations extends AbstractRedisOperations {

    public Supplier<Long> add(String key, String... values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().add(key, values));
    }

    public Supplier<Long> remove(String key, Object... values) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().remove(key, values));
    }

    public SetOperations pop(String key) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().pop(key));
        return this;
    }

    public Supplier<Boolean> move(String key, String value, String destKey) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().move(key, value, destKey));
    }

    public Supplier<Long> size(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().size(key));
    }

    public Supplier<Boolean> isMember(String key, Object o) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().isMember(key, o));
    }

    public Supplier<Set<String>> intersect(String key, String otherKey) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().intersect(key, otherKey));
    }

    public Supplier<Set<String>> intersect(String key, Collection<String> otherKeys) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().intersect(key, otherKeys));
    }

    public Supplier<Long> intersectAndStore(String key, String otherKey, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForSet().intersectAndStore(key, otherKey, destKey));
    }

    public Supplier<Long> intersectAndStore(String key, Collection<String> otherKeys, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForSet().intersectAndStore(key, otherKeys, destKey));
    }

    public Supplier<Set<String>> union(String key, String otherKey) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().union(key, otherKey));
    }

    public Supplier<Set<String>> union(String key, Collection<String> otherKeys) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().union(key, otherKeys));
    }

    public Supplier<Long> unionAndStore(String key, String otherKey, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForSet().unionAndStore(key, otherKey, destKey));
    }

    public Supplier<Long> unionAndStore(String key, Collection<String> otherKeys, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForSet().unionAndStore(key, otherKeys, destKey));
    }

    public Supplier<Set<String>> difference(String key, String otherKey) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().difference(key, otherKey));
    }

    public Supplier<Set<String>> difference(String key, Collection<String> otherKeys) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().difference(key, otherKeys));
    }

    public Supplier<Long> differenceAndStore(String key, String otherKey, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForSet().differenceAndStore(key, otherKey, destKey));
    }

    public Supplier<Long> differenceAndStore(String key, Collection<String> otherKeys, String destKey) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForSet().differenceAndStore(key, otherKeys, destKey));
    }

    public Supplier<Set<String>> members(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().members(key));
    }

    public SetOperations randomMember(String key) {
        this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().randomMember(key));
        return this;
    }

    public Supplier<Set<String>> distinctRandomMembers(String key, long count) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForSet().distinctRandomMembers(key, count));
    }

    public Supplier<List<String>> randomMembers(String key, long count) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().randomMembers(key, count));
    }

    public Supplier<Cursor<String>> scan(String key, ScanOptions options) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForSet().scan(key, options));
    }

    SetOperations(StringRedisPipeline stringRedisPipeline) {
        super(stringRedisPipeline);
    }
}
