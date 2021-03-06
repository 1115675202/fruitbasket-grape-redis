package cn.fruitbasket.grape.redis.pipeline;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * hash 管道操作
 *
 * @author LiuBing
 * @date 2021/1/22
 */
public class HashOperations extends AbstractRedisOperations {

	public Supplier<Long> delete(String key, Object... hashKeys) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().delete(key, hashKeys));
	}

	public Supplier<Boolean> hasKey(String key, String hashKey) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().hasKey(key, hashKey));
	}

	public Supplier<String> get(String key, String hashKey) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().get(key, hashKey));
	}

	public Supplier<List<String>> multiGet(String key, Collection<String> hashKeys) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().multiGet(key, hashKeys));
	}

	public Supplier<Long> increment(String key, String hashKey, long delta) {
		return this.stringRedisPipeline.addOperation(operations ->
				operations.opsForHash().increment(key, hashKey, delta));
	}

	public Supplier<Double> increment(String key, String hashKey, double delta) {
		return this.stringRedisPipeline.addOperation(operations ->
				operations.opsForHash().increment(key, hashKey, delta));
	}

	public Supplier<Set<String>> keys(String key) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().keys(key));
	}

	public Supplier<Long> size(String key) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().size(key));
	}

	public HashOperations putAll(String key, Map<String, String> m) {
		this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().putAll(key, m));
		return this;
	}

	public HashOperations put(String key, String hashKey, String value) {
		this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().put(key, hashKey, value));
		return this;
	}

	public Supplier<Boolean> putIfAbsent(String key, String hashKey, String value) {
		return this.stringRedisPipeline.addOperation(operations ->
				operations.opsForHash().putIfAbsent(key, hashKey, value));
	}

	public Supplier<List<String>> values(String key) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().values(key));
	}

	public Supplier<Map<String, String>> entries(String key) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().entries(key));
	}

	public Supplier<Cursor<Map.Entry<String, String>>> scan(String key, ScanOptions options) {
		return this.stringRedisPipeline.addOperation(operations -> operations.opsForHash().scan(key, options));
	}

	HashOperations(StringRedisPipeline stringRedisPipeline) {
		super(stringRedisPipeline);
	}
}
