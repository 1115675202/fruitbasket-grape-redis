package cn.fruitbasket.grape.redis.pipeline;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.hash.HashMapper;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author LiuBing
 * @date 2021/4/27
 */
public class StreamOperations extends AbstractRedisOperations {

    public Supplier<Long> acknowledge(String key, String group, String... recordIds) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().acknowledge(key, group, recordIds));
    }

    public Supplier<Long> acknowledge(String key, String group, RecordId... recordIds) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().acknowledge(key, group, recordIds));
    }

    public Supplier<Long> acknowledge(String group, Record<String, ?> record) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().acknowledge(group, record));
    }

    public Supplier<RecordId> add(String key, Map<? extends String, ? extends String> content) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().add(key, content));
    }

    public Supplier<RecordId> add(MapRecord<String, ? extends String, ? extends String> record) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().add(record));
    }

    public Supplier<RecordId> add(Record<String, ?> record) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().add(record));
    }

    public Supplier<Long> delete(String key, String... recordIds) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().delete(key, recordIds));
    }

    public Supplier<Long> delete(Record<String, ?> record) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().delete(record));
    }

    public Supplier<Long> delete(String key, RecordId... recordIds) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().delete(key, recordIds));
    }

    public Supplier<String> createGroup(String key, String group) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().createGroup(key, group));
    }

    public Supplier<String> createGroup(String key, ReadOffset readOffset, String group) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().createGroup(key, readOffset, group));
    }

    public Supplier<Boolean> deleteConsumer(String key, Consumer consumer) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().deleteConsumer(key, consumer));
    }

    public Supplier<Boolean> destroyGroup(String key, String group) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().destroyGroup(key, group));
    }

    public Supplier<StreamInfo.XInfoConsumers> consumers(String key, String group) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().consumers(key, group));
    }

    public Supplier<StreamInfo.XInfoConsumers> groups(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().groups(key));
    }

    public Supplier<StreamInfo.XInfoStream> info(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().info(key));
    }

    public Supplier<PendingMessagesSummary> pending(String key, String group) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().pending(key, group));
    }

    public Supplier<PendingMessages> pending(String key, Consumer consumer) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().pending(key, consumer));
    }

    public Supplier<PendingMessages> pending(String key, String group, Range<?> range, long count) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().pending(key, group, range, count));
    }

    public Supplier<PendingMessages> pending(String key, Consumer consumer, Range<?> range, long count) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().pending(key, consumer, range, count));
    }

    public Supplier<Long> size(String key) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().size(key));
    }

    public Supplier<List<MapRecord<String, String, String>>> range(String key, Range<String> range) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().range(key, range));
    }

    public Supplier<List<MapRecord<String, String, String>>>
    range(String key, Range<String> range, RedisZSetCommands.Limit limit) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().range(key, range, limit));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>> range(Class<V> targetType, String key, Range<String> range) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().range(targetType, key, range));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>>
    range(Class<V> targetType, String key, Range<String> range, RedisZSetCommands.Limit limit) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().range(targetType, key, range, limit));
    }

    public Supplier<List<MapRecord<String, String, String>>> read(StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().read(streams));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>> read(Class<V> targetType, StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().read(targetType, streams));
    }

    public Supplier<List<MapRecord<String, String, String>>>
    read(StreamReadOptions readOptions, StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().read(readOptions, streams));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>>
    read(Class<V> targetType, StreamReadOptions readOptions, StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().read(targetType, readOptions, streams));
    }

    public Supplier<List<MapRecord<String, String, String>>> read(Consumer consumer, StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().read(consumer, streams));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>>
    read(Class<V> targetType, Consumer consumer, StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().read(targetType, consumer, streams));
    }

    public Supplier<List<MapRecord<String, String, String>>>
    read(Consumer consumer, StreamReadOptions readOptions, StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().read(consumer, readOptions, streams));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>>
    read(Class<V> targetType, Consumer consumer, StreamReadOptions readOptions, StreamOffset<String>... streams) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().read(targetType, consumer, readOptions, streams));
    }

    public Supplier<List<MapRecord<String, String, String>>> reverseRange(String key, Range<String> range) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().reverseRange(key, range));
    }

    public Supplier<List<MapRecord<String, String, String>>>
    reverseRange(String key, Range<String> range, RedisZSetCommands.Limit limit) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().reverseRange(key, range, limit));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>>
    reverseRange(Class<V> targetType, String key, Range<String> range) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().reverseRange(targetType, key, range));
    }

    public <V> Supplier<List<ObjectRecord<String, V>>>
    reverseRange(Class<V> targetType, String key, Range<String> range, RedisZSetCommands.Limit limit) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().reverseRange(targetType, key, range, limit));
    }

    public Supplier<Long> trim(String key, long count) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForStream().trim(key, count));
    }

    public <V> Supplier<HashMapper<V, String, String>> getHashMapper(Class<V> targetType) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForStream().getHashMapper(targetType));
    }

    StreamOperations(StringRedisPipeline stringRedisPipeline) {
        super(stringRedisPipeline);
    }
}
