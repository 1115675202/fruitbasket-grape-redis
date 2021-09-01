package cn.fruitbasket.grape.redis.pipeline;

import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Redis 管道操作，利用一次与Redis服务器交互，执行多个操作并获取返回结果
 * 步骤：
 * 1、构建管道对象，利用 **Ops 对象添加 Redis 操作，添加操作会返回 Supplier 函数
 * 2、利用 Supplier 函数获取对应操作返回值
 * 3、调用 Supplier 函数或者执行 execute() 都会提交管道中的操作
 *
 * @author LiuBing
 * @date 2021/1/22
 */
public class StringRedisPipeline {

    /**
     * 为了避免一次性执行的操作数太多，占用过多 Redis 服务器缓存空间。程序会按批次提交，这个值代表一批操作最大数量
     */
    private static final int OPERATION_BATCH_SIZE = 50;

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 操作队列
     */
    private final Queue<Consumer<RedisOperations>> operationSetterQueue;

    /**
     * 操作结果
     */
    private final List<Object> opsResults;

    /**
     * 操作结果索引
     */
    private int opsResultIndex;

    private ValueOperations valueOperations;
    private HashOperations hashOperations;
    private ListOperations listOperations;
    private SetOperations setOperations;
    private ZSetOperations zSetOperations;
    private HyperLogLogOperations hyperLogLogOperations;
    private GeoOperations geoOperations;
    private StreamOperations streamOperations;

    /**
     * 构建一个管道对象
     */
    public static StringRedisPipeline build(StringRedisTemplate stringRedisTemplate) {
        return new StringRedisPipeline(requireNonNull(stringRedisTemplate));
    }

    /**
     * Pipeline 执行所有操作
     *
     * @return true：成功执行，false：管道中无操作
     */
    public boolean execute() {
        if (this.operationSetterQueue.isEmpty()) {
            return false;
        }
        for (List<Consumer<RedisOperations>> operationSetters = splitABatchOfOperation();
             !operationSetters.isEmpty(); operationSetters = splitABatchOfOperation()) {
            this.opsResults.addAll(executeWithPipeline(operationSetters));
        }
        return true;
    }

    /**
     * {@link RedisOperations#delete(Object)}
     */
    public StringRedisPipeline delete(String key) {
        addOperation(operations -> operations.delete(key));
        return this;
    }

    /**
     * {@link RedisOperations#delete(Collection)}
     */
    public Supplier<Long> delete(Collection<String> keys) {
        return addOperation(operations -> operations.delete(keys));
    }

    /**
     * {@link RedisOperations#keys(Object)}
     */
    public Supplier<Set<String>> keys(String pattern) {
        return addOperation(operations -> operations.keys(pattern));
    }

    public ValueOperations opsForValue() {
        if (valueOperations == null) valueOperations = new ValueOperations(this);
        return valueOperations;
    }

    public HashOperations opsForHash() {
        if (hashOperations == null) hashOperations = new HashOperations(this);
        return hashOperations;
    }

    public ListOperations opsForList() {
        if (listOperations == null) listOperations = new ListOperations(this);
        return listOperations;
    }

    public SetOperations opsForSet() {
        if (setOperations == null) setOperations = new SetOperations(this);
        return setOperations;
    }

    public ZSetOperations opsForZSet() {
        if (zSetOperations == null) zSetOperations = new ZSetOperations(this);
        return zSetOperations;
    }

    public HyperLogLogOperations opsForHyperLogLog() {
        if (hyperLogLogOperations == null) hyperLogLogOperations = new HyperLogLogOperations(this);
        return hyperLogLogOperations;
    }

    public GeoOperations opsForGeo() {
        if (geoOperations == null) geoOperations = new GeoOperations(this);
        return geoOperations;
    }

    public StreamOperations opsForStream() {
        if (streamOperations == null) streamOperations = new StreamOperations(this);
        return streamOperations;
    }

    <T> Supplier<T> addOperation(Consumer<RedisOperations> operationSetter) {
        this.operationSetterQueue.offer(operationSetter);
        int resultIndex = this.opsResultIndex++;
        return () -> {
            if (resultIndex >= this.opsResults.size()) this.execute();
            return (T) opsResults.get(resultIndex);
        };
    }

    /**
     * 按配置大小分割出一组操作
     */
    private List<Consumer<RedisOperations>> splitABatchOfOperation() {
        if (operationSetterQueue.isEmpty()) {
            return Collections.emptyList();
        }
        List<Consumer<RedisOperations>> operationSetters = new LinkedList<>();
        for (int i = OPERATION_BATCH_SIZE; i > 0 && !operationSetterQueue.isEmpty(); i--) {
            operationSetters.add(operationSetterQueue.poll());
        }
        return operationSetters;
    }

    /**
     * 通过 Redis 管道执行所有操作
     *
     * @return 操作结果
     */
    private List<Object> executeWithPipeline(List<Consumer<RedisOperations>> operationSetters) {
        return this.stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) {
                operationSetters.forEach(operationSetter -> operationSetter.accept(operations));
                return null;
            }
        });
    }

    private StringRedisPipeline(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = requireNonNull(stringRedisTemplate);
        this.operationSetterQueue = new LinkedList<>();
        this.opsResults = new ArrayList<>();
    }
}

