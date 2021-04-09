package cn.fruitbasket.grape.jedis.pipeline;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Jedis 管道操作，利用一次与Redis服务器交互，执行多个操作并获取返回结果
 * 步骤：
 * 1、构建管道对象，利用 **Ops 对象添加 Redis 操作，添加操作会返回 Supplier 函数
 * 2、利用 Supplier 函数获取对应操作返回值
 * 3、调用 Supplier 函数或者执行 excute() 都会提交管道中的操作
 *
 * @author LiuBing
 * @date 2021/1/22
 */
public class JedisPipeline {

    /**
     * 为了避免一次性执行的操作数太多，占用过多 Redis 服务器缓存空间。程序会按批次提交，这个值代表一批操作最大数量
     */
    private static final int OPERATION_BATCH_SIZE = 10;

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 操作队列
     */
    private Queue<Consumer<RedisOperations>> operationSetterQueue;

    /**
     * 操作结果
     */
    private List<Object> opsResults;

    /**
     * 操作结果索引
     */
    private int opsResultIndex;

    private ValueOps valueOps;
    private HashOps hashOps;
    private ListOps listOps;
    private SetOps setOps;
    private ZSetOps zSetOps;

    /**
     * 构建一个管道对象
     */
    public static JedisPipeline build(StringRedisTemplate stringRedisTemplate) {
        return new JedisPipeline(requireNonNull(stringRedisTemplate));
    }

    /**
     * 利用 Jedis Pipeline 执行所有操作
     *
     * @return true：成功执行，false：管道中无操作
     */
    public boolean excute() {
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
     * 删除Key
     */
    public JedisPipeline delete(String key) {
        addOperation(operations -> operations.delete(key));
        return this;
    }

    /**
     * 普通（String）类型的数据操作
     */
    public ValueOps opsForValue() {
        if (valueOps == null) valueOps = new ValueOps(this);
        return valueOps;
    }

    /**
     * Hash 类型的数据操作
     */
    public HashOps opsForHash() {
        if (hashOps == null) hashOps = new HashOps(this);
        return hashOps;
    }

    /**
     * 列表 类型的数据操作
     */
    public ListOps opsForList() {
        if (listOps == null) listOps = new ListOps(this);
        return listOps;
    }

    /**
     * Set 类型的数据操作
     */
    public SetOps opsForSet() {
        if (setOps == null) setOps = new SetOps(this);
        return setOps;
    }

    /**
     * ZSet 类型的数据操作
     */
    public ZSetOps opsForZSet() {
        if (zSetOps == null) zSetOps = new ZSetOps(this);
        return zSetOps;
    }

    <T> Supplier<T> addOperation(Consumer<RedisOperations> operationSetter) {
        this.operationSetterQueue.offer(operationSetter);
        int resultIndex = this.opsResultIndex++;
        return () -> {
            if (resultIndex >= this.opsResults.size()) this.excute();
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
     * 通过 Jedis 管道执行所有操作
     *
     * @return 操作结果
     */
    private List<Object> executeWithPipeline(List<Consumer<RedisOperations>> operationSetters) {
        return this.stringRedisTemplate.executePipelined(new SessionCallback() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operationSetters.forEach(operationSetter -> operationSetter.accept(operations));
                return null;
            }
        });
    }

    private JedisPipeline(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = requireNonNull(stringRedisTemplate);
        this.operationSetterQueue = new LinkedList<>();
        this.opsResults = new ArrayList<>();
    }
}

