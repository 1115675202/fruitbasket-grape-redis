package cn.fruitbasket.grape.redis.pipeline;

/**
 * Redis 管道操作抽象
 *
 * @author LiuBing
 * @date 2021/1/22
 */
public abstract class AbstractRedisOperations {

	protected final StringRedisPipeline stringRedisPipeline;

	/**
	 * @return 关联的管道对象，实现流式操作
	 */
	public StringRedisPipeline and() {
		return stringRedisPipeline;
	}

	/**
	 * 利用 Redis Pipeline 执行所有操作
	 * @return true：成功执行，false：管道中无操作
	 */
	public boolean execute() {
		return stringRedisPipeline.execute();
	}

	AbstractRedisOperations(StringRedisPipeline stringRedisPipeline) {
		this.stringRedisPipeline = stringRedisPipeline;
	}
}
