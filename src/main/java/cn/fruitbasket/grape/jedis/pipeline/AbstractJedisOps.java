package cn.fruitbasket.grape.jedis.pipeline;

/**
 * Jedis 管道操作抽象
 *
 * @author LiuBing
 * @date 2021/1/22
 */
public abstract class AbstractJedisOps {

	protected final JedisPipeline jedisPipeline;

	/**
	 * @return 关联的管道对象，实现流式操作
	 */
	public JedisPipeline and() {
		return jedisPipeline;
	}

	/**
	 * 利用 Jedis Pipeline 执行所有操作
	 * @return true：成功执行，false：管道中无操作
	 */
	public boolean execute() {
		return jedisPipeline.execute();
	}

	AbstractJedisOps(JedisPipeline jedisPipeline) {
		this.jedisPipeline = jedisPipeline;
	}
}
