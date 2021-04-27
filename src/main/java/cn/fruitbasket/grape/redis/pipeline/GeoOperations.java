package cn.fruitbasket.grape.redis.pipeline;

import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author LiuBing
 * @date 2021/4/27
 */
public class GeoOperations extends AbstractRedisOperations {

    public Supplier<Long> add(String key, Point point, String member) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().add(key, point, member));
    }


    public Supplier<Long> add(String key, RedisGeoCommands.GeoLocation<String> location) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().add(key, location));
    }


    public Supplier<Long> add(String key, Map<String, Point> memberCoordinateStringap) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForGeo().add(key, memberCoordinateStringap));
    }


    public Supplier<Long> add(String key, Iterable<RedisGeoCommands.GeoLocation<String>> locations) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().add(key, locations));
    }


    public Supplier<Distance> distance(String key, String member1, String member2) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForGeo().distance(key, member1, member2));
    }


    public Supplier<Distance> distance(String key, String member1, String member2, Metric metric) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForGeo().distance(key, member1, member2, metric));
    }


    public Supplier<List<String>> hash(String key, String... members) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().hash(key, members));
    }


    public Supplier<List<Point>> position(String key, String... members) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().position(key, members));
    }


    public Supplier<GeoResults<RedisGeoCommands.GeoLocation<String>>> radius(String key, Circle within) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().radius(key, within));
    }


    public Supplier<GeoResults<RedisGeoCommands.GeoLocation<String>>>
    radius(String key, Circle within, RedisGeoCommands.GeoRadiusCommandArgs args) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().radius(key, within, args));
    }


    public Supplier<GeoResults<RedisGeoCommands.GeoLocation<String>>>
    radius(String key, String member, double radius) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().radius(key, member, radius));
    }


    public Supplier<GeoResults<RedisGeoCommands.GeoLocation<String>>>
    radius(String key, String member, Distance distance) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForGeo().radius(key, member, distance));
    }


    public Supplier<GeoResults<RedisGeoCommands.GeoLocation<String>>>
    radius(String key, String member, Distance distance, RedisGeoCommands.GeoRadiusCommandArgs args) {
        return this.stringRedisPipeline.addOperation(operations ->
                operations.opsForGeo().radius(key, member, distance, args));
    }


    public Supplier<Long> remove(String key, String... members) {
        return this.stringRedisPipeline.addOperation(operations -> operations.opsForGeo().remove(key, members));
    }

    GeoOperations(StringRedisPipeline stringRedisPipeline) {
        super(stringRedisPipeline);
    }
}
