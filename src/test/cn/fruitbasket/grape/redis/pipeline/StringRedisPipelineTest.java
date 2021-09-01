package cn.fruitbasket.grape.redis.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource("classpath:application.yml")
class StringRedisPipelineTest {

    private static final String KEY_PREFIX = "StringRedisPipeline:";

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void add() {
        StringRedisPipeline jedisPipeline = StringRedisPipeline.build(stringRedisTemplate);
        jedisPipeline.opsForValue()
                .set(KEY_PREFIX + "key1", "1")
                .set(KEY_PREFIX + "key2", "2")
                .and().opsForHash()
                .put(KEY_PREFIX + "key3", "hashKey1", "1")
                .put(KEY_PREFIX + "key3", "hashKey2", "2")
                .execute();
    }

    @Test
    void get() {
        StringRedisPipeline jedisPipeline = StringRedisPipeline.build(stringRedisTemplate);
        Supplier<String> valu1 = jedisPipeline.opsForValue().get(KEY_PREFIX + "key1");
        Supplier<String> valu2 = jedisPipeline.opsForValue().get(KEY_PREFIX + "key2");
        Supplier<String> hashValue1 = jedisPipeline.opsForHash().get(KEY_PREFIX + "key3", "hashKey1");
        Supplier<String> hashValue2 = jedisPipeline.opsForHash().get(KEY_PREFIX + "key3", "hashKey2");
        assertEquals("1", valu1.get());
        assertEquals("2", valu2.get());
        assertEquals("1", hashValue1.get());
        assertEquals("2", hashValue2.get());
    }

    @Test
    void delete() {
        StringRedisPipeline jedisPipeline = StringRedisPipeline.build(stringRedisTemplate);
        jedisPipeline
                .delete(KEY_PREFIX + "key1")
                .delete(KEY_PREFIX + "key2")
                .opsForHash().delete(KEY_PREFIX + "key3", "hashKey1", "hashKey2");
        assertTrue(jedisPipeline.keys(KEY_PREFIX + "key*").get().isEmpty());
    }

    @Test
    void compare() {
        int sampleCount = 1000;
        StopWatch sw = new StopWatch();
        StringRedisPipeline jedisPipeline = StringRedisPipeline.build(stringRedisTemplate);
        Map<String, String> sampleMap = IntStream.range(0, sampleCount)
                .mapToObj(String::valueOf)
                .collect(Collectors.toMap(i -> KEY_PREFIX + i, Function.identity()));

        sw.start("Pipeline Set");
        sampleMap.forEach((k, v) -> jedisPipeline.opsForValue().set(k, v));
        jedisPipeline.execute();
        sw.stop();


        sw.start("Pipeline Delete");
        sampleMap.keySet().forEach(jedisPipeline::delete);
        jedisPipeline.execute();
        sw.stop();

        sw.start("Normal Set");
        sampleMap.forEach((k, v) -> stringRedisTemplate.opsForValue().set(k, v));
        sw.stop();

        sw.start("Normal Delete");
        sampleMap.keySet().forEach(stringRedisTemplate::delete);
        sw.stop();
        System.out.println(sw.prettyPrint());
    }
}