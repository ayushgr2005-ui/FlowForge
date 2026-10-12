package com.flowforge.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;

@Service
public class RedisLockService {

    private final StringRedisTemplate redisTemplate;

    // Atomic Lua Script: Check value match before deleting
    private static final String UNLOCK_LUA_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "return redis.call('del', KEYS[1]) " +
            "else return 0 end";

    public RedisLockService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean acquireLock(String lockKey, String lockToken, Duration timeout) {
        Boolean success = redisTemplate.opsForValue().setIfAbsent(lockKey, lockToken, timeout);
        return Boolean.TRUE.equals(success);
    }

    public boolean releaseLock(String lockKey, String lockToken) {
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptText(UNLOCK_LUA_SCRIPT);
        redisScript.setResultType(Long.class);

        Long result = redisTemplate.execute(redisScript, Collections.singletonList(lockKey), lockToken);
        return Long.valueOf(1).equals(result);
    }
}