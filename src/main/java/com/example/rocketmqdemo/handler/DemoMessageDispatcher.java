package com.example.rocketmqdemo.handler;

import com.example.rocketmqdemo.dto.DemoCollectMessage;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 【阶段5】按 message.type 找到对应 Handler 并调用（简单版「策略分发」）。
 *
 * 小白理解整条链：
 * 1. HTTP /send-json 组出 DemoCollectMessage
 * 2. StreamBridge 发到 demoCollect-out-0 → Topic demo-topic
 * 3. demoCollect-in-0 收到 → Consumer 解析 JSON
 * 4. 调用本类 dispatch(message)
 * 5. 查表 type → Handler，执行 handle
 *
 * 对齐熔炼：按 callback 找 Handler；这里用更短的字段名 type。
 */
@Component
public class DemoMessageDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DemoMessageDispatcher.class);

    /** type 字符串 → Handler 实例 */
    private final Map<String, DemoMessageHandler> handlerMap = new HashMap<>();

    /**
     * Spring 会把所有实现了 DemoMessageHandler 的 @Component 注进这个 List。
     * 你新增一个 Handler 类并加 @Component，重启后自动进表，不必改 Dispatcher。
     */
    public DemoMessageDispatcher(List<DemoMessageHandler> handlers) {
        for (DemoMessageHandler h : handlers) {
            String key = normalize(h.supportType());
            if (key.isEmpty()) {
                log.warn("忽略 supportType 为空的 Handler: {}", h.getClass().getName());
                continue;
            }
            DemoMessageHandler old = handlerMap.put(key, h);
            if (old != null) {
                log.warn("type={} 被重复注册：{} 覆盖 {}", key,
                        h.getClass().getSimpleName(), old.getClass().getSimpleName());
            }
        }
    }

    @PostConstruct
    public void onReady() {
        log.info("阶段5 Dispatcher 已就绪，已注册 type={}", handlerMap.keySet());
    }

    /**
     * 按类型分发。未知 type 只打 WARN，不抛异常（避免无意义的 MQ 重试刷屏）。
     */
    public void dispatch(DemoCollectMessage message) {
        if (message == null) {
            log.warn("DISPATCH skipped: message is null");
            return;
        }
        String type = normalize(message.getType());
        log.info("DISPATCH type={} pointCode={} value={}", type, message.getPointCode(), message.getValue());
        if (type.isEmpty()) {
            log.warn("DISPATCH unknown type: (空) raw={}", message);
            return;
        }
        DemoMessageHandler handler = handlerMap.get(type);
        if (handler == null) {
            log.warn("DISPATCH unknown type={} （未注册 Handler） raw={}", type, message);
            return;
        }
        handler.handle(message);
    }

    /** 统一成大写，避免 temp / Temp / TEMP 对不上 */
    private static String normalize(String type) {
        if (type == null) {
            return "";
        }
        return type.trim().toUpperCase(Locale.ROOT);
    }
}
