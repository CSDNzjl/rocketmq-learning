package com.example.rocketmqdemo.stream;

import com.alibaba.cloud.stream.binder.rocketmq.constant.RocketMQConst;
import com.example.rocketmqdemo.dto.DemoCollectMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * 【阶段4 + 5 + 6】用 Spring Cloud Stream 发消息（对齐 fom-daq 的 DaqMessageProducer）。
 *
 * 和阶段1～3 的差别
 * - 以前：RocketMQTemplate.syncSend(topic, ...)  → 代码里直接写 Topic
 * - 现在：StreamBridge.send(binding名, ...)     → 代码里只写插头名 out-0
 *           真正 Topic 在 application.yml 的 destination
 *
 * 阶段4：send(String) 纯文本（可选 Tag，默认 STREAM）
 * 阶段5：sendJson(DemoCollectMessage) 把对象序列化成 JSON 再发
 * 阶段6：发送时在 Message header 里带 RocketMQ Tag（TAGS）
 *         JSON 消息的 Tag = type（如 TEMP / PRESSURE）
 *         入站侧用 yml 的 subscription 只订部分 Tag，未订到的消息到不了本应用
 *
 * fom-daq 真实写法：
 *   streamBridge.send("daqCollect-out-0", message);
 * 本 Demo：
 *   streamBridge.send("demoCollect-out-0", message);
 */
@Component
public class StreamDemoProducer {

    private static final Logger log = LoggerFactory.getLogger(StreamDemoProducer.class);

    /** Spring Cloud Stream 提供的「按 binding 名发送」工具 */
    private final StreamBridge streamBridge;

    /**
     * Jackson：把 Java 对象 ↔ JSON 字符串。
     * spring-boot-starter-web 里自带，直接注入即可。
     */
    private final ObjectMapper objectMapper;

    /**
     * 出站 binding 名，必须和 application.yml 里
     * spring.cloud.stream.bindings.demoCollect-out-0 对得上。
     */
    @Value("${demo.stream.binding-out:demoCollect-out-0}")
    private String bindingOut;

    public StreamDemoProducer(StreamBridge streamBridge, ObjectMapper objectMapper) {
        this.streamBridge = streamBridge;
        this.objectMapper = objectMapper;
    }

    /**
     * 【阶段4 / 6】经 Stream 出站 binding 发一条纯文本。
     * 默认打 Tag=STREAM，方便和 JSON 的 TEMP/PRESSURE 区分；
     * 当前 yml 若只订 TEMP，这条不会进 Consumer（这是阶段6要观察的现象之一）。
     *
     * @param body 消息正文，例如 STREAM:hello
     * @return StreamBridge 是否认为发送成功（false 时查日志/Broker）
     */
    public boolean send(String body) {
        return sendWithTag(body, "STREAM");
    }

    /**
     * 【阶段6】带指定 Tag 发纯文本（学习用；Tag 为空则不设 header）。
     */
    public boolean sendWithTag(String body, String tag) {
        MessageBuilder<String> builder = MessageBuilder.withPayload(body);
        String normalizedTag = normalizeTag(tag);
        if (!normalizedTag.isEmpty()) {
            // Binder 认 header 名 "TAGS"（见 RocketMQConst.Headers.TAGS）
            builder.setHeader(RocketMQConst.Headers.TAGS, normalizedTag);
        }
        boolean ok = streamBridge.send(bindingOut, builder.build());
        if (ok) {
            log.info("STREAM_SENT binding=[{}] tag=[{}] body={}", bindingOut, normalizedTag, body);
        } else {
            log.warn("STREAM_SENT_FAIL binding=[{}] tag=[{}] body={}", bindingOut, normalizedTag, body);
        }
        return ok;
    }

    /**
     * 【阶段5 + 6】把 DTO 变成 JSON，经同一条 binding 发出去，并打 Tag=type。
     *
     * 为什么仍发「字符串」而不是直接丢对象：
     * - 入站 Consumer 继续是 Consumer&lt;String&gt;，阶段4 纯文本还能走同一条路
     * - 自己用 Jackson 序列化/反序列化，少和 Fastjson 默认转换器打架（减少 WARN）
     * - 正式项目也可能「先 toJSONString 再 send」，思路一致
     *
     * @param message 业务体（type / pointCode / value / ts）
     * @return 是否发送成功
     */
    public boolean sendJson(DemoCollectMessage message) {
        if (message == null) {
            log.warn("JSON_SENT_FAIL message is null");
            return false;
        }
        // 没有时间戳就补当前毫秒，方便对照日志
        if (message.getTs() == null) {
            message.setTs(System.currentTimeMillis());
        }
        final String json;
        try {
            json = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error("JSON_SENT_FAIL 序列化失败: {}", e.getMessage());
            return false;
        }

        // 【阶段6】Tag = type。Broker 按订阅表达式过滤时看的是这个 Tag，不是 JSON 里的字段
        String tag = normalizeTag(message.getType());
        MessageBuilder<String> builder = MessageBuilder.withPayload(json)
                .setHeader("contentType", MediaType.APPLICATION_JSON_VALUE);
        if (!tag.isEmpty()) {
            builder.setHeader(RocketMQConst.Headers.TAGS, tag);
        }

        boolean ok = streamBridge.send(bindingOut, builder.build());
        if (ok) {
            log.info("JSON_SENT binding=[{}] tag=[{}] type={} json={}",
                    bindingOut, tag, message.getType(), json);
        } else {
            log.warn("JSON_SENT_FAIL binding=[{}] tag=[{}] json={}", bindingOut, tag, json);
        }
        return ok;
    }

    /** Tag 统一成大写；空/空白视为未设置 */
    private static String normalizeTag(String tag) {
        if (tag == null) {
            return "";
        }
        return tag.trim().toUpperCase(Locale.ROOT);
    }
}