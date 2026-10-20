package com.example.rocketmqdemo.stream;

import com.example.rocketmqdemo.dto.DemoCollectMessage;
import com.example.rocketmqdemo.handler.DemoMessageDispatcher;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

/**
 * 【阶段4 + 5 + 6】用 Spring Cloud Stream 收消息（对齐 fom-smelt 的 RocketMQConsumerConfig）。
 *
 * 关键点（阶段4）：
 * 1. 写一个 @Bean，类型是 {@code Consumer&lt;String&gt;}
 * 2. Bean 方法名 = demoCollect
 * 3. application.yml 里 spring.cloud.function.definition=demoCollect
 * 4. 框架自动创建入站 binding 名：demoCollect-in-0
 * 5. yml 里给 demoCollect-in-0 的 destination 指到真正 Topic，并写上 group
 *
 * 阶段5 增强：
 * - 正文若以 { 开头：当 JSON → 解析成 DemoCollectMessage → Dispatcher 按 type 分发
 * - 否则仍按阶段4 打 STREAM_RECEIVED（兼容 /send-stream 纯文本）
 *
 * 阶段6：
 * - 入站「能不能进到本方法」由 yml 的 subscription（Tag 表达式）决定
 * - 本类不再做 Tag 过滤；能走进来的，说明 Broker 已经认为匹配当前订阅
 * - 默认 subscription=TEMP 时：PRESSURE / STREAM 的 send 日志会有，但这里不会出现 RECEIVED
 *
 * fom-smelt 真实写法：
 *   @Bean public Consumer&lt;String&gt; daqCollect() { ... 再按 callback 找 Handler ... }
 *   binding：daqCollect-in-0；group：fom-smelt-daq
 */
@Configuration
public class StreamDemoConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamDemoConsumerConfig.class);

    private final ObjectMapper objectMapper;
    private final DemoMessageDispatcher dispatcher;

    /** 当前入站订阅的 Tag 表达式（与 yml demo.stream.subscription-tags 一致，仅用于启动日志提示） */
    @Value("${demo.stream.subscription-tags:*}")
    private String subscriptionTags;

    public StreamDemoConsumerConfig(ObjectMapper objectMapper, DemoMessageDispatcher dispatcher) {
        this.objectMapper = objectMapper;
        this.dispatcher = dispatcher;
    }

    @PostConstruct
    public void onReady() {
        log.info("阶段4/5/6 Stream 消费已注册：Bean 名 demoCollect → binding demoCollect-in-0；"
                + "当前 Tag 订阅 subscription=[{}]（改 demo.stream.subscription-tags 后需重启）；"
                + "纯文本试 POST /send-stream；JSON+Handler 试 POST /send-json",
                subscriptionTags);
    }

    /**
     * 函数式消费者：每来一条消息，框架就调用一次 accept。
     * 方法名 demoCollect 很重要：必须同时对上 yml 的 function.definition 和 bindings 前缀。
     *
     * 为什么继续用 Consumer&lt;String&gt; 而不是 Consumer&lt;DemoCollectMessage&gt;：
     * - 同一条 binding 还要兼容阶段4 的纯文本 STREAM:...
     * - 自己 parse JSON，失败时日志更直观，还能拿到原始字符串
     */
    @Bean
    public Consumer<String> demoCollect() {
        return payload -> {
            if (payload == null) {
                log.warn("STREAM_RECEIVED null payload");
                return;
            }
            String text = payload.trim();
            // 以 { 开头：当作阶段5 JSON
            if (text.startsWith("{")) {
                try {
                    DemoCollectMessage message = objectMapper.readValue(text, DemoCollectMessage.class);
                    log.info("JSON_RECEIVED(via demoCollect-in-0): {}", text);
                    dispatcher.dispatch(message);
                } catch (Exception e) {
                    // 解析失败只打错误，不抛出去，避免刷 MQ 重试；正式环境可改成进死信/告警
                    log.error("JSON_PARSE_FAIL body={} err={}", text, e.getMessage());
                }
                return;
            }
            // 否则走阶段4 纯文本路径
            log.info("STREAM_RECEIVED(via demoCollect-in-0): {}", payload);
        };
    }
}