package com.example.rocketmqdemo.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * 【阶段4】用 Spring Cloud Stream 发消息（对齐 fom-daq 的 DaqMessageProducer）。
 *
 * 和阶段1～3 的差别：
 * - 以前：RocketMQTemplate.syncSend(topic, ...)  —— 代码里直接写 Topic
 * - 现在：StreamBridge.send(binding名, ...)     —— 代码里只写插头名 out-0
 *           真正 Topic 在 application.yml 的 destination
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
     * 出站 binding 名，必须和 application.yml 里
     * spring.cloud.stream.bindings.demoCollect-out-0 对得上。
     */
    @Value("${demo.stream.binding-out:demoCollect-out-0}")
    private String bindingOut;

    public StreamDemoProducer(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    /**
     * 经 Stream 出站 binding 发一条纯文本。
     *
     * @param body 消息正文，例如 STREAM:hello
     * @return StreamBridge 是否认为发送成功（false 时看日志/Broker）
     */
    public boolean send(String body) {
        // withPayload：把字符串包成 Spring Message；和 daq 里拼 JSON 再发是同一套路
        boolean ok = streamBridge.send(bindingOut, MessageBuilder.withPayload(body).build());
        if (ok) {
            log.info("STREAM_SENT binding=[{}] body={}", bindingOut, body);
        } else {
            log.warn("STREAM_SENT_FAIL binding=[{}] body={}", bindingOut, body);
        }
        return ok;
    }
}
