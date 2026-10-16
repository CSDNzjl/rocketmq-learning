package com.example.rocketmqdemo.producer;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * 生产者（Producer）：负责「往消息队列里发消息」。
 *
 * 类比：你是寄信人，Topic 是邮箱地址，消息内容是信纸上的字。
 *
 * 【阶段说明】本类 send / sendRoute 仍给阶段1～3 的 HTTP 接口用（RocketMQTemplate 直连）。
 * 阶段4 请看 stream.StreamDemoProducer（StreamBridge + demoCollect-out-0），不要改本类方法。
 *
 * 本类做两件事：
 * 1. 提供 send(...) 方法，给 Controller 或别处调用，随时发一条
 * 2. 实现 CommandLineRunner：Spring Boot 启动完成后，自动再发一条演示消息
 */
@Component // 告诉 Spring：请把这个类注册成 Bean，启动时创建好，别处可以注入使用
public class DemoProducer implements CommandLineRunner {

    // 日志对象：把发送结果打印到控制台，方便你观察
    private static final Logger log = LoggerFactory.getLogger(DemoProducer.class);

    /**
     * RocketMQ 官方 Spring 封装好的「发消息工具」。
     * 你不用自己 new 客户端，starter 会根据 application.yml 自动创建并注入进来。
     */
    private final RocketMQTemplate rocketMQTemplate;

    /**
     * 要发到哪个 Topic。
     * ${demo.topic} 表示从 application.yml 里读 demo.topic 的值（当前是 demo-topic）。
     */
    @Value("${demo.topic}")
    private String topic;

    /**
     * 构造方法注入：Spring 启动时把已经配好的 RocketMQTemplate 传进来。
     * 比自己写 new 更安全，也方便换配置。
     */
    public DemoProducer(RocketMQTemplate rocketMQTemplate) {
        this.rocketMQTemplate = rocketMQTemplate;
    }

    /**
     * 真正发消息的方法（同步发送）。【阶段1 / 阶段2 / 阶段3 发送仍用本方法或 sendRoute】
     * 当前阶段4 下 DemoConsumer 直连监听已关，本方法仍可发，但不会走阶段1～3 学习路径。
     *
     * @param body 消息正文，本演示用普通字符串即可，例如 "hello"；重试学习则为 RETRY:3:hello
     *
     * syncSend = 同步：发出后会等 Broker 应答（成功或失败）再往下走。
     * MessageBuilder.withPayload(body).build() = 把字符串包成一条 Spring 消息。
     */
    public void send(String body) {
        rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(body).build());
        // {} 是占位符，后面的 topic、body 会依次填进去
        log.info("SENT topic=[{}] body={}", topic, body);
    }

    /**
     * Spring Boot 启动完成后会自动调用这个 run 方法（因为实现了 CommandLineRunner）。
     * 用途：你一启动就能看到「发了一条、消费者是否收到」，不用先手动调接口。
     * 阶段4：这条仍走 RocketMQTemplate；Stream 消费组不会自动练到，请再调 /send-stream。
     */
    @Override
    public void run(String... args) {
        // 拼一条带时间戳的内容，避免每次消息一模一样不好区分
        String startupMsg = "hello-from-startup-" + System.currentTimeMillis();
        try {
            send(startupMsg);
        } catch (Exception e) {
            // 启动发送失败不直接把整个应用弄挂（方便你先把服务拉起来再排查 MQ）
            // 常见原因：Topic 不存在（No route info）、NameServer 连不上
            log.warn("Startup send failed (check NameServer/Topic): {}", e.getMessage());
        }
    }

    /**
     * 【阶段3·路由学习】发送一条带 moduleName 的 JSON 到【同一个 Topic】。
     *
     * 为什么要学这个？
     * fom-daq 里很多模块共用一个 Topic（例如 daqCollect），靠消息体里的 moduleName
     * 区分"这条是给谁的"。本方法就是把这种写法缩成最小 Demo。
     *
     * 发出去的字符串长这样（JSON）：
     *   {"moduleName":"smelt","payload":"hello-route"}
     *
     * 注意：这里【没有改】原来的 send(...) 方法，老接口 /send、/send-retry 完全不动。
     *
     * @param moduleName 业务模块名，例如 smelt / furnace；消费者会拿它和配置比对
     * @param payload    真正的业务内容（学习阶段用普通字符串即可）
     */
    public void sendRoute(String moduleName, String payload) {
        // 学习阶段手动拼 JSON，字段名写死，方便在日志里一眼看懂。
        // 若 moduleName/payload 里出现双引号或反斜杠，先做简单转义，避免拼出非法 JSON。
        String safeModule = moduleName == null ? "" : moduleName.replace("\\", "\\\\").replace("\"", "\\\"");
        String safePayload = payload == null ? "" : payload.replace("\\", "\\\\").replace("\"", "\\\"");
        String json = "{\"moduleName\":\"" + safeModule + "\",\"payload\":\"" + safePayload + "\"}";

        // 仍然走原来的 syncSend + 同一个 topic，只是 body 换成了 JSON
        rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(json).build());
        log.info("SENT(route) topic=[{}] moduleName={} payload={} json={}", topic, moduleName, payload, json);
    }
}
