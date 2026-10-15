package com.example.rocketmqdemo.consumer;

import jakarta.annotation.PostConstruct;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

/**
 * 消费者（Consumer）：从 Topic 收消息。
 *
 * 【普通消息】正文任意字符串 → 打印 RECEIVED，消费成功。
 *
 * 【故意失败重试】正文格式：RETRY:{失败次数}:{真正内容}
 *   例：RETRY:3:hello
 *   含义：前 3 次投递故意失败（抛异常），第 4 次成功。
 *
 * RocketMQ 约定（本演示用的方式）：
 * - onMessage 里抛出异常 → 框架认为消费失败 → 稍后自动重投（reconsume）
 * - 正常返回（不抛异常）→ 消费成功，不再重投
 *
 * MessageExt 比 String 多带了元数据，其中 getReconsumeTimes() = 已经重试过几次
 * （第 1 次投递时是 0，失败后再来是 1、2、3...）。
 */
@Service
@RocketMQMessageListener(
        topic = "${demo.topic}",
        consumerGroup = "${demo.consumer-group}",
        selectorExpression = "*",
        // 最多重试多少次；超过后进死信（本演示 failTimes 请小于这个数）
        maxReconsumeTimes = 16
)
public class DemoConsumer implements RocketMQListener<MessageExt> {

    private static final Logger log = LoggerFactory.getLogger(DemoConsumer.class);

    /**
     * 【新增·路由学习】本消费者"认领"的模块名。
     * 值来自 application.yml 的 demo.route.my-module（当前默认 smelt）。
     * 只有消息 JSON 里的 moduleName 与此相同，才会真正处理；否则 SKIP。
     */
    @Value("${demo.route.my-module:smelt}")
    private String myModule;

    /** 解析路由 JSON 用。Spring Boot 已带 Jackson，直接 new 即可（学习阶段够用）。 */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 约定前缀：RETRY:次数:内容 */
    public static final String RETRY_PREFIX = "RETRY:";

    @PostConstruct
    public void onReady() {
        log.info("消费者已注册: 监听 Topic/Group 见 yml。普通消息直接 RECEIVED；正文以 RETRY: 开头则演示失败重试。");
    }

    @Override
    public void onMessage(MessageExt message) {
        // 把字节数组转成字符串（生产者发的 UTF-8 文本）
        String body = new String(message.getBody(), java.nio.charset.StandardCharsets.UTF_8);
        // 当前是第几次投递：0=首次，1=第 1 次重试后，以此类推
        int reconsumeTimes = message.getReconsumeTimes();

        log.info("收到投递: body={}, reconsumeTimes={}, msgId={}",
                body, reconsumeTimes, message.getMsgId());

        // 不是 RETRY: 开头 → 普通消息，直接成功
        
        // =====================================================================
        // 【新增·路由学习】同 Topic + JSON.moduleName 过滤（对应 fom-daq daqCollect）
        // 若消息是 {"moduleName":"...","payload":"..."} 这种路由 JSON：
        //   - moduleName == myModule  → 本模块的消息，打印 ROUTE_PROCESS 后成功返回
        //   - moduleName != myModule  → 别人的消息，打印 ROUTE_SKIP，【成功返回、不抛异常】
        //     （跳过 ≠ 失败；抛异常会触发重试，那是错的）
        // 若不是这种 JSON → 不进本分支，下面原来的普通消息 / RETRY: 逻辑【完全不动】
        // =====================================================================
        String trimmed = body == null ? "" : body.trim();
        if (trimmed.startsWith("{") && trimmed.contains("\"moduleName\"")) {
            try {
                JsonNode root = objectMapper.readTree(trimmed);
                JsonNode moduleNode = root.get("moduleName");
                if (moduleNode != null && !moduleNode.isNull()) {
                    String moduleName = moduleNode.asText("");
                    String payload = root.has("payload") && !root.get("payload").isNull()
                            ? root.get("payload").asText("")
                            : "";

                    if (myModule != null && myModule.equals(moduleName)) {
                        // 命中本模块：这里学习阶段只打日志；真实项目里会调用业务 Service
                        log.info("ROUTE_PROCESS: 本模块消息 myModule={}, moduleName={}, payload={}, msgId={}",
                                myModule, moduleName, payload, message.getMsgId());
                        return; // 成功消费，不重试
                    } else {
                        // 不是本模块：跳过。必须 return 且不抛异常！
                        log.info("ROUTE_SKIP: 非本模块消息 myModule={}, moduleName={}, payload={}, msgId={}",
                                myModule, moduleName, payload, message.getMsgId());
                        return; // 成功跳过，Broker 不会再投递
                    }
                }
                // JSON 里没有 moduleName → 当普通消息，落到下面旧逻辑
                log.debug("JSON 无 moduleName 字段，按旧逻辑处理: {}", trimmed);
            } catch (Exception parseEx) {
                // 解析失败 → 不当路由消息，落到下面旧逻辑（兼容误发的 { 开头文本）
                log.warn("疑似 JSON 但解析失败，按旧逻辑处理: {}, err={}", trimmed, parseEx.getMessage());
            }
        }

        if (!body.startsWith(RETRY_PREFIX)) {
            log.info("RECEIVED: {}", body);
            return;
        }

        // 解析 RETRY:{N}:{payload}
        // 例 RETRY:3:hello → failTimes=3, payload=hello
        String rest = body.substring(RETRY_PREFIX.length());
        int colon = rest.indexOf(':');
        if (colon <= 0) {
            // 格式不对就当普通消息，避免一直失败
            log.warn("RETRY 格式不正确（应为 RETRY:次数:内容），按成功处理: {}", body);
            log.info("RECEIVED: {}", body);
            return;
        }

        int failTimes;
        try {
            failTimes = Integer.parseInt(rest.substring(0, colon).trim());
        } catch (NumberFormatException e) {
            log.warn("RETRY 次数不是数字，按成功处理: {}", body);
            log.info("RECEIVED: {}", body);
            return;
        }
        if (failTimes < 0) {
            failTimes = 0;
        }
        String payload = rest.substring(colon + 1);

        // reconsumeTimes < failTimes → 还没失败够次数 → 抛异常触发 RocketMQ 重试
        if (reconsumeTimes < failTimes) {
            log.warn("模拟消费失败，将触发重试: payload={}, 已失败次数≈{}, 目标失败次数={}, msgId={}",
                    payload, reconsumeTimes, failTimes, message.getMsgId());
            // ★ 关键：抛异常 = 告诉 RocketMQ「这次没处理成，请稍后重投」
            throw new RuntimeException("demo intentional fail, reconsumeTimes="
                    + reconsumeTimes + ", wantFailTimes=" + failTimes);
        }

        // 已经失败够次数 → 本次成功
        log.info("RECEIVED(after-retry): payload={}, reconsumeTimes={}, 此前故意失败 {} 次",
                payload, reconsumeTimes, failTimes);
    }
}
