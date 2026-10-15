package com.example.rocketmqdemo.consumer;

import jakarta.annotation.PostConstruct;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * （第 1 次投递时是 0，失败后再来是 1、2、3…）。
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