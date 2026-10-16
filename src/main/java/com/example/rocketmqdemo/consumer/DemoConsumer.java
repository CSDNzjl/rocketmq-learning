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
 * 消费者（Consumer）：【阶段1～3】rocketmq-spring 直连 Topic。
 *
 * ============================================================
 * 【学习阶段怎么切换？】
 * ============================================================
 * 当前【阶段4 启用中】：收发走 Spring Cloud Stream（见 stream 包 + /send-stream）。
 * 本类 @RocketMQMessageListener 已注释，避免和 demoCollect-in-0 混在一起学。
 *
 * 重学阶段1 / 2 / 3：
 *   1）到 stream 包可先不测 /send-stream；
 *   2）恢复下面的 @RocketMQMessageListener，并 implements RocketMQListener<MessageExt>；
 *   3）取消「onMessage + 对应阶段」注释，其它阶段保持注释；
 *   4）重启应用。
 *
 * 阶段4 对照：fom-daq 用 StreamBridge + daqCollect-out-0；
 *            fom-smelt 用 Consumer Bean daqCollect → daqCollect-in-0。
 */
@Service
// ===== 阶段1～3 直连监听：当前注释（阶段4 用 Stream 入站）=====
// 恢复时：取消下一行注解注释，并给类加上 implements RocketMQListener<MessageExt>
// @RocketMQMessageListener(
//         topic = "${demo.topic}",
//         consumerGroup = "${demo.consumer-group}",
//         selectorExpression = "*",
//         maxReconsumeTimes = 16
// )
public class DemoConsumer {

    private static final Logger log = LoggerFactory.getLogger(DemoConsumer.class);

    /** 【阶段3·路由】本消费者认领的模块名（application.yml → demo.route.my-module） */
    @Value("${demo.route.my-module:smelt}")
    private String myModule;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 【阶段2约定前缀】RETRY:次数:内容
     * /send-retry 仍引用，类必须保留。
     */
    public static final String RETRY_PREFIX = "RETRY:";

    @PostConstruct
    public void onReady() {
        log.info("DemoConsumer 已加载；当前学习阶段=4（Stream binding）。"
                + " 直连监听已关。请 POST /send-stream。重学1～3见本类注释。myModule={}",
                myModule);
    }

    /*
    ===== 恢复阶段1～3时：取消本 onMessage 整段注释，并恢复类上的 @RocketMQMessageListener =====
    public void onMessage(MessageExt message) {
        String body = new String(message.getBody(), java.nio.charset.StandardCharsets.UTF_8);
        int reconsumeTimes = message.getReconsumeTimes();
        log.info("收到投递: body={}, reconsumeTimes={}, msgId={}",
                body, reconsumeTimes, message.getMsgId());

        // =====================================================================
        // ===== 阶段3：同 Topic + moduleName 路由 ROUTE_PROCESS/SKIP =====
        // 测法：
        //   curl -X POST "http://127.0.0.1:8088/send-route?moduleName=smelt&msg=hello-route"
        //   curl -X POST "http://127.0.0.1:8088/send-route?moduleName=furnace&msg=other-module"
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
                        log.info("ROUTE_PROCESS: 本模块消息 myModule={}, moduleName={}, payload={}, msgId={}",
                                myModule, moduleName, payload, message.getMsgId());
                        return;
                    } else {
                        log.info("ROUTE_SKIP: 非本模块消息 myModule={}, moduleName={}, payload={}, msgId={}",
                                myModule, moduleName, payload, message.getMsgId());
                        return;
                    }
                }
                log.warn("JSON 无 moduleName 字段，阶段3忽略: {}", trimmed);
                return;
            } catch (Exception parseEx) {
                log.warn("疑似 JSON 但解析失败，阶段3忽略: {}, err={}", trimmed, parseEx.getMessage());
                return;
            }
        }
        log.info("非路由消息。若当前只开阶段3可跳过；否则继续阶段1/2。body={}", body);

        // =====================================================================
        // ===== 阶段1：普通接收 =====
        // 测法：POST http://127.0.0.1:8088/send?msg=hello
        // =====================================================================
        if (!body.startsWith(RETRY_PREFIX)) {
            log.info("RECEIVED: {}", body);
            return;
        }

        // =====================================================================
        // ===== 阶段2：故意失败重试 RETRY:n:payload =====
        // 测法：POST http://127.0.0.1:8088/send-retry?failTimes=3&msg=hello-retry
        // =====================================================================
        String rest = body.substring(RETRY_PREFIX.length());
        int colon = rest.indexOf(':');
        if (colon <= 0) {
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
        if (reconsumeTimes < failTimes) {
            log.warn("模拟消费失败，将触发重试: payload={}, 已失败次数≈{}, 目标失败次数={}, msgId={}",
                    payload, reconsumeTimes, failTimes, message.getMsgId());
            throw new RuntimeException("demo intentional fail, reconsumeTimes="
                    + reconsumeTimes + ", wantFailTimes=" + failTimes);
        }
        log.info("RECEIVED(after-retry): payload={}, reconsumeTimes={}, 此前故意失败 {} 次",
                payload, reconsumeTimes, failTimes);
    }
    */
}
