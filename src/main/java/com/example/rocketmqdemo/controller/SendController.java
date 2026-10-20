package com.example.rocketmqdemo.controller;

import com.example.rocketmqdemo.consumer.DemoConsumer;
import com.example.rocketmqdemo.dto.DemoCollectMessage;
import com.example.rocketmqdemo.producer.DemoProducer;
import com.example.rocketmqdemo.stream.StreamDemoProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * HTTP 接口层：浏览器 / curl 触发发消息。
 *
 * - POST /send          阶段1：直连发送（当前直连监听已压测注释）
 * - POST /send-retry    阶段2：直连重试（同上）
 * - POST /send-route    阶段3：moduleName 路由（同上）
 * - POST /send-stream   阶段4/6：StreamBridge 纯文本（默认 Tag=STREAM）
 * - POST /send-json     阶段5/6：发 JSON，Tag=type；入站按 subscription 过滤后再 Handler
 */
@RestController
public class SendController {

    private final DemoProducer demoProducer;
    private final StreamDemoProducer streamDemoProducer;

    /** 与 yml demo.stream.subscription-tags 一致，给 /health 和接口提示用 */
    @Value("${demo.stream.subscription-tags:*}")
    private String subscriptionTags;

    public SendController(DemoProducer demoProducer, StreamDemoProducer streamDemoProducer) {
        this.demoProducer = demoProducer;
        this.streamDemoProducer = streamDemoProducer;
    }

    /** 健康检查：GET http://127.0.0.1:8088/health */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> m = new HashMap<>();
        m.put("ok", true);
        m.put("app", "rocketmq-springboot-demo");
        m.put("stage", 6);
        m.put("subscriptionTags", subscriptionTags);
        m.put("hint", "阶段6：同 Topic + Tag 提前过滤。默认只订 TEMP。"
                + " 试 POST /send-json?type=TEMP 应收到；type=PRESSURE 应只有 JSON_SENT 无 RECEIVED。"
                + " 改 demo.stream.subscription-tags 后重启。");
        return m;
    }

    /** 阶段1：POST http://127.0.0.1:8088/send?msg=hello */
    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> send(
            @RequestParam(value = "msg", defaultValue = "hello-rocketmq") String msg) {
        Map<String, Object> body = new HashMap<>();
        try {
            demoProducer.send(msg);
            body.put("success", true);
            body.put("msg", msg);
            body.put("hint", "直连已发送。阶段5起 DemoConsumer 监听已压测；要学阶段1请恢复 DemoConsumer。");
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /** 阶段2：POST http://127.0.0.1:8088/send-retry?failTimes=3&msg=hello */
    @PostMapping("/send-retry")
    public ResponseEntity<Map<String, Object>> sendRetry(
            @RequestParam(value = "failTimes", defaultValue = "3") int failTimes,
            @RequestParam(value = "msg", defaultValue = "hello-retry") String msg) {
        Map<String, Object> body = new HashMap<>();
        if (failTimes < 0) {
            failTimes = 0;
        }
        if (failTimes > 15) {
            failTimes = 15;
        }
        String mqBody = DemoConsumer.RETRY_PREFIX + failTimes + ":" + msg;
        try {
            demoProducer.send(mqBody);
            body.put("success", true);
            body.put("failTimes", failTimes);
            body.put("msg", msg);
            body.put("mqBody", mqBody);
            body.put("hint", "要复学重试，恢复 DemoConsumer 直连监听与阶段2逻辑后再测。");
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /** 阶段3：POST http://127.0.0.1:8088/send-route?moduleName=smelt&msg=hello */
    @PostMapping("/send-route")
    public ResponseEntity<Map<String, Object>> sendRoute(
            @RequestParam(value = "moduleName", defaultValue = "smelt") String moduleName,
            @RequestParam(value = "msg", defaultValue = "hello-route") String msg) {
        Map<String, Object> body = new HashMap<>();
        try {
            demoProducer.sendRoute(moduleName, msg);
            body.put("success", true);
            body.put("moduleName", moduleName);
            body.put("msg", msg);
            body.put("hint", "要复学路由，恢复 DemoConsumer 直连监听与阶段3逻辑后再测。");
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /**
     * 阶段4/6：POST http://127.0.0.1:8088/send-stream?msg=hello-stream
     * 默认 Tag=STREAM；若 subscription 只订 TEMP，会有 SENT 无 RECEIVED。
     */
    @PostMapping("/send-stream")
    public ResponseEntity<Map<String, Object>> sendStream(
            @RequestParam(value = "msg", defaultValue = "hello-stream") String msg) {
        Map<String, Object> body = new HashMap<>();
        try {
            String payload = "STREAM:" + msg;
            boolean ok = streamDemoProducer.send(payload);
            body.put("success", ok);
            body.put("msg", msg);
            body.put("mqBody", payload);
            body.put("tag", "STREAM");
            body.put("bindingOut", "demoCollect-out-0");
            body.put("bindingIn", "demoCollect-in-0");
            body.put("destination", "demo-topic");
            body.put("subscriptionTags", subscriptionTags);
            body.put("hint", ok
                    ? ("已发送 Tag=STREAM。当前订阅=[" + subscriptionTags
                    + "]；若不含 STREAM 或 *，日志只有 STREAM_SENT，不会有 STREAM_RECEIVED。")
                    : "StreamBridge 返回 false，检查 NameServer / Topic / binder 配置");
            return ok ? ResponseEntity.ok(body) : ResponseEntity.internalServerError().body(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /**
     * 阶段5/6：POST /send-json?type=TEMP&pointCode=T01&value=36.5
     * 发送端 Tag=type；入站先按 Tag 过滤，再按 type 分发 Handler。
     */
    @PostMapping("/send-json")
    public ResponseEntity<Map<String, Object>> sendJson(
            @RequestParam(value = "type", defaultValue = "TEMP") String type,
            @RequestParam(value = "pointCode", defaultValue = "T01") String pointCode,
            @RequestParam(value = "value", defaultValue = "36.5") double value,
            @RequestParam(value = "ts", required = false) Long ts) {
        Map<String, Object> body = new HashMap<>();
        try {
            DemoCollectMessage message = new DemoCollectMessage();
            message.setType(type);
            message.setPointCode(pointCode);
            message.setValue(value);
            message.setTs(ts);
            boolean ok = streamDemoProducer.sendJson(message);
            String tag = type == null ? "" : type.trim().toUpperCase(Locale.ROOT);
            body.put("success", ok);
            body.put("type", message.getType());
            body.put("tag", tag);
            body.put("pointCode", message.getPointCode());
            body.put("value", message.getValue());
            body.put("ts", message.getTs());
            body.put("bindingOut", "demoCollect-out-0");
            body.put("subscriptionTags", subscriptionTags);
            body.put("hint", ok
                    ? ("已发送 Tag=" + tag + "。当前订阅=[" + subscriptionTags
                    + "]。只有 Tag 匹配订阅时才会出现 JSON_RECEIVED → DISPATCH → HANDLER_xxx。"
                    + " 不匹配时只有 JSON_SENT，属阶段6正常现象。")
                    : "发送失败，检查 NameServer / Topic / binder");
            return ok ? ResponseEntity.ok(body) : ResponseEntity.internalServerError().body(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }
}
