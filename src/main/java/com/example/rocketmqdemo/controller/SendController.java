package com.example.rocketmqdemo.controller;

import com.example.rocketmqdemo.consumer.DemoConsumer;
import com.example.rocketmqdemo.producer.DemoProducer;
import com.example.rocketmqdemo.stream.StreamDemoProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * HTTP 接口层：用浏览器 / curl 触发发消息。
 *
 * - POST /send          阶段1（直连）；当前直连消费者已关，仅练发送
 * - POST /send-retry    阶段2（直连重试）；同上，需恢复 DemoConsumer 才能重学
 * - POST /send-route    阶段3（moduleName 路由）；同上
 * - POST /send-stream   阶段4【当前启用】StreamBridge → demoCollect-out-0
 *
 * 各阶段在消费者侧用注释分隔，见 DemoConsumer / StreamDemoConsumerConfig。
 */
@RestController
public class SendController {

    private final DemoProducer demoProducer;
    private final StreamDemoProducer streamDemoProducer;

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
        m.put("stage", 4);
        m.put("hint", "当前学 Spring Cloud Stream binding；测 POST /send-stream");
        return m;
    }

    /**
     * 阶段1：直连发普通消息。
     * POST http://127.0.0.1:8088/send?msg=hello
     */
    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> send(
            @RequestParam(value = "msg", defaultValue = "hello-rocketmq") String msg) {
        Map<String, Object> body = new HashMap<>();
        try {
            demoProducer.send(msg);
            body.put("success", true);
            body.put("msg", msg);
            body.put("hint", "直连已发送。阶段4下 DemoConsumer 监听已关；重学阶段1请恢复 DemoConsumer。");
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /**
     * 阶段2：故意失败重试。
     * POST http://127.0.0.1:8088/send-retry?failTimes=3&msg=hello
     */
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
            body.put("hint", "要重学重试：恢复 DemoConsumer 直连监听与阶段2逻辑后重启。");
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /**
     * 阶段3：按 moduleName 路由的 JSON。
     * POST http://127.0.0.1:8088/send-route?moduleName=smelt&msg=hello-route
     */
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
            body.put("hint", "要重学路由：恢复 DemoConsumer 直连监听与阶段3逻辑后重启。");
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /**
     * 【阶段4·当前启用】经 Spring Cloud Stream 出站 binding 发消息。
     *
     * 调用：
     *   POST http://127.0.0.1:8088/send-stream?msg=hello-stream
     *
     * 链路：
     *   StreamBridge.send("demoCollect-out-0") 
     *     → yml destination=demo-topic
     *     → Consumer Bean demoCollect（binding demoCollect-in-0）打日志 STREAM_RECEIVED
     *
     * 对照 fom-daq：send("daqCollect-out-0")；fom-smelt：Consumer daqCollect。
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
            body.put("bindingOut", "demoCollect-out-0");
            body.put("bindingIn", "demoCollect-in-0");
            body.put("destination", "demo-topic");
            body.put("hint", ok
                    ? "看控制台：STREAM_SENT 然后 STREAM_RECEIVED(via demoCollect-in-0)"
                    : "StreamBridge 返回 false，检查 NameServer / Topic / binder 配置");
            return ok ? ResponseEntity.ok(body) : ResponseEntity.internalServerError().body(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }
}
