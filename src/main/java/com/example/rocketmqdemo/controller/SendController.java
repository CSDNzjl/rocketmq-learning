package com.example.rocketmqdemo.controller;

import com.example.rocketmqdemo.consumer.DemoConsumer;
import com.example.rocketmqdemo.producer.DemoProducer;
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
 * - POST /send          发普通消息（消费一次成功）
 * - POST /send-retry    发「故意失败 N 次再成功」的消息，用来观察 RocketMQ 重试
 */
@RestController
public class SendController {

    private final DemoProducer demoProducer;

    public SendController(DemoProducer demoProducer) {
        this.demoProducer = demoProducer;
    }

    /** 健康检查：GET http://127.0.0.1:8088/health */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> m = new HashMap<>();
        m.put("ok", true);
        m.put("app", "rocketmq-springboot-demo");
        return m;
    }

    /**
     * 发普通消息。
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
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }

    /**
     * 发一条「学习用」重试消息。
     *
     * 例：POST http://127.0.0.1:8088/send-retry?failTimes=3&msg=hello
     * 实际发到 MQ 的正文：RETRY:3:hello
     * 消费者会故意失败 3 次，第 4 次才打印 RECEIVED(after-retry)。
     *
     * @param failTimes 故意失败几次（建议 1～5；须小于消费者 maxReconsumeTimes=16）
     * @param msg       成功后日志里展示的真正内容
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
            // 避免一发就很快进死信，学习时没必要太大
            failTimes = 15;
        }
        // 拼出消费者约定的格式
        String mqBody = DemoConsumer.RETRY_PREFIX + failTimes + ":" + msg;
        try {
            demoProducer.send(mqBody);
            body.put("success", true);
            body.put("failTimes", failTimes);
            body.put("msg", msg);
            body.put("mqBody", mqBody);
            body.put("hint", "看控制台：先出现多次「模拟消费失败」，最后出现 RECEIVED(after-retry)。重试有延迟，请多等一会儿。");
            return ResponseEntity.ok(body);
        } catch (Exception e) {
            body.put("success", false);
            body.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(body);
        }
    }
}