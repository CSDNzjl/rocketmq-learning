package com.example.rocketmqdemo.handler;

import com.example.rocketmqdemo.dto.DemoCollectMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 【阶段5】温度消息 Handler：只处理 type = TEMP。
 *
 * Spring 的 @Component 会把它注册成 Bean；
 * DemoMessageDispatcher 启动时会收集所有 DemoMessageHandler，按 supportType 建表。
 */
@Component
public class TempMessageHandler implements DemoMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(TempMessageHandler.class);

    /** 和发送端 /send-json?type=TEMP 对齐 */
    public static final String TYPE = "TEMP";

    @Override
    public String supportType() {
        return TYPE;
    }

    @Override
    public void handle(DemoCollectMessage message) {
        // 学习阶段：只打日志。真实业务这里可能写温度表、判断超温等。
        log.info("HANDLER_TEMP handled pointCode={} value={} ts={} raw={}",
                message.getPointCode(), message.getValue(), message.getTs(), message);
    }
}
