package com.example.rocketmqdemo.handler;

import com.example.rocketmqdemo.dto.DemoCollectMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 【阶段5】压力消息 Handler：只处理 type = PRESSURE。
 */
@Component
public class PressureMessageHandler implements DemoMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(PressureMessageHandler.class);

    /** 和发送端 /send-json?type=PRESSURE 对齐 */
    public static final String TYPE = "PRESSURE";

    @Override
    public String supportType() {
        return TYPE;
    }

    @Override
    public void handle(DemoCollectMessage message) {
        log.info("HANDLER_PRESSURE handled pointCode={} value={} ts={} raw={}",
                message.getPointCode(), message.getValue(), message.getTs(), message);
    }
}
