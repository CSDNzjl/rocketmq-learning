package com.example.rocketmqdemo.stream;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

/**
 * 【阶段4】用 Spring Cloud Stream 收消息（对齐 fom-smelt 的 RocketMQConsumerConfig）。
 *
 * 关键点：
 * 1. 写一个 @Bean，类型是 {@code Consumer<String>}（或你自己的消息类型）
 * 2. Bean 方法名 = demoCollect
 * 3. application.yml 里 spring.cloud.function.definition=demoCollect
 * 4. 框架自动创建入站 binding 名：demoCollect-in-0
 * 5. yml 里把 demoCollect-in-0 的 destination 指到真正 Topic，并写上 group
 *
 * fom-smelt 真实写法：
 *   @Bean public Consumer<String> daqCollect() { ... }
 *   binding：daqCollect-in-0，group：fom-smelt-daq
 */
@Configuration
public class StreamDemoConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamDemoConsumerConfig.class);

    @PostConstruct
    public void onReady() {
        log.info("阶段4 Stream 消费者已注册：Bean 名 demoCollect → binding demoCollect-in-0。"
                + " 测法：POST /send-stream?msg=hello-stream");
    }

    /**
     * 函数式消费者：每来一条消息，框架就调用一次 accept。
     * 方法名 demoCollect 不要随便改，除非同时改 yml 的 function.definition 和 bindings 前缀。
     */
    @Bean
    public Consumer<String> demoCollect() {
        return payload -> {
            // 学习阶段只打日志；真实业务里这里会像熔炼一样 dispatch 到 Handler
            log.info("STREAM_RECEIVED(via demoCollect-in-0): {}", payload);
        };
    }
}
