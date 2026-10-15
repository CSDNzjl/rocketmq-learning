package com.example.rocketmqdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 整个演示项目的「入口类」（启动类）。
 *
 * 你运行 main 方法后，Spring Boot 会：
 * 1. 读取 application.yml 配置
 * 2. 扫描本包及子包下的 @Component / @Service / @RestController 等类
 * 3. 自动连上 RocketMQ NameServer
 * 4. 启动内嵌 Web 服务器（本项目端口 8088）
 * 5. 触发生产者启动时发一条消息；消费者开始监听 Topic
 *
 * 小白提示：平时改业务逻辑一般不改这个文件，改它下面的 producer / consumer / controller 即可。
 */
@SpringBootApplication // 这是一个组合注解：打开自动配置、组件扫描、配置支持
public class RocketmqDemoApplication {

    /**
     * Java 程序真正开始执行的地方。
     * 右键 Run，或命令行 java -jar / mvn spring-boot:run，最终都会进到这里。
     */
    public static void main(String[] args) {
        // 把当前类交给 Spring Boot，由它创建「应用上下文」并启动
        SpringApplication.run(RocketmqDemoApplication.class, args);
    }
}