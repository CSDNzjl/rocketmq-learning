# RocketMQ Spring Boot 最小演示

独立 Spring Boot 3.2 + `rocketmq-spring-boot-starter` 示例：启动发一条消息，并提供 HTTP 接口再发；消费者订阅同一 Topic 并打日志。支持「故意失败 N 次再成功」来观察 RocketMQ 消费重试。

## 环境要求

- JDK 17（本机可用 `D:\jdk`）
- Maven 3.9+
- 本机能访问 NameServer：`192.168.103.143:30094`
- **必须先有 Topic `demo-topic`**（若 Broker 未自动建 Topic，请在 Dashboard 手动创建）

## 配置说明（application.yml）

| 项 | 值 |
|---|---|
| NameServer | `192.168.103.143:30094` |
| 生产者组 | `demo-producer-group` |
| 消费者组 | `demo-consumer-group` |
| Topic | `demo-topic` |
| HTTP 端口 | `8088` |

## 如何运行

```bat
cd /d C:\Users\16899\Desktop\rocketmq-springboot-demo
set JAVA_HOME=D:\jdk
set PATH=%JAVA_HOME%\bin;%PATH%
mvn -DskipTests spring-boot:run
```

或：

```bat
set JAVA_HOME=D:\jdk
mvn -DskipTests package
java -jar target\rocketmq-springboot-demo-1.0.0.jar
```

## 如何测试普通发送

```bat
curl http://127.0.0.1:8088/health
curl -X POST "http://127.0.0.1:8088/send?msg=hello-from-api"
```

期望日志：

- `SENT topic=[demo-topic] body=hello-from-api`
- `RECEIVED: hello-from-api`（若 Broker 拉取正常）

## 如何演示「消费失败 → 自动重试」

### 原理（小白话）

1. 消费者 `onMessage` 里**抛异常** → RocketMQ 认为这次没消费成功。
2. Broker / 客户端稍后会**再投递同一条消息**（`reconsumeTimes` 会 +1）。
3. 直到你处理成功（不抛异常），或超过最大重试次数（本演示 `maxReconsumeTimes=16`）。

### 约定正文格式

`RETRY:{失败次数}:{真正内容}`

例：`RETRY:3:hello` → 前 3 次故意失败，第 4 次成功。

### 一键发重试消息（推荐）

```bat
curl -X POST "http://127.0.0.1:8088/send-retry?failTimes=3&msg=hello"
```

返回 JSON 里的 `mqBody` 就是实际发到队列的字符串，例如 `RETRY:3:hello`。

也可以自己发：

```bat
curl -X POST "http://127.0.0.1:8088/send?msg=RETRY:3:hello"
```

### 控制台应看到的日志（按时间顺序）

1. `SENT topic=[demo-topic] body=RETRY:3:hello`
2. 多次：`收到投递: ... reconsumeTimes=0/1/2 ...`
3. 多次：`模拟消费失败，将触发重试: ...`
4. 最后：`RECEIVED(after-retry): payload=hello, reconsumeTimes=3, 此前故意失败 3 次`

注意：

- **重试不是立刻发生的**，RocketMQ 有延迟级别，两次失败之间可能隔几秒到几十秒，请耐心等。
- 若环境仍存在「只能 SENT、不能消费」的 Broker 故障，则重试也演示不了，需先修好 Broker。

## 代码结构

```
src/main/java/com/example/rocketmqdemo/
  RocketmqDemoApplication.java   # 启动类
  producer/DemoProducer.java      # 发送
  consumer/DemoConsumer.java      # 消费；RETRY: 消息会抛异常触发重试
  controller/SendController.java  # /send 、 /send-retry
```

## 常见问题

1. **No route info of this topic: demo-topic** → Dashboard 先建 Topic。
2. **Maven 用了 JDK 8** → `set JAVA_HOME=D:\jdk`。
3. **只有 SENT、没有 RECEIVED** → 本机曾测到 Broker `broker-jnpf` 拉取时报 `StoreUtil` 相关错误，需运维修 Broker（详见下文）。

## 只有 SENT、没有 RECEIVED？

本机曾核实：发送成功，但拉取时报 Broker 端  
`NoClassDefFoundError: Could not initialize class org.apache.rocketmq.store.StoreUtil`。  
属 Broker 问题，应用侧改代码无法替代修复。Broker 正常后即可看到 `RECEIVED` / 重试日志。