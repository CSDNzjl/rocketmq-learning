# 学习：同 Topic 按 moduleName 路由

对应 fom-daq 里 **一个 Topic（如 `daqCollect`）被多个业务模块共用**，靠消息体里的 **`moduleName`** 决定谁处理。

本 Demo **没有新建 Topic**，仍用 `demo-topic` + `demo-consumer-group`。只是把消息 body 改成 JSON，消费者按配置过滤。

## 配置

`application.yml`：

```yaml
demo:
  route:
    my-module: smelt   # 本进程认领的模块名；可改成 furnace 等做对比实验
```

## 新接口（旧接口不动）

| 接口 | 作用 |
|------|------|
| `POST /send` | 普通消息（旧） |
| `POST /send-retry` | 故意失败重试（旧） |
| **`POST /send-route`** | **新增：带 moduleName 的 JSON** |

发出去的 MQ body 示例：

```json
{"moduleName":"smelt","payload":"hello-route"}
```

## 怎么测

先启动应用（端口 8088），然后：

**1）命中本模块（应 PROCESS）**

```bat
curl -X POST "http://127.0.0.1:8088/send-route?moduleName=smelt&msg=hello-route"
```

PowerShell：

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8088/send-route?moduleName=smelt&msg=hello-route"
```

控制台应出现：

- `SENT(route) ... moduleName=smelt ...`
- `ROUTE_PROCESS: 本模块消息 myModule=smelt, moduleName=smelt, payload=hello-route ...`

**2）非本模块（应 SKIP，且不再重试）**

```bat
curl -X POST "http://127.0.0.1:8088/send-route?moduleName=furnace&msg=other-module"
```

控制台应出现：

- `SENT(route) ... moduleName=furnace ...`
- `ROUTE_SKIP: 非本模块消息 myModule=smelt, moduleName=furnace ...`

注意：SKIP 是 **正常 return**，**不抛异常**。若误抛异常，RocketMQ 会当成消费失败去重试，那就错了。

**3）确认旧功能仍可用**

```bat
curl -X POST "http://127.0.0.1:8088/send?msg=plain-still-ok"
curl -X POST "http://127.0.0.1:8088/send-retry?failTimes=2&msg=retry-still-ok"
```

应仍看到原来的 `RECEIVED` / `RECEIVED(after-retry)` 日志。

## 和 fom-daq 的对应关系（一段话）

fom-daq 采集侧往往往共享 Topic（例如 `daqCollect`）丢消息，body 里带 `moduleName`（以及回调等信息）；各业务模块的消费者都订阅同一 Topic，但只处理 `moduleName` 属于自己的消息，其它消息直接跳过。本 Demo 的 `/send-route` + `demo.route.my-module` + `ROUTE_PROCESS` / `ROUTE_SKIP` 就是把这件事缩成最小可跑通的例子，方便你对照正式项目阅读。

## 代码落点

- `DemoProducer.sendRoute` - 新增发送
- `SendController` 的 `/send-route` - 新增 HTTP
- `DemoConsumer.onMessage` 开头的路由分支 - 新增过滤（其后 RETRY / 普通逻辑未改）
- `demo.route.my-module` - 认领哪个模块
