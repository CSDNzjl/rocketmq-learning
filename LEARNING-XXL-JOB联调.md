# XXL-JOB 联调正确流程（本机 Demo ↔ 线上定制 Admin）

> 配套项目：`rocketmq-springboot-demo`  
> 适用：本机与 Admin **不在同一局域网**，走 ngrok 把执行器暴露给 `192.168.103.143` 上的定制 Admin。  
> 详细入门仍见 `LEARNING.md`「阶段 XXL-JOB」；**本文只保留已跑通的正确路径**。

---

## 一句话结论

必须同时满足：

1. 执行器用公司包 `com.kocel:xxl-job-core:5.0.0-RELEASE`（`jobId` 是 **String**，能装下雪花 ID）  
2. 另加 Groovy：`org.apache.groovy:groovy:4.0.21`（否则启动报 `GroovyClassLoader`）  
3. Admin 执行器地址填 **http** 的 ngrok 域名（不要 https）  
4. 任务参数是 TimeTaskEntity 的 **JSON**（至少 `{}`）  
5. `xxl_job_info.taskId` 指向真实存在的 `base_timetask` 行（面板上看不到该字段）

缺任一项：要么 Admin 空指针不写调度日志，要么请求到本机后 `NumberFormatException`。

---

## 1. 依赖（pom.xml）

**不要用** 官方 `com.xuxueli:xxl-job-core:2.4.1`。  
线上定制 Admin 发来的任务主键是超长雪花数；官方 2.4.1 把 `jobId` 当 `int` 解析 → `NumberFormatException`。

正确依赖：

```xml
<dependency>
  <groupId>com.kocel</groupId>
  <artifactId>xxl-job-core</artifactId>
  <version>5.0.0-RELEASE</version>
</dependency>

<!-- 公司 core 启动 Glue 需要 Groovy -->
<dependency>
  <groupId>org.apache.groovy</groupId>
  <artifactId>groovy</artifactId>
  <version>4.0.21</version>
</dependency>
```

### 私服 / 本地仓

| 场景 | 路径 |
|------|------|
| 公司 Maven settings 的 localRepository | `D:\ifom_repository\com\kocel\xxl-job-core\5.0.0-RELEASE\` |
| IDEA 默认用 `~\.m2` 时 | 需 `mvn install:install-file` 装到 `C:\Users\16899\.m2\repository\com\kocel\xxl-job-core\5.0.0-RELEASE\` |
| 私服聚合仓 | `http://192.168.103.135:8081/repository/maven-public/`（或 `http://harbor.kocel.com:8081/repository/maven-public/`） |

装到默认 `.m2` 示例：

```powershell
mvn install:install-file `
  -DgroupId=com.kocel `
  -DartifactId=xxl-job-core `
  -Dversion=5.0.0-RELEASE `
  -Dpackaging=jar `
  -Dfile=（xxl-job-core-5.0.0-RELEASE.jar 的路径）
```

IDEA：Reload Maven 后再启动。

---

## 2. 配置（application.yml）

| 项 | 值 | 说明 |
|----|----|------|
| Admin 地址 | `http://192.168.103.143:30020/xxl-job-admin/` | 末尾带 `/` |
| accessToken | `432e62f3b488bc861d91b0e274e850cc` | 须与 Admin 一致 |
| appname | `rocketmq-springboot-demo` | 执行器应用名 |
| 执行器 port | `9998` | **不要用 9999**（易冲突） |
| logpath | 本机日志目录即可 | 如 `./logs/xxl-job` |

代码入口：

- 配置类：`XxlJobConfig`（注册 `XxlJobSpringExecutor`）  
- Handler：`@XxlJob("demoHelloJob")`（如 `DemoXxlJobHandler`）

---

## 3. 跨网：ngrok（本机 ↔ Admin 不同局域网）

Admin 必须能 **HTTP 回调** 你的执行器。本机不在同网时：

1. 先停掉旧隧道（避免占端口/旧域名）：

```powershell
Stop-Process -Name ngrok -Force
```

2. 起 **http** 隧道（不要默认 https）：

```powershell
C:\Users\16899\Downloads\ngrok-v3-stable-windows-amd64\ngrok.exe http 9998 --scheme=http
```

3. 在 Admin「执行器管理」里把该执行器地址改成 **手动录入**：

```text
http://<ngrok域名>/
```

注意：

- 必须是 `http://`，不是 `https://`  
- 末尾建议带 `/`  
- 换 ngrok 域名后，Admin 里地址也要改  

探测：对 `http://<ngrok>/beat` 能通（无 token 时可能返回 token 错误 JSON，但说明隧道到本机 9998 已通）。

---

## 4. 定制 Admin 额外要求（和开源 XXL 不同）

线上 Admin 是二次封装，调度前会：

1. 把「任务参数」反序列化成 **TimeTaskEntity**（须是 JSON，不能是纯中文）  
2. 用 `xxl_job_info.taskId` 去查 **`base_timetask`**；查不到 → `entity == null` → NPE，**不写 `xxl_job_log`**

### 4.1 任务参数

至少填：

```json
{}
```

或更完整占位：

```json
{
  "cron": "0/5 * * * * ?",
  "endTime": null,
  "executeType": "1",
  "interfaceId": "0",
  "interfaceName": "demo",
  "localHostTaskId": null,
  "parameter": [],
  "startTime": 0,
  "token": "",
  "userInfo": null
}
```

### 4.2 taskId（面板上看不到）

- XXL 面板上的「任务ID」= `xxl_job_info.id`（雪花），**不是** `taskId`  
- `taskId` 是库字段，指向平台表 `base_timetask.F_Id`  
- 正规入口：平台「任务调度」建任务（自动写 `base_timetask` 并回填）  
- 联调绕过：先 INSERT 占位行，再 UPDATE Demo 任务的 `taskId`  

示例（库名/表以实际为准，联调常用 `tmp_xxjob`）：

```sql
INSERT INTO tmp_xxjob.base_timetask (
  F_Id, F_EnCode, F_FullName, F_ExecuteType,
  F_ExecuteContent, F_ExecuteCycleJson,
  F_RunCount, F_EnabledMark, F_DeleteMark, F_SortCode,
  F_CreatorTime, F_ExecutorName, F_TenantId
) VALUES (
  'demo-hello-task-001',
  'demoHelloJob',
  'Demo Hello Job（联调占位）',
  '1', NULL, NULL, 0, 1, 0, 0,
  NOW(), 'rocketmq-springboot-demo', NULL
);

UPDATE tmp_xxjob.xxl_job_info
SET taskId = 'demo-hello-task-001', update_time = NOW()
WHERE id = '2112348542904905729'
  AND executor_handler = 'demoHelloJob';
```

点「执行一次」时：Admin 用面板任务主键查 `xxl_job_info` → 读出已存的 `taskId` → 再查 `base_timetask`。**不用在面板上手填 taskId。**

关系简述：

- **`base_timetask`**：平台自己的定时任务配置表  
- **`xxl_job_info`**：XXL-JOB 本身的调度配置（Cron、执行器、JobHandler 等）  
- **`taskId`**：把上面两张表串起来（`xxl_job_info.taskId` → `base_timetask.F_Id`）

就**本次 Demo 联调**而言：

- 真正决定「何时跑、打谁、跑哪个 Handler」的，是 **XXL 侧**（执行器地址、JobHandler、Cron）  
- `base_timetask` 占位行只是定制 Admin 调度前的「通行证」--查得到就能往下走；占位行里的业务字段基本没用上  
- **定制层额外**：必须先拿到平台 `TimeTaskEntity`，否则中断（NPE、不写调度日志）


---

## 5. 验证步骤（正确路径）

1. `pom` 已换公司 core + groovy，Maven Reload，**重启 Demo**  
2. ngrok `http 9998 --scheme=http` 在跑；Admin 执行器地址已改成当前 http 域名  
3. 任务：参数为 JSON；`taskId` 已绑到存在的 `base_timetask`  
4. Admin 上确认执行器在线 → 点「执行一次」（或等 Cron）  
5. 本机控制台出现类似：

```text
XXL-JOB demoHelloJob executed, param=...
```

（类名一般为 `DemoXxlJobHandler`）

若仍无本机日志：先看 Admin 是否还在 JSONException / NPE；再看 Demo 是否 `NumberFormatException`（依赖未换对公司包）或 token/地址不对。

---

## 6. 常见坑速查

| 现象 | 原因 | 处理 |
|------|------|------|
| Admin 每 N 秒 `JSONException` | 任务参数是纯文本 | 改成 `{}` 等 JSON |
| Admin `NullPointerException` / `setRunCount` | `taskId` 空或指向不存在的行 | INSERT `base_timetask` 再 UPDATE |
| 本机 `Expected an int but was 2112...` | 仍用官方 2.4.1 | 换 `com.kocel:xxl-job-core:5.0.0-RELEASE` |
| 启动 `NoClassDefFoundError: GroovyClassLoader` | 缺 groovy | 加 `org.apache.groovy:groovy:4.0.21` |
| 隧道通但调度不到 | Admin 填了 https ngrok | 改 http，`--scheme=http` |
| IDEA 找不到公司依赖 | 本地仓不是 settings 那个 | install 到 `~\.m2` 或改 IDEA Maven settings |

---

## 7. 和平台调度页的关系

- **平台「任务调度」**：正规建任务，自动写 `base_timetask` + 回填 `taskId`；可选执行器可能只有已接入业务的 appname，不一定能选本机 Demo。  
- **XXL 面板**：补好 `taskId` 后，仍可用「执行一次」/Cron；不是必须去平台页点执行。  
- **直接调执行器 `/run`**：可验证 Handler 与隧道，但绕过定制 Admin，不算完整调度链路。

---

*文档对应已跑通状态：本机看到 `demoHelloJob` 执行日志。业务代码入口不变：`XxlJobConfig` + `@XxlJob("demoHelloJob")`。*


