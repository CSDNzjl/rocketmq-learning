package com.example.rocketmqdemo.handler;

import com.example.rocketmqdemo.dto.DemoCollectMessage;

/**
 * 【阶段5】消息处理器接口（对齐熔炼侧「一种 callback / 类型 → 一个 Handler」的思路）。
 *
 * 小白理解：
 * - 接口 = 约定「所有 Handler 都必须会做的两件事」
 * - supportType()：我负责哪种 type（如 TEMP）
 * - handle(...)：真正处理这条消息（Demo 里只打日志）
 *
 * 正式项目里 handle 里会写库、算指标等；这里只练「按类型找到正确类」。
 */
public interface DemoMessageHandler {

    /**
     * 本 Handler 认领的 type 字符串，必须和消息 JSON 里的 type 完全一致（区分大小写）。
     */
    String supportType();

    /**
     * 处理一条已解析好的消息。
     *
     * @param message 从 MQ JSON 反序列化得到的对象
     */
    void handle(DemoCollectMessage message);
}
