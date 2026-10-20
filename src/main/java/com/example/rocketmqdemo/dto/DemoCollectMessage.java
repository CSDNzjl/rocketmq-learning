package com.example.rocketmqdemo.dto;

/**
 * 【阶段5】学习用的采集消息 DTO（Data Transfer Object = 在进程之间传递的数据结构）。
 *
 * 小白理解：
 * - 阶段4 发的是纯文本，例如 STREAM:hello
 * - 阶段5 发的是 JSON，反序列化后变成这个类的对象
 * - 正式 daq 的报文字段更多；这里只保留最常用的几个，方便对照 Handler 分发
 *
 * JSON 示例：
 * {"type":"TEMP","pointCode":"T01","value":36.5,"ts":1730000000000}
 */
public class DemoCollectMessage {

    /**
     * 业务类型，决定交给哪个 Handler。
     * 约定：TEMP=温度，PRESSURE=压力（还可扩展其它字符串）。
     */
    private String type;

    /** 点位编码（学习用假数据，例如 T01 / P02） */
    private String pointCode;

    /** 采集数值 */
    private Double value;

    /** 时间戳（毫秒）。不传时发送端会补当前时间 */
    private Long ts;

    public DemoCollectMessage() {
        // Jackson / Fastjson 反序列化需要无参构造
    }

    public DemoCollectMessage(String type, String pointCode, Double value, Long ts) {
        this.type = type;
        this.pointCode = pointCode;
        this.value = value;
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPointCode() {
        return pointCode;
    }

    public void setPointCode(String pointCode) {
        this.pointCode = pointCode;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "DemoCollectMessage{type='" + type + "', pointCode='" + pointCode
                + "', value=" + value + ", ts=" + ts + "}";
    }
}
