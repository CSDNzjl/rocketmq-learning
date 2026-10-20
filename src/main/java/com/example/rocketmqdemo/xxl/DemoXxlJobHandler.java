package com.example.rocketmqdemo.xxl;

import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 阶段 XXL-JOB：示例任务 Handler（小白向说明）
 *
 * <p>在 Admin 控制台「任务管理 → 新增」时：
 * <ul>
 *   <li>运行模式选 BEAN</li>
 *   <li><b>JobHandler 必须填：demoHelloJob</b>（与下面 @XxlJob 注解里的名字完全一致）</li>
 *   <li>执行器选 appname = rocketmq-springboot-demo 那一组</li>
 * </ul>
 *
 * <p>保存任务后，点「执行一次」，Admin 会回调本执行器，进入本方法。
 * 任务参数（Admin 里填的「任务参数」）可通过 {@link XxlJobHelper#getJobParam()} 读取；
 * 执行过程日志用 {@link XxlJobHelper#log(String)} 写入，可在 Admin「执行日志」里查看。
 */
@Component
public class DemoXxlJobHandler {

    private static final Logger log = LoggerFactory.getLogger(DemoXxlJobHandler.class);

    /**
     * 示例任务：打印参数并标记成功。
     * Admin JobHandler 名称：demoHelloJob
     */
    @XxlJob("demoHelloJob")
    public void demoHelloJob() {
        String param = XxlJobHelper.getJobParam();
        XxlJobHelper.log("demoHelloJob start, param={}", param);
        log.info("XXL-JOB demoHelloJob executed, param={}", param);

        // 业务可写在这里；示例仅打日志
        XxlJobHelper.log("demoHelloJob done, hello from rocketmq-springboot-demo");
        XxlJobHelper.handleSuccess("ok, param=" + (param == null ? "" : param));
    }
}
