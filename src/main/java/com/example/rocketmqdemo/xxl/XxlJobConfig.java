package com.example.rocketmqdemo.xxl;

import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 阶段 XXL-JOB：执行器配置类（小白向说明）
 *
 * <p>XXL-JOB 分两块：
 * <ul>
 *   <li><b>调度中心 Admin</b>：网页控制台，负责任务配置、触发、看日志（本 Demo 连远程
 *       http://192.168.103.143:30020/xxl-job-admin/）。</li>
 *   <li><b>执行器 Executor</b>：就是本 Spring Boot 进程。启动后向 Admin 注册，
 *       Admin 触发任务时会回调本机 port（默认 9998），执行带 @XxlJob 的方法。</li>
 * </ul>
 *
 * <p>本类只做一件事：把 application.yml 里的 xxl.job.* 读出来，创建并交给 Spring 管理
 * {@link XxlJobSpringExecutor}。官方 Spring Boot 示例也是这个写法。
 *
 * <p>注意：appname / accessToken 必须和 Admin 里「执行器管理」配置一致，否则注册不上或鉴权失败。
 */
@Configuration
public class XxlJobConfig {

    private static final Logger log = LoggerFactory.getLogger(XxlJobConfig.class);

    /** Admin 地址，多个可用逗号分隔；末尾建议带 /xxl-job-admin/ */
    @Value("${xxl.job.admin.addresses}")
    private String adminAddresses;

    /** 通讯 TOKEN，须与 Admin 配置的 accessToken 一致 */
    @Value("${xxl.job.accessToken}")
    private String accessToken;

    /** 执行器 AppName，Admin「执行器管理」里新建时填同一个名字 */
    @Value("${xxl.job.executor.appname}")
    private String appname;

    /** 执行器注册地址（可空：空则自动推断） */
    @Value("${xxl.job.executor.address:}")
    private String address;

    /** 执行器 IP（可空：空则自动获取） */
    @Value("${xxl.job.executor.ip:}")
    private String ip;

    /** 执行器端口：Admin 回调本机用；勿与线上占用的 9999 冲突，本 Demo 用 9998 */
    @Value("${xxl.job.executor.port}")
    private int port;

    /** 执行器运行日志存放目录 */
    @Value("${xxl.job.executor.logpath}")
    private String logpath;

    /** 日志保留天数 */
    @Value("${xxl.job.executor.logretentiondays}")
    private int logretentiondays;

    @Bean
    public XxlJobSpringExecutor xxlJobExecutor() {
        log.info(">>>>>>>>>>> xxl-job config init. adminAddresses={}, appname={}, port={}",
                adminAddresses, appname, port);

        XxlJobSpringExecutor executor = new XxlJobSpringExecutor();
        executor.setAdminAddresses(adminAddresses);
        executor.setAppname(appname);
        executor.setAddress(address);
        executor.setIp(ip);
        executor.setPort(port);
        executor.setAccessToken(accessToken);
        executor.setLogPath(logpath);
        executor.setLogRetentionDays(logretentiondays);
        return executor;
    }
}
