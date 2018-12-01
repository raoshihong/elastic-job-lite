package com.rao.study.elasticjob.spring;

import com.rao.study.elasticjob.spring.job.MyElasticJobTwo;
import io.elasticjob.lite.api.JobScheduler;
import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.LiteJobConfiguration;
import io.elasticjob.lite.config.simple.SimpleJobConfiguration;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import io.elasticjob.lite.reg.zookeeper.ZookeeperConfiguration;
import io.elasticjob.lite.reg.zookeeper.ZookeeperRegistryCenter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Application {
    public static void main(String[] args) {
        customJob();
    }

    public static void useSpring() {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:/elastic-job.xml");
    }

    public static void customJob() {
        //创建注册中心,也就是协调器
        ZookeeperConfiguration zookeeperConfiguration = new ZookeeperConfiguration("localhost:2181", "dd-job-study");
        zookeeperConfiguration.setBaseSleepTimeMilliseconds(1000);
        zookeeperConfiguration.setMaxSleepTimeMilliseconds(3000);
        zookeeperConfiguration.setMaxRetries(3);

        CoordinatorRegistryCenter registryCenter = new ZookeeperRegistryCenter(zookeeperConfiguration);
        registryCenter.init();//调用init启动zookeeper客户端,连接zookeeper服务

        //job的核心配置JobCoreConfiguration,这个类采用了构建者模式
        JobCoreConfiguration jobCoreConfiguration = JobCoreConfiguration.newBuilder("myJob", "0/10 * * * * ?", 3).shardingItemParameters("0=A,1=B,2=C").failover(true).build();

        //创建SimpleJobConfiguration,并绑定任务job
        SimpleJobConfiguration simpleJobConfiguration = new SimpleJobConfiguration(jobCoreConfiguration, MyElasticJobTwo.class.getName());

        //lite作业配置,增加一些任务监听等配置  jobShardingStrategyClass 配置Job的分片策略
        LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(simpleJobConfiguration).monitorPort(8881).jobShardingStrategyClass("io.elasticjob.lite.api.strategy.impl.AverageAllocationJobShardingStrategy").build();

        //启动调度
        new JobScheduler(registryCenter, liteJobConfiguration).init();


        //上面的步骤跟quartz的任务调度的步骤是一致的
        //1.创建trigger触发器规则
        //2.绑定trigger和job
        //3.通过scheduler调度任务

        //而elastic-job-lite是基于quartz开发的,所以可以理解为elastic-job-lite是在quartz调度进行包装，在任务调度中插入了一个调度策略,来分配任务

    }
}