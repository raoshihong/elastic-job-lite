package com.rao.study.elasticjob.spring.job;

import io.elasticjob.lite.api.ShardingContext;
import io.elasticjob.lite.api.simple.SimpleJob;

public class MyElasticJob implements SimpleJob {
    @Override
    public void execute(ShardingContext shardingContext) {
        System.out.println("jobName="+shardingContext.getJobName());
        System.out.println("taskId="+shardingContext.getTaskId());
        System.out.println("shardingItem="+shardingContext.getShardingItem());
        System.out.println("shardingParameter="+shardingContext.getShardingParameter());
        System.out.println("shardingTotalCount="+shardingContext.getShardingTotalCount());

        //分片处理
        switch (shardingContext.getShardingItem()){
            case 0:
                System.out.println("0000000000000");
                break;
            case 1:
                System.out.println("1111111111111");
                break;
            case 2:
                System.out.println("2222222222222");
                break;
        }
    }
}
