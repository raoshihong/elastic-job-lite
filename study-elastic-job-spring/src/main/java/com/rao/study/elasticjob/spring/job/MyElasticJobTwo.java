package com.rao.study.elasticjob.spring.job;

import io.elasticjob.lite.api.ShardingContext;
import io.elasticjob.lite.api.simple.SimpleJob;

public class MyElasticJobTwo implements SimpleJob {
    @Override
    public void execute(ShardingContext shardingContext) {
        System.out.println("sss");
    }
}
