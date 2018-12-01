package io.elasticjob.lite.internal.schedule;

import io.elasticjob.lite.api.ElasticJob;
import io.elasticjob.lite.executor.JobExecutorFactory;
import io.elasticjob.lite.executor.JobFacade;
import lombok.Setter;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Lite调度作业.
 *
 * @author zhangliang
 */
public final class LiteJob implements Job {

    //这两个属性的值是在
    @Setter
    private ElasticJob elasticJob;
    
    @Setter
    private JobFacade jobFacade;
    
    @Override
    public void execute(final JobExecutionContext context) throws JobExecutionException {
        //通过job调度器工厂，根据job类型进行调用ElasticJob子类对应的任务

        //所以整个任务的入口就是在这里
        JobExecutorFactory.getJobExecutor(elasticJob, jobFacade).execute();
    }
}
