package org.apache.zeppelin.scheduler;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface PeriodicScheduler {

    String getName();

    Boolean isPeriodic(String jobId);

    List<Job> getAllJobs();

    Job getJob(String jobId);

    void submitDelayed(Job job, long delay, TimeUnit unit);

    void submitPeriodical(Job job, long delay, long period, TimeUnit unit);

    Job cancelJob(String jobId);

    void cancelAll();
}
