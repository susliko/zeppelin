package org.apache.zeppelin.scheduler;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TimeSchedulerTest {
    private PeriodicScheduler s = new TimeScheduler("testScheduler", 2);

    @After
    public void tearDown() {
        s.cancelAll();
    }

    @Test
    public void testDelayedRun() throws InterruptedException {
        Job job = new SleepingJob("job1", null, 200);
        s.submitDelayed(job, 100, TimeUnit.MILLISECONDS);
        Assert.assertFalse(s.getAllJobs().isEmpty());
        Assert.assertFalse(s.isPeriodic(job.getId()));
        Assert.assertEquals(Job.Status.READY, job.getStatus());
        Thread.sleep(200);
        Thread.sleep(200);
        Assert.assertEquals(Job.Status.FINISHED, job.getStatus());
        Assert.assertTrue(s.getAllJobs().isEmpty());
    }

    @Test
    public void testPeriodicRun() throws InterruptedException {
        Job job = new SleepingJob("job1", null, 100);
        s.submitPeriodical(job, 0, 200, TimeUnit.MILLISECONDS);
        Assert.assertTrue(s.isPeriodic(job.getId()));
        Thread.sleep(50);
        Assert.assertEquals(Job.Status.RUNNING, job.getStatus());
        Thread.sleep(100);
        Assert.assertEquals(Job.Status.FINISHED, job.getStatus());
        Thread.sleep(100);
        Assert.assertEquals(Job.Status.RUNNING, job.getStatus());
        s.cancelJob(job.getId());
        Thread.sleep(100);
        Assert.assertEquals(Job.Status.ABORT, job.getStatus());
        Assert.assertTrue(s.getAllJobs().isEmpty());
    }
}
