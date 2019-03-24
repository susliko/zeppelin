package org.apache.zeppelin.scheduler;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TimeScheduler implements PeriodicScheduler {

    private Logger logger = LoggerFactory.getLogger(TimeScheduler.class);
    private ScheduledExecutorService executor;
    private Map<String, Job> submittedJobs = new ConcurrentHashMap<>();
    private Map<String, ScheduledFuture<?>> periodicJobs = new ConcurrentHashMap<>();
    private String name;


    public TimeScheduler(String name, int poolSize) {
        this.name = name;
        this.executor = Executors.newScheduledThreadPool(poolSize);
    }

    @Override
    public Boolean isPeriodic(String jobId) {
        return periodicJobs.containsKey(jobId);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Job> getAllJobs() {
        return new ArrayList<>(submittedJobs.values());
    }

    @Override
    public Job getJob(String jobId) {
        return submittedJobs.get(jobId);
    }

    @Override
    public void submitDelayed(Job job, long delay, TimeUnit unit) {
        executor.schedule(() -> runJob(job), delay, unit);
        submittedJobs.put(job.getId(), job);
    }

    @Override
    public void submitPeriodical(Job job, long initDelay, long period, TimeUnit unit) {
        ScheduledFuture<?> fut = executor.scheduleAtFixedRate(() -> runJob(job), initDelay, period, unit);
        submittedJobs.put(job.getId(), job);
        periodicJobs.put(job.getId(), fut);
    }

    @Override
    public Job cancelJob(String jobId) {
        System.out.println();
        ScheduledFuture<?> fut = periodicJobs.remove(jobId);
        if (fut != null) {
            fut.cancel(false);
        }
        Job job = submittedJobs.remove(jobId);
        job.aborted = true;
        job.abort();
        return job;
    }

    @Override
    public void cancelAll() {
        for (String jobId : submittedJobs.keySet()) {
            Job job = submittedJobs.remove(jobId);
            ScheduledFuture<?> fut = periodicJobs.remove(jobId);
            job.aborted = true;
            job.jobAbort();
            if (fut != null) {
                fut.cancel(false);
            }
        }
    }

    private void runJob(Job job) {
        if (job.isAborted()) {
            job.setStatus(Job.Status.ABORT);
            job.aborted = false;
            return;
        }

        logger.info("Job " + job.getId() + " started by scheduler " + name);
        job.run();
        Object jobResult = job.getReturn();
        if (job.isAborted()) {
            job.setStatus(Job.Status.ABORT);
            logger.debug("Job Aborted, " + job.getId() + ", " +
                    job.getErrorMessage());
        } else if (job.getException() != null) {
            logger.debug("Job Error, " + job.getId() + ", " +
                    job.getReturn());
            job.setStatus(Job.Status.ERROR);
        } else if (jobResult instanceof InterpreterResult
                && ((InterpreterResult) jobResult).code() == InterpreterResult.Code.ERROR) {
            logger.debug("Job Error, " + job.getId() + ", " +
                    job.getReturn());
            job.setStatus(Job.Status.ERROR);
        } else {
            logger.debug("Job Finished, " + job.getId() + ", Result: " +
                    job.getReturn());
            job.setStatus(Job.Status.FINISHED);
        }

        logger.info("Job " + job.getId() + " finished by scheduler " + name);
        // reset aborted flag to allow retry
        job.aborted = false;
        if (!periodicJobs.containsKey(job.getId())) {
            submittedJobs.remove(job.getId());
        }
    }
}
