/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migration.job;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * @author Sergey Bushik
 */
public class JobExecutorImpl implements JobExecutor {

    private transient final Log log = LogFactory.getLog(getClass());
    private final Job job;
    private final JobStatusImpl jobStatus;
    private List<JobExecutionListener> listeners = Lists.newArrayList();

    public JobExecutorImpl(Job job) {
        this.job = job;
        this.jobStatus = new JobStatusImpl();
    }

    @Override
    public JobStatus getJobStatus() {
        return jobStatus;
    }

    @Override
    public void addJobExecutionListener(JobExecutionListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeJobExecutionListener(JobExecutionListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void pause() {
        synchronized (jobStatus) {
            if (!jobStatus.isPaused()) {
                jobStatus.pause();
            }
        }
    }

    @Override
    public void resume() {
        synchronized (jobStatus) {
            if (jobStatus.isPaused()) {
                jobStatus.resume();
            }
        }
    }

    @Override
    public boolean execute(Map<String, Object> context) {
        synchronized (jobStatus) {
            if (!jobStatus.isRunning() && !jobStatus.isStopped()) {
                jobStatus.setRunning(true);
                jobStatus.setExecutionStartDate(new Date());
            } else {
                if (log.isDebugEnabled()) {
                    log.info(String.format("Job %1$s is already running or it has been stop", job.getName()));
                }
                return false;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Starting execution of %1$s job", job.getName()));
        }
        JobExecution execution = createJobExecution(context);
        try {
            fireJobExecutionEvent(new JobExecutionEvent(execution));
            job.execute(execution);
            synchronized (jobStatus) {
                jobStatus.setExecutionEndDate(new Date());
                jobStatus.setRunning(false);
            }
            fireJobExecutionEvent(new JobExecutionEvent(execution));
        } catch (Throwable error) {
            if (log.isErrorEnabled()) {
                log.error(String.format("Job %1$s execution failed", job.getName()), error);
            }
            synchronized (jobStatus) {
                jobStatus.setExecutionEndDate(new Date());
                jobStatus.setRunning(false);
                jobStatus.setFailure(error);
            }
            fireJobExecutionEvent(new JobExecutionEvent(execution));
        }
        return true;
    }

    protected JobExecution createJobExecution(Map<String, Object> context) {
        return new JobExecutionImpl(job, jobStatus, context);
    }

    protected void fireJobExecutionEvent(JobExecutionEvent event) {
        for (JobExecutionListener listener : listeners) {
            listener.onJobExecuted(event);
        }
    }

    @Override
    public void stop() {
        synchronized (jobStatus) {
            if (!jobStatus.isStopped()) {
                jobStatus.stop();
            }
        }
    }

    @Override
    public Job getJob() {
        return job;
    }
}
