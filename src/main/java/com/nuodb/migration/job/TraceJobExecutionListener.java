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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;

/**
 * @author Sergey Bushik
 */
public class TraceJobExecutionListener implements JobExecutionListener {

    private transient final Log log = LogFactory.getLog(getClass());

    @Override
    public void onJobExecuted(JobExecutionEvent event) {
        JobExecution execution = event.getJobExecution();
        JobStatus jobStatus = execution.getJobStatus();
        if (log.isInfoEnabled()) {
            log.info(String.format("Job %s status type is %s",
                    execution.getJob().getName(), jobStatus.getJobStatusType()));
        }
        if (!jobStatus.isRunning()) {
            Date startDate = jobStatus.getExecutionStartDate();
            Date endDate = jobStatus.getExecutionEndDate();
            long duration = endDate.getTime() - startDate.getTime();
            duration(execution, duration);
        }
    }

    protected void duration(JobExecution execution, long duration) {
        if (log.isInfoEnabled()) {
            long left = duration;
            long millis = duration % 1000;
            left = left / 1000;
            long seconds = left % 60;
            left = left / 60;
            long minutes = left % 60;
            left = left / 60;
            long hours = left / 60;
            log.info(String.format("Job %1s execution duration %02d:%02d:%02d.%03d",
                    execution.getJob().getName(), hours, minutes, seconds, millis));
        }
    }
}
