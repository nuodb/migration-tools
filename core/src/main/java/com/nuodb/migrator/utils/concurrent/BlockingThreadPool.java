/**
 * Copyright (c) 2015, NuoDB, Inc.
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
package com.nuodb.migrator.utils.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sergey Bushik
 */
public class BlockingThreadPool extends ThreadPoolExecutor {

    public static final long KEEP_ALIVE_TIME = 100L;
    public static final TimeUnit KEEP_ALIVE_TIME_UNIT = MILLISECONDS;

    public BlockingThreadPool(int poolSize, long blockTime, TimeUnit blockTimeUnit) {
        this(poolSize, blockTime, blockTimeUnit, null);
    }

    public BlockingThreadPool(int poolSize, long blockTime, TimeUnit blockTimeUnit,
            Callable<Boolean> blockTimeCallback) {
        this(poolSize, KEEP_ALIVE_TIME, KEEP_ALIVE_TIME_UNIT, blockTime, blockTimeUnit, blockTimeCallback);
    }

    public BlockingThreadPool(int poolSize, long keepAliveTime, TimeUnit keepAliveTimeUnit, long blockTime,
            TimeUnit blockTimeUnit, Callable<Boolean> blockTimeCallback) {
        super(poolSize, poolSize, keepAliveTime, keepAliveTimeUnit, new LinkedBlockingDeque<Runnable>(),
                new BlockingPolicy(blockTime, blockTimeUnit, blockTimeCallback));
        allowCoreThreadTimeOut(true);
    }

    @Override
    public void setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
        throw new UnsupportedOperationException("Setting rejected execution handler is not supported");
    }

    static class BlockingPolicy implements RejectedExecutionHandler {

        private long blockTimeout;
        private TimeUnit blockTimeoutUnit;
        private Callable<Boolean> blockTimeoutCallback;

        private BlockingPolicy(long blockTimeout, TimeUnit blockTimeoutUnit, Callable<Boolean> blockTimeoutCallback) {
            this.blockTimeout = blockTimeout;
            this.blockTimeoutUnit = blockTimeoutUnit;
            this.blockTimeoutCallback = blockTimeoutCallback;
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            BlockingQueue<Runnable> queue = executor.getQueue();
            boolean offered = false;
            while (!offered) {
                if (executor.isShutdown()) {
                    throw new RejectedExecutionException("Executor was shutdown while attempting to offer a new task");
                }
                try {
                    // offer the task to the queue, for a blocking-timeout
                    if (queue.offer(task, blockTimeout, blockTimeoutUnit)) {
                        offered = true;
                    } else {
                        // task was not accepted - call the user's Callback
                        boolean result;
                        try {
                            result = blockTimeoutCallback != null ? blockTimeoutCallback.call() : true;
                        } catch (Exception exception) {
                            // wrap the Callback exception and re-throw
                            throw new RejectedExecutionException(exception);
                        }
                        // check the callback result
                        if (!result) {
                            throw new RejectedExecutionException("Task rejected for submission");
                        }
                    }
                } catch (InterruptedException exception) {
                    // go back to the offer call
                }
            }
        }
    }
}