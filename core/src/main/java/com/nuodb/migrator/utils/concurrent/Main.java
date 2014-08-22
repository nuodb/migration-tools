/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import com.google.common.collect.Lists;
import com.nuodb.migrator.utils.Collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.System.err;
import static java.lang.System.out;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sergey Bushik
 */
public class Main {

    public static abstract class ForkJoinTaskWithRawResult<V> extends ForkJoinTask<V> {

        private V rawResult;

        @Override
        public V getRawResult() {
            return rawResult;
        }

        @Override
        public void setRawResult(V rawResult) {
            this.rawResult = rawResult;
        }
    }

    public static void main(final String[] args) throws InterruptedException {
        final ForkJoinPool forkJoinPool = new ForkJoinPool();
        ForkJoinTaskWithRawResult task = new ForkJoinTaskWithRawResult() {

            @Override
            protected boolean exec() {
                out.println("Parent task");
                List<ForkJoinTask> tasks = new ArrayList<ForkJoinTask>();
                for (int i = 0; i < 100; i++) {
                    final int j = i;
                    ForkJoinTaskWithRawResult childTask = new ForkJoinTaskWithRawResult() {
                        @Override
                        protected boolean exec() {
                            out.println("Child task starting " + j);
//                            try {
//                                Thread.sleep(1000);
//                            } catch (InterruptedException error) {
//                                error.printStackTrace();
//                            }
                            out.println("Child task completed " + j);
                            return true;
                        }
                    };
                    try {
                        childTask.fork();
                    } catch (Throwable error) {
                        error.printStackTrace();
                    }
                    tasks.add(childTask);
                }
                for (int i = tasks.size(); i > 0; i--) {
                    ForkJoinTask task = tasks.get(i - 1);
                    out.println("Child task joining " + (i - 1));
                    try {
                        task.join();
                    } catch (Throwable error) {
                        error.printStackTrace();
                    }
                }
                return true;
            }
        };
        forkJoinPool.submit(task);
        forkJoinPool.tryAwaitJoin(task);
        out.print(">>> Completed");
    }
}
