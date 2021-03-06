/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * This file is available under and governed by the GNU General Public
 * License version 2 only, as published by the Free Software Foundation.
 * However, the following notice accompanied the original version of this
 * file:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package com.nuodb.migrator.utils.concurrent;

/**
 * A recursive resultless {@link ForkJoinTask}. This class establishes
 * conventions to parameterize resultless actions as {@code Void}
 * {@code ForkJoinTask}s. Because {@code null} is the only valid value of type
 * {@code Void}, methods such as join always return {@code null} upon
 * completion.
 * <p/>
 * <p>
 * <b>Sample Usages.</b> Here is a sketch of a ForkJoin sort that sorts a given
 * {@code long[]} array:
 * <p/>
 * 
 * <pre>
 * {
 *     &#64;code
 *     class SortTask extends RecursiveAction {
 *         final long[] array;
 *         final int lo;
 *         final int hi;
 * 
 *         SortTask(long[] array, int lo, int hi) {
 *             this.array = array;
 *             this.lo = lo;
 *             this.hi = hi;
 *         }
 * 
 *         protected void compute() {
 *             if (hi - lo < THRESHOLD)
 *                 sequentiallySort(array, lo, hi);
 *             else {
 *                 int mid = (lo + hi) >>> 1;
 *                 invokeAll(new SortTask(array, lo, mid), new SortTask(array, mid, hi));
 *                 merge(array, lo, hi);
 *             }
 *         }
 *     }
 * }
 * </pre>
 * <p/>
 * You could then sort {@code anArray} by creating
 * {@code new SortTask(anArray, 0, anArray.length-1) } and invoking it in a
 * ForkJoinPool. As a more concrete simple example, the following task
 * increments each element of an array:
 * 
 * <pre>
 * {
 *     &#64;code
 *     class IncrementTask extends RecursiveAction {
 *         final long[] array;
 *         final int lo;
 *         final int hi;
 * 
 *         IncrementTask(long[] array, int lo, int hi) {
 *             this.array = array;
 *             this.lo = lo;
 *             this.hi = hi;
 *         }
 * 
 *         protected void compute() {
 *             if (hi - lo < THRESHOLD) {
 *                 for (int i = lo; i < hi; ++i)
 *                     array[i]++;
 *             } else {
 *                 int mid = (lo + hi) >>> 1;
 *                 invokeAll(new IncrementTask(array, lo, mid), new IncrementTask(array, mid, hi));
 *             }
 *         }
 *     }
 * }
 * </pre>
 * <p/>
 * <p>
 * The following example illustrates some refinements and idioms that may lead
 * to better performance: RecursiveActions need not be fully recursive, so long
 * as they maintain the basic divide-and-conquer approach. Here is a class that
 * sums the squares of each element of a double array, by subdividing out only
 * the right-hand-sides of repeated divisions by two, and keeping track of them
 * with a chain of {@code next} references. It uses a dynamic threshold based on
 * method {@code getSurplusQueuedTaskCount}, but counterbalances potential
 * excess partitioning by directly performing leaf actions on unstolen tasks
 * rather than further subdividing.
 * <p/>
 * 
 * <pre>
 *  {@code
 * double sumOfSquares(ForkJoinPool pool, double[] array) {
 *   int n = array.length;
 *   Applyer a = new Applyer(array, 0, n, null);
 *   pool.invoke(a);
 *   return a.result;
 * }
 * <p/>
 * class Applyer extends RecursiveAction {
 *   final double[] array;
 *   final int lo, hi;
 *   double result;
 *   Applyer next; // keeps track of right-hand-side tasks
 *   Applyer(double[] array, int lo, int hi, Applyer next) {
 *     this.array = array; this.lo = lo; this.hi = hi;
 *     this.next = next;
 *   }
 * <p/>
 *   double atLeaf(int l, int h) {
 *     double sum = 0;
 *     for (int i = l; i < h; ++i) // perform leftmost base step
 *       sum += array[i] * array[i];
 *     return sum;
 *   }
 * <p/>
 *   protected void compute() {
 *     int l = lo;
 *     int h = hi;
 *     Applyer right = null;
 *     while (h - l > 1 && getSurplusQueuedTaskCount() <= 3) {
 *        int mid = (l + h) >>> 1;
 *        right = new Applyer(array, mid, h, right);
 *        right.fork();
 *        h = mid;
 *     }
 *     double sum = atLeaf(l, h);
 *     while (right != null) {
 *        if (right.tryUnfork()) // directly calculate if not stolen
 *          sum += right.atLeaf(right.lo, right.hi);
 *       else {
 *          right.join();
 *          sum += right.result;
 *        }
 *        right = right.next;
 *      }
 *     result = sum;
 *   }
 * }}
 * </pre>
 *
 * @author Doug Lea
 * @since 1.7
 */
public abstract class RecursiveAction extends ForkJoinTask<Void> {
    private static final long serialVersionUID = 5232453952276485070L;

    /**
     * The main computation performed by this task.
     */
    protected abstract void compute();

    /**
     * Always returns {@code null}.
     *
     * @return {@code null} always
     */
    public final Void getRawResult() {
        return null;
    }

    /**
     * Requires null completion value.
     */
    protected final void setRawResult(Void mustBeNull) {
    }

    /**
     * Implements execution conventions for RecursiveActions.
     */
    protected final boolean exec() {
        compute();
        return true;
    }
}