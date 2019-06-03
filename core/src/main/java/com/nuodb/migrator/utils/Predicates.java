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
package com.nuodb.migrator.utils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import java.io.Serializable;
import java.util.Collection;
import java.util.regex.Pattern;

/**
 * @author Sergey Bushik
 */

public class Predicates {

    public static <T> Predicate<T> alwaysTrue() {
        return com.google.common.base.Predicates.alwaysTrue();
    }

    public static Predicate<Class<?>> assignableFrom(Class<?> clazz) {
        return com.google.common.base.Predicates.assignableFrom(clazz);
    }

    public static <T> Predicate<T> alwaysFalse() {
        return com.google.common.base.Predicates.alwaysFalse();
    }

    public static <T> Predicate<T> and(Predicate<? super T> first, Predicate<? super T> second) {
        return com.google.common.base.Predicates.and(first, second);
    }

    public static <T> Predicate<T> or(Iterable<? extends Predicate<? super T>> components) {
        return com.google.common.base.Predicates.or(components);
    }

    public static <T> Predicate<T> isNull() {
        return com.google.common.base.Predicates.isNull();
    }

    public static <T> Predicate<T> and(Iterable<? extends Predicate<? super T>> components) {
        return com.google.common.base.Predicates.and(components);
    }

    public static <T> Predicate<T> or(Predicate<? super T>... components) {
        return com.google.common.base.Predicates.or(components);
    }

    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return com.google.common.base.Predicates.not(predicate);
    }

    public static Predicate<CharSequence> containsPattern(String pattern) {
        return com.google.common.base.Predicates.containsPattern(pattern);
    }

    public static <T> Predicate<T> in(Collection<? extends T> target) {
        return com.google.common.base.Predicates.in(target);
    }

    public static Predicate<CharSequence> contains(Pattern pattern) {
        return com.google.common.base.Predicates.contains(pattern);
    }

    public static Predicate<Object> instanceOf(Class<?> clazz) {
        return com.google.common.base.Predicates.instanceOf(clazz);
    }

    public static <A, B> Predicate<A> compose(Predicate<B> predicate, Function<A, ? extends B> function) {
        return com.google.common.base.Predicates.compose(predicate, function);
    }

    public static <T> Predicate<T> and(Predicate<? super T>... components) {
        return com.google.common.base.Predicates.and(components);
    }

    public static <T> Predicate<T> notNull() {
        return com.google.common.base.Predicates.notNull();
    }

    public static <T> Predicate<T> or(Predicate<? super T> first, Predicate<? super T> second) {
        return com.google.common.base.Predicates.or(first, second);
    }

    public static <T> Predicate<T> equalTo(T target) {
        return com.google.common.base.Predicates.equalTo(target);
    }

    /**
     * Returns a predicate that evaluates to {@code true} if the object being
     * tested {@code ==} the given target or both are null.
     */
    public static <T> Predicate<T> is(T target) {
        return target == null ? Predicates.<T>isNull() : new IsPredicate<T>(target);
    }

    private static class IsPredicate<T> implements Predicate<T>, Serializable {
        private final T target;

        private IsPredicate(T target) {
            this.target = target;
        }

        public boolean apply(T t) {
            return target == t;
        }

        @Override
        public int hashCode() {
            return target.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof IsPredicate) {
                IsPredicate<?> that = (IsPredicate<?>) object;
                return target.equals(that.target);
            }
            return false;
        }

        @Override
        public String toString() {
            return "Is(" + target + ")";
        }

        private static final long serialVersionUID = 0;
    }
}
