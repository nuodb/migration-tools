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
package com.nuodb.migrator.jdbc.model;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

/**
 * @author Sergey Bushik
 */
public class SimpleValueModelList<T extends ValueModel> implements ValueModelList<T> {

    private List<T> values = newArrayList();

    public SimpleValueModelList() {
    }

    public SimpleValueModelList(T... values) {
        this.values.addAll(newArrayList(values));
    }

    public SimpleValueModelList(Iterable<T> values) {
        this.values.addAll(newArrayList(values));
    }

    @Override
    public T get(int index) {
        return values.get(index);
    }

    @Override
    public T get(String name) {
        for (T column : values) {
            if (equalsIgnoreCase(column.getName(), name)) {
                return column;
            }
        }
        return null;
    }

    @Override
    public T set(int index, T element) {
        return values.set(index, element);
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return values.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return values.iterator();
    }

    public boolean add(T t) {
        return values.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return values.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return values.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return values.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return values.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return values.retainAll(c);
    }

    @Override
    public void clear() {
        values.clear();
    }

    @Override
    public Object[] toArray() {
        return values.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return values.toArray(a);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleValueModelList that = (SimpleValueModelList) o;
        if (values.equals(that.values)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public String toString() {
        return values.toString();
    }
}
