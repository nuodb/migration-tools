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
package com.nuodb.migration.jdbc.model;

import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class ColumnModelSetImpl<T extends ColumnModel> implements ColumnModelSet<T> {

    private List<T> columns = newArrayList();

    public ColumnModelSetImpl(T... columns) {
        this.columns.addAll(newArrayList(columns));
    }

    public ColumnModelSetImpl(Iterable<T> columns) {
        this.columns.addAll(newArrayList(columns));
    }

    @Override
    public T get(int index) {
        return columns.get(index);
    }

    @Override
    public T get(String name) {
        for (T column : columns) {
            if (StringUtils.equals(column.getName(), name)) {
                return column;
            }
        }
        return null;
    }

    @Override
    public T set(int index, T element) {
        return columns.set(index, element);
    }

    @Override
    public int size() {
        return columns.size();
    }

    @Override
    public boolean isEmpty() {
        return columns.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return columns.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return columns.iterator();
    }

    public boolean add(T t) {
        return columns.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return columns.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return columns.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return columns.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return columns.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return columns.retainAll(c);
    }

    @Override
    public void clear() {
        columns.clear();
    }

    @Override
    public Object[] toArray() {
        return columns.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return columns.toArray(a);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnModelSetImpl that = (ColumnModelSetImpl) o;
        if (columns.equals(that.columns)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public String toString() {
        return columns.toString();
    }
}
