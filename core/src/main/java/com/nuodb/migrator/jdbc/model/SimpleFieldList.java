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
package com.nuodb.migrator.jdbc.model;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

/**
 * @author Sergey Bushik
 */
public class SimpleFieldList<T extends Field> implements FieldList<T> {

    private List<T> items = newArrayList();

    public SimpleFieldList() {
    }

    public SimpleFieldList(T... items) {
        this.items.addAll(newArrayList(items));
    }

    public SimpleFieldList(Iterable<T> items) {
        this.items.addAll(newArrayList(items));
    }

    @Override
    public T get(int index) {
        return items.get(index);
    }

    @Override
    public T get(String name) {
        for (T item : items) {
            if (equalsIgnoreCase(item.getName(), name)) {
                return item;
            }
        }
        return null;
    }

    @Override
    public T set(int index, T element) {
        return items.set(index, element);
    }

    @Override
    public int size() {
        return items.size();
    }

    @Override
    public boolean isEmpty() {
        return items.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return items.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return items.iterator();
    }

    public boolean add(T t) {
        return items.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return items.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return items.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return items.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return items.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return items.retainAll(c);
    }

    @Override
    public void clear() {
        items.clear();
    }

    @Override
    public Object[] toArray() {
        return items.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return items.toArray(a);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimpleFieldList that = (SimpleFieldList) o;
        if (items.equals(that.items))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return items.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public String toString() {
        return items.toString();
    }
}
