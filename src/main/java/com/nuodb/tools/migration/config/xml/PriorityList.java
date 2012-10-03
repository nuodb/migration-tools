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
package com.nuodb.tools.migration.config.xml;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

class PriorityList<T> implements Iterable<T>, Serializable {

    private AtomicInteger id = new AtomicInteger();
    private Set<Item<T>> items = new TreeSet<Item<T>>();

    public void add(T item, int priority) {
        items.add(new Item<T>(item, priority, id.incrementAndGet()));
    }

    @Override
    public Iterator<T> iterator() {
        return new ItemIterator<T>(items.iterator());
    }

    class ItemIterator<T> implements Iterator<T> {

        private Iterator<Item<T>> iterator;

        public ItemIterator(Iterator<Item<T>> iterator) {
            this.iterator = iterator;
        }

        public void remove() {
            iterator.remove();
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public T next() {
            return iterator.next().value;
        }
    }

    @SuppressWarnings("unchecked")
    class Item<T> implements Comparable<Item<T>>, Serializable {

        private T value;
        private int priority;
        private int id;

        public Item(T value, int priority, int id) {
            this.value = value;
            this.priority = priority;
            this.id = id;
        }

        public int compareTo(Item<T> other) {
            return this.priority != other.priority ?
                    other.priority - this.priority : other.id - this.id;
        }

        public boolean equals(Object other) {
            return other == this || other instanceof Item && this.id == ((Item) other).id;
        }
    }
}


