package com.nuodb.tools.migration.definition.xml;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

public class PriorityList<T> implements Iterable<T>, Serializable {

    private AtomicInteger id = new AtomicInteger();
    private Set<Item<T>> items = new TreeSet<Item<T>>();

    public void add(T item, int priority) {
        this.items.add(new Item<T>(item, priority, id.incrementAndGet()));
    }

    @Override
    public Iterator<T> iterator() {
        return new ItemIterator<T>(items.iterator());
    }

    static class ItemIterator<T> implements Iterator<T> {

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

    static class Item<T> implements Comparable<Item<T>>, Serializable {

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
            if (other == this) return true;
            if (!(other instanceof Item<?>)) return false;
            return this.id == ((Item<?>) other).id;
        }
    }
}


