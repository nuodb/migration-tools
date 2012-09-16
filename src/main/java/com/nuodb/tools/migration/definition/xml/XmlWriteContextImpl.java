package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class XmlWriteContextImpl implements XmlWriteContext {

    private Strategy strategy;
    private Map map;

    public XmlWriteContextImpl(Strategy strategy, Map map) {
        this.strategy = strategy;
        this.map = map;
    }

    @Override
    public boolean write(Object value, Class type, OutputNode node) throws XmlPersisterException {
        try {
            return strategy.write(new ClassType(type), value, node.getAttributes(), this);
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return map.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(Map m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set keySet() {
        return map.keySet();
    }

    @Override
    public Collection values() {
        return map.values();
    }

    @Override
    public Set entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        XmlWriteContextImpl that = (XmlWriteContextImpl) o;

        if (map != null ? !map.equals(that.map) : that.map != null) return false;
        if (strategy != null ? !strategy.equals(that.strategy) : that.strategy != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = strategy != null ? strategy.hashCode() : 0;
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }
}
