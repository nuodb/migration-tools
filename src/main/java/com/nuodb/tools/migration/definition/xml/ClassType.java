package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.strategy.Type;

import java.lang.annotation.Annotation;

public class ClassType implements Type {

    private final Class type;

    public ClassType(Class type) {
        this.type = type;
    }

    public Class getType() {
        return type;
    }

    public <T extends Annotation> T getAnnotation(Class<T> type) {
        return null;
    }

    public String toString() {
        return type.toString();
    }
}