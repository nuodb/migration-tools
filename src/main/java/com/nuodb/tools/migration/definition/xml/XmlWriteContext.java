package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.stream.OutputNode;

import java.util.Map;

public interface XmlWriteContext extends Map {

    boolean write(Object value, Class type, OutputNode node) throws XmlPersisterException;
}
