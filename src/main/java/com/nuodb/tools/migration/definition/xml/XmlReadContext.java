package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.stream.InputNode;

import java.util.Map;

public interface XmlReadContext extends Map {

    <T> T read(InputNode node, Class<T> type);
}
