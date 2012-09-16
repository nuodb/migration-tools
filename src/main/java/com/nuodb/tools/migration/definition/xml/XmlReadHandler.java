package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.stream.InputNode;

public interface XmlReadHandler<T> extends XmlHandler {

    T read(InputNode input, Class<? extends T> type, XmlReadContext context);

    boolean canRead(InputNode input, Class type, XmlReadContext context);
}
