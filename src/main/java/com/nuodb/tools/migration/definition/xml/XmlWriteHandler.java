package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.stream.OutputNode;

public interface XmlWriteHandler<T> extends XmlHandler {

    boolean write(T value, Class<? extends T> type, OutputNode output, XmlWriteContext context);

    boolean canWrite(Object value, Class type, OutputNode output, XmlWriteContext context);
}
