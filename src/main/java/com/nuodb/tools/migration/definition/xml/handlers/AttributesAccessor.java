package com.nuodb.tools.migration.definition.xml.handlers;

import com.nuodb.tools.migration.definition.xml.XmlPersisterException;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

public class AttributesAccessor {

    public static final String EMPTY = "";

    public static String get(InputNode input, String name) {
        InputNode attribute = input.getAttribute(name);
        String value = null;
        if (attribute != null) {
            try {
                value = attribute.getValue();
            } catch (Exception e) {
                throw new XmlPersisterException(String.format("Can't access value of %1$s attribute", name), e);
            }
        }
        return value;
    }

    public static void set(OutputNode output, String name, String value) {
        output.setAttribute(name, value == null ? EMPTY : value);
    }
}
