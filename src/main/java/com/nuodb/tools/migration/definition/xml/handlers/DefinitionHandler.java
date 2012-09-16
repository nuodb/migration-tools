package com.nuodb.tools.migration.definition.xml.handlers;

import com.nuodb.tools.migration.definition.Definition;
import com.nuodb.tools.migration.definition.xml.XmlConstants;
import com.nuodb.tools.migration.definition.xml.XmlReadWriteAdapter;
import com.nuodb.tools.migration.definition.xml.XmlReadContext;
import com.nuodb.tools.migration.definition.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.tools.migration.definition.xml.handlers.AttributesAccessor.*;

public class DefinitionHandler<T extends Definition> extends XmlReadWriteAdapter<T> implements XmlConstants {

    protected DefinitionHandler(Class type) {
        super(type);
    }

    @Override
    protected void read(InputNode input, Definition definition, XmlReadContext context) throws Exception {
        definition.setId(get(input, ID_ATTRIBUTE));
        definition.setType(get(input, TYPE_ATTRIBUTE));
    }

    @Override
    protected boolean write(Definition definition, OutputNode output, XmlWriteContext context) throws Exception {
        output.getNamespaces().setReference(MIGRATION_NAMESPACE);
        set(output, ID_ATTRIBUTE, definition.getId());
        set(output, TYPE_ATTRIBUTE, definition.getType() == null ? definition.getClass().getName() : definition.getType());
        return true;
    }
}
