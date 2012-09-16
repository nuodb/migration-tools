package com.nuodb.tools.migration.definition.xml.handlers;

import com.nuodb.tools.migration.definition.xml.XmlReadWriteHandler;
import com.nuodb.tools.migration.definition.xml.XmlPersisterException;
import com.nuodb.tools.migration.definition.xml.XmlReadContext;
import com.nuodb.tools.migration.definition.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;
import org.simpleframework.xml.transform.RegistryMatcher;
import org.simpleframework.xml.transform.Transformer;

public class TransformerHandler implements XmlReadWriteHandler {

    private Transformer transformer;

    public TransformerHandler() {
        this(new Transformer(new RegistryMatcher()));
    }

    public TransformerHandler(Transformer transformer) {
        this.transformer = transformer;
    }

    @Override
    public Object read(InputNode input, Class type, XmlReadContext context) {
        try {
            return transformer.read(input.getValue(), type);
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return canConvert(type);
    }

    @Override
    public boolean write(Object value, Class type, OutputNode output, XmlWriteContext context) {
        try {
            output.setValue(transformer.write(value, type));
            return true;
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }

    @Override
    public boolean canWrite(Object value, Class type, OutputNode output, XmlWriteContext context) {
        return canConvert(type);
    }

    protected boolean canConvert(Class type) {
        try {
            return transformer.valid(type);
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }
}
