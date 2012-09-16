package com.nuodb.tools.migration.definition.xml;

import com.nuodb.tools.migration.utils.Assertions;
import com.nuodb.tools.migration.utils.ClassUtils;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

@SuppressWarnings("unchecked")
public class XmlReadWriteAdapter<T> implements XmlReadWriteHandler<T> {

    private Class type;

    public XmlReadWriteAdapter(Class type) {
        Assertions.assertNotNull(type, "Type is required");
        this.type = type;
    }

    @Override
    public T read(InputNode input, Class<? extends T> type, XmlReadContext context) {
        T target = createObject(input, type, context);
        try {
            read(input, target, context);
        } catch (XmlPersisterException e) {
            throw e;
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
        return target;
    }

    protected T createObject(InputNode input, Class<? extends T> type, XmlReadContext context) {
        return ClassUtils.newInstance(type);
    }

    protected void read(InputNode input, T value, XmlReadContext context) throws Exception {
        throw new XmlPersisterException("Read method is not implemented");
    }

    @Override
    public boolean write(T value, Class<? extends T> type, OutputNode output, XmlWriteContext context) {
        try {
            return write(value, output, context);
        } catch (XmlPersisterException e) {
            throw e;
        } catch (Exception e) {
            throw new XmlPersisterException(e);
        }
    }

    protected boolean write(T value, OutputNode output, XmlWriteContext context) throws Exception {
        throw new XmlPersisterException("Write method is not implemented");
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return this.type.equals(type);
    }

    @Override
    public boolean canWrite(Object value, Class type, OutputNode output, XmlWriteContext context) {
        return this.type.equals(type);
    }
}
