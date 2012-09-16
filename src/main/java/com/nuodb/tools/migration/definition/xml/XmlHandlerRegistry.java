package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.convert.Converter;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

@SuppressWarnings("unchecked")
public class XmlHandlerRegistry {

    public static final int PRIORITY_VERY_HIGH = 100;
    public static final int PRIORITY_NORMAL = 0;
    public static final int PRIORITY_LOW = -1;
    public static final int PRIORITY_VERY_LOW = -100;

    private PriorityList<XmlReadHandler> readers = new PriorityList<XmlReadHandler>();
    private PriorityList<XmlWriteHandler> writers = new PriorityList<XmlWriteHandler>();

    public XmlHandlerRegistry register(Class type, Converter converter) {
        return register(new ConverterAdapter(type, converter));
    }

    public XmlHandlerRegistry register(XmlHandler handler) {
        return register(handler, PRIORITY_NORMAL);
    }

    public XmlHandlerRegistry register(XmlHandler handler, int priority) {
        if (handler instanceof XmlWriteHandler) {
            writers.add((XmlWriteHandler) handler, priority);
        }
        if (handler instanceof XmlReadHandler) {
            readers.add((XmlReadHandler) handler, priority);
        }
        return this;
    }

    public XmlWriteHandler lookupWriter(Object value, Class type, OutputNode output, XmlWriteContext context) {
        for (XmlWriteHandler writer : writers) {
            if (writer.canWrite(value, type, output, context)) {
                return writer;
            }
        }
        return null;
    }

    public XmlReadHandler lookupReader(InputNode input, Class type, XmlReadContext context) {
        for (XmlReadHandler reader : readers) {
            if (reader.canRead(input, type, context)) {
                return reader;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    class ConverterAdapter implements XmlReadWriteHandler {

        private Class type;
        private Converter converter;

        public ConverterAdapter(Class type, Converter converter) {
            this.type = type;
            this.converter = converter;
        }

        @Override
        public Object read(InputNode input, Class type, XmlReadContext context) {
            try {
                return converter.read(input);
            } catch (Exception e) {
                throw new XmlPersisterException("Underlying converter failed to read", e);
            }
        }

        @Override
        public boolean write(Object value, Class type, OutputNode output, XmlWriteContext context) {
            try {
                converter.write(output, value);
                return true;
            } catch (Exception e) {
                throw new XmlPersisterException("Underlying converter failed to write", e);
            }
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

}
