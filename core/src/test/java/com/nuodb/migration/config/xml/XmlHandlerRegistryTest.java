package com.nuodb.migration.config.xml;

import com.nuodb.migration.config.xml.handler.XmlDumpHandler;
import com.nuodb.migration.config.xml.handler.XmlJdbcConnectionHandler;
import com.nuodb.migration.config.xml.handler.XmlMigrationHandler;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class XmlHandlerRegistryTest {

    private XmlHandlerRegistry xmlHandlerRegistry;

    @BeforeMethod
    public void setUp() throws Exception {
        xmlHandlerRegistry = new XmlHandlerRegistry();
    }

    @Test
    public void testRegister() throws Exception {
        xmlHandlerRegistry.registerHandler(mock(XmlJdbcConnectionHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlJdbcConnectionHandler.class), 1);
        xmlHandlerRegistry.registerHandler(mock(XmlJdbcConnectionHandler.class), -1);
        xmlHandlerRegistry.registerHandler(mock(XmlDumpHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlMigrationHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlHandlerRegistry.ConverterAdapter.class));
    }

    @Test
    public void testLookupReader() throws Exception {
        XmlReadHandler xmlReadHandler = xmlHandlerRegistry.lookupReader(
                mock(InputNode.class),
                XmlJdbcConnectionHandler.class,
                mock(XmlReadContext.class)
        );
        assertNull(xmlReadHandler);

        final XmlJdbcConnectionHandler mock = mock(XmlJdbcConnectionHandler.class);

        when(mock.canRead(any(InputNode.class),
                eq(XmlJdbcConnectionHandler.class),
                any(XmlReadContext.class))).thenReturn(true);

        xmlHandlerRegistry.registerHandler(mock);

        xmlReadHandler = xmlHandlerRegistry.lookupReader(
                mock(InputNode.class),
                XmlJdbcConnectionHandler.class,
                mock(XmlReadContext.class)
        );

        assertNotNull(xmlReadHandler);
        assertTrue(xmlReadHandler == mock);
        verify(xmlReadHandler).canRead(any(InputNode.class),
                eq(XmlJdbcConnectionHandler.class), any(XmlReadContext.class));
    }

    @Test
    public void testLookupWriter() throws Exception {
        final XmlJdbcConnectionHandler mock = mock(XmlJdbcConnectionHandler.class);

        XmlWriteHandler xmlWriteHandler = xmlHandlerRegistry.lookupWriter(
                mock(Object.class),
                XmlJdbcConnectionHandler.class,
                mock(OutputNode.class),
                mock(XmlWriteContext.class)
        );
        assertNull(xmlWriteHandler);

        when(mock.canWrite(anyObject(),
                eq(XmlJdbcConnectionHandler.class),
                any(OutputNode.class),
                any(XmlWriteContext.class))).thenReturn(true);
        xmlHandlerRegistry.registerHandler(mock);

        xmlWriteHandler = xmlHandlerRegistry.lookupWriter(
                mock(Object.class),
                XmlJdbcConnectionHandler.class,
                mock(OutputNode.class),
                mock(XmlWriteContext.class)
        );

        assertNotNull(xmlWriteHandler);
        assertTrue(xmlWriteHandler == mock);
        verify(xmlWriteHandler).canWrite(anyObject(),
                eq(XmlJdbcConnectionHandler.class),
                any(OutputNode.class), any(XmlWriteContext.class));
    }

    @Test(expectedExceptions = AssertionError.class)
    public void testRegisterWrongType() throws Exception {
        xmlHandlerRegistry.registerHandler(mock(XmlHandler.class));
    }
}
