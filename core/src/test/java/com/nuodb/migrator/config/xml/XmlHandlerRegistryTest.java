package com.nuodb.migrator.config.xml;

import com.nuodb.migrator.config.xml.handler.XmlDriverConnectionSpecHandler;
import com.nuodb.migrator.config.xml.handler.XmlDumpHandler;
import com.nuodb.migrator.config.xml.handler.XmlMigratorHandler;
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
        xmlHandlerRegistry.registerHandler(mock(XmlDriverConnectionSpecHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlDriverConnectionSpecHandler.class), 1);
        xmlHandlerRegistry.registerHandler(mock(XmlDriverConnectionSpecHandler.class), -1);
        xmlHandlerRegistry.registerHandler(mock(XmlDumpHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlMigratorHandler.class));
        xmlHandlerRegistry.registerHandler(mock(XmlHandlerRegistry.ConverterAdapter.class));
    }

    @Test
    public void testLookupReader() throws Exception {
        XmlReadHandler xmlReadHandler = xmlHandlerRegistry.lookupReader(
                mock(InputNode.class),
                XmlDriverConnectionSpecHandler.class,
                mock(XmlReadContext.class)
        );
        assertNull(xmlReadHandler);

        final XmlDriverConnectionSpecHandler mock = mock(XmlDriverConnectionSpecHandler.class);

        when(mock.canRead(any(InputNode.class),
                eq(XmlDriverConnectionSpecHandler.class),
                any(XmlReadContext.class))).thenReturn(true);

        xmlHandlerRegistry.registerHandler(mock);

        xmlReadHandler = xmlHandlerRegistry.lookupReader(
                mock(InputNode.class),
                XmlDriverConnectionSpecHandler.class,
                mock(XmlReadContext.class)
        );

        assertNotNull(xmlReadHandler);
        assertTrue(xmlReadHandler == mock);
        verify(xmlReadHandler).canRead(any(InputNode.class),
                eq(XmlDriverConnectionSpecHandler.class), any(XmlReadContext.class));
    }

    @Test
    public void testLookupWriter() throws Exception {
        final XmlDriverConnectionSpecHandler mock = mock(XmlDriverConnectionSpecHandler.class);

        XmlWriteHandler xmlWriteHandler = xmlHandlerRegistry.lookupWriter(
                mock(Object.class),
                XmlDriverConnectionSpecHandler.class,
                mock(OutputNode.class),
                mock(XmlWriteContext.class)
        );
        assertNull(xmlWriteHandler);

        when(mock.canWrite(anyObject(),
                eq(XmlDriverConnectionSpecHandler.class),
                any(OutputNode.class),
                any(XmlWriteContext.class))).thenReturn(true);
        xmlHandlerRegistry.registerHandler(mock);

        xmlWriteHandler = xmlHandlerRegistry.lookupWriter(
                mock(Object.class),
                XmlDriverConnectionSpecHandler.class,
                mock(OutputNode.class),
                mock(XmlWriteContext.class)
        );

        assertNotNull(xmlWriteHandler);
        assertTrue(xmlWriteHandler == mock);
        verify(xmlWriteHandler).canWrite(anyObject(),
                eq(XmlDriverConnectionSpecHandler.class),
                any(OutputNode.class), any(XmlWriteContext.class));
    }

    @Test(expectedExceptions = AssertionError.class)
    public void testRegisterWrongType() throws Exception {
        xmlHandlerRegistry.registerHandler(mock(XmlHandler.class));
    }
}
