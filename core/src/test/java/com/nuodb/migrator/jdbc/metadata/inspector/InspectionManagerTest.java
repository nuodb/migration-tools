package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertNotNull;

@SuppressWarnings("unchecked")
public class InspectionManagerTest {

    private InspectionManager inspectionManager;

    /**
     * Returns inspection context, which creates inspection context doing
     * nothing during commit() call.
     */
    @BeforeMethod
    public void setUp() throws Exception {
        inspectionManager = spy(new InspectionManager());

        given(inspectionManager.createInspectionContext(any(Connection.class), any(InspectionResults.class),
                (MetaDataType[]) anyVararg())).will(new Answer<InspectionContext>() {
                    @Override
                    public InspectionContext answer(InvocationOnMock invocation) throws Throwable {
                        InspectionContext inspectionContext = (InspectionContext) spy(invocation.callRealMethod());
                        willDoNothing().given(inspectionContext).close();
                        return inspectionContext;
                    }
                });
    }

    @DataProvider(name = "inspect")
    public Object[][] createInspectData() {
        MetaDataType[] objectTypes = MetaDataType.TYPES;
        Object[][] inspectData = new Object[objectTypes.length][];
        int index = 0;
        for (MetaDataType objectType : objectTypes) {
            inspectData[index++] = new Object[] { objectType };
        }
        return inspectData;
    }

    /**
     * Verifies that correspondent inspector is called by the inspection
     * manager.
     *
     * @param objectType
     *            to be inspected
     * @throws Exception
     */
    @Test(dataProvider = "inspect")
    public void testInspect(MetaDataType objectType) throws Exception {
        Inspector inspector = mock(Inspector.class);
        when(inspector.supports(objectType)).thenReturn(true);
        when(inspector.supportsScope(any(InspectionContext.class), any(InspectionScope.class))).thenReturn(true);

        inspectionManager.addInspector(inspector);

        assertNotNull(inspectionManager.inspect(mock(Connection.class), objectType));
        verify(inspector).inspectScope(any(InspectionContext.class), any(InspectionScope.class));
    }
}
