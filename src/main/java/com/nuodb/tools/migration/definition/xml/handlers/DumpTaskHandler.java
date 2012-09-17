package com.nuodb.tools.migration.definition.xml.handlers;

import com.nuodb.tools.migration.definition.DumpTask;
import com.nuodb.tools.migration.definition.xml.XmlReadContext;
import com.nuodb.tools.migration.definition.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

public class DumpTaskHandler extends DefinitionHandler<DumpTask> {

    public DumpTaskHandler() {
        super(DumpTask.class);
    }

    @Override
    protected boolean write(DumpTask task, OutputNode output, XmlWriteContext context) throws Exception {
        super.write(task, output, context);
        // TODO: implement
        return true;
    }

    @Override
    protected void read(InputNode input, DumpTask task, XmlReadContext context) throws Exception {
        super.read(input, task, context);
        // TODO: implement
    }
}
