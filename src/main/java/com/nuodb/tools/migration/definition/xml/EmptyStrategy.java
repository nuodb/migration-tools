package com.nuodb.tools.migration.definition.xml;

import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.strategy.Type;
import org.simpleframework.xml.strategy.Value;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.NodeMap;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Map;

public class EmptyStrategy implements Strategy {
    @Override
    public Value read(Type type, NodeMap<InputNode> node, Map map) throws Exception {
        return null;
    }

    @Override
    public boolean write(Type type, Object value, NodeMap<OutputNode> node, Map map) throws Exception {
        return false;
    }
}
