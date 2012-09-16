package com.nuodb.tools.migration.definition.xml.handlers;

import com.nuodb.tools.migration.definition.Typeable;
import com.nuodb.tools.migration.definition.xml.XmlConstants;
import com.nuodb.tools.migration.definition.xml.XmlReadContext;
import com.nuodb.tools.migration.definition.xml.XmlReadHandler;
import org.simpleframework.xml.stream.InputNode;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class TypeableResolver implements XmlReadHandler<Typeable>, XmlConstants {

    private Map<TypeableAlias, Class<? extends Typeable>> typeAliasesClasses = new HashMap<TypeableAlias, Class<? extends Typeable>>();

    public void bind(String namespace, String element, String alias, Class<? extends Typeable> type) {
        typeAliasesClasses.put(new TypeableAlias(namespace, element, alias), type);
        typeAliasesClasses.put(new TypeableAlias(namespace, element, type.getName()), type);
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return resolve(
                input.getReference(), input.getName(), AttributesAccessor.get(input, TYPE_ATTRIBUTE)) != null;
    }

    @Override
    public Typeable read(InputNode input, Class<? extends Typeable> type, XmlReadContext context) {
        return context.read(input, resolve(
                input.getReference(), input.getName(), AttributesAccessor.get(input, TYPE_ATTRIBUTE)));
    }

    protected Class<? extends Typeable> resolve(String namespace, String element, String alias) {
        return typeAliasesClasses.get(new TypeableAlias(namespace, element, alias));
    }

    class TypeableAlias {
        private String namespace;
        private String element;
        private String alias;

        public TypeableAlias(String namespace, String element, String alias) {
            this.namespace = namespace;
            this.element = element;
            this.alias = alias;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TypeableAlias that = (TypeableAlias) o;

            if (alias != null ? !alias.equals(that.alias) : that.alias != null)
                return false;
            if (element != null ? !element.equals(that.element) : that.element != null) return false;
            if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = namespace != null ? namespace.hashCode() : 0;
            result = 31 * result + (element != null ? element.hashCode() : 0);
            result = 31 * result + (alias != null ? alias.hashCode() : 0);
            return result;
        }
    }
}
