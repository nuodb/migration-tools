package com.nuodb.tools.migration.definition.xml;

import com.nuodb.tools.migration.MigrationException;

public class XmlPersisterException extends MigrationException {

    public XmlPersisterException(String message) {
        super(message);
    }

    public XmlPersisterException(String message, Throwable cause) {
        super(message, cause);
    }

    public XmlPersisterException(Throwable cause) {
        super(cause);
    }
}
