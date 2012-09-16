package com.nuodb.tools.migration.utils;

import com.nuodb.tools.migration.MigrationException;

public class AssertionException extends MigrationException {

    public AssertionException(String message) {
        super(message);
    }

    public AssertionException(String message, Throwable cause) {
        super(message, cause);
    }

    public AssertionException(Throwable cause) {
        super(cause);
    }
}
