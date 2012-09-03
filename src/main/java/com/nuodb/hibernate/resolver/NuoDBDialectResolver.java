package com.nuodb.hibernate.resolver;

import com.nuodb.hibernate.dialect.NuoDBDialect;
import org.hibernate.service.jdbc.dialect.internal.BasicDialectResolver;

public class NuoDBDialectResolver extends BasicDialectResolver {

    public static final String DATABASE_NAME = "NuoDB";

    public NuoDBDialectResolver() {
        super(DATABASE_NAME, NuoDBDialect.class);
    }
}
