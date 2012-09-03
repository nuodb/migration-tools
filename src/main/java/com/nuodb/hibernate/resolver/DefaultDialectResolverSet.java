package com.nuodb.hibernate.resolver;

import org.hibernate.service.jdbc.dialect.internal.DialectResolverSet;
import org.hibernate.service.jdbc.dialect.internal.StandardDialectResolver;
import org.hibernate.service.jdbc.dialect.spi.DialectResolver;

public class DefaultDialectResolverSet extends DialectResolverSet {

    public static final DialectResolver INSTANCE = new DefaultDialectResolverSet();

    public DefaultDialectResolverSet() {
        super(new NuoDBDialectResolver(), new StandardDialectResolver());
    }
}
