package com.nuodb.tools.migration.jdbc.metamodel;

public class Name {

    public static final Name DEFAULT = new Name();

    private String name;

    protected Name() {
    }

    protected Name(String name) {
        this.name = name;
    }

    public String value() {
        return name;
    }

    private static boolean isEmpty(String name) {
        return name == null || name.length() == 0;
    }

    public static Name valueOf(String name) {
        return isEmpty(name) ? null : new Name(name);
    }

    @Override
    public String toString() {
        return value();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Name name1 = (Name) o;

        if (name != null ? !name.equals(name1.name) : name1.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
