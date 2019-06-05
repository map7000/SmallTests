package ru.mfilatov.SmallTests.dpl;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.util.Objects;

/**
 * Ключ кэша глобального индекса
 */
public final class GlobalIndexKey {
    @AffinityKeyMapped
    @QuerySqlField(index = true)
    private final String value;
    @QuerySqlField
    private final Object payload;

    public GlobalIndexKey(String value, Object payload) {
        this.value = value;
        this.payload = payload;
    }

    public String getValue() {
        return value;
    }

    public Object getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GlobalIndexKey that = (GlobalIndexKey) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, payload);
    }

    @Override
    public String toString() {
        return "GlobalIndexKey{" +
                "value='" + value + '\'' +
                ", payload=" + payload +
                '}';
    }
}
