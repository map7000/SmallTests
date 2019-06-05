package ru.mfilatov.SmallTests.dpl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Хранилище данных глобальных индексов.
 */
public class GlobalIndexStore {
    private static final String QUERY = "SELECT payload FROM \"%s\".GlobalIndexValue WHERE value IS ?";

    private final IgniteCache<GlobalIndexKey, GlobalIndexValue> cache;
    private final Affinity<GlobalIndexKey> affinity;
    private final String globalIndexQuery;

    public GlobalIndexStore(Ignite ignite, final String cacheName) {
        this.cache = ignite.cache(cacheName);
        this.affinity = ignite.affinity(cacheName);
        this.globalIndexQuery = String.format(QUERY, cacheName);
    }

    public void insert(final GlobalIndexKey indexKey) {
        cache.put(indexKey, GlobalIndexValue.VALUE);
    }

    public void remove(final GlobalIndexKey globalIndexKey) {
        cache.remove(globalIndexKey);
    }

    public Set<Object> get(final String key) {
        final GlobalIndexKey indexKey = new GlobalIndexKey(key, null);
        final int partition = affinity.partition(indexKey);

        final SqlFieldsQuery fieldsQuery = new SqlFieldsQuery(globalIndexQuery);
        fieldsQuery.setArgs(key == null ? "GLOBAL_INDEX_NULL_VALUE" : key);
        fieldsQuery.setPartitions(partition);

        final Set<Object> result = new HashSet<>();
        try(FieldsQueryCursor<List<?>> cursor = cache.query(fieldsQuery)) {
            for (List<?> values : cursor) {
                result.add(values.get(0));
            }
        }

        return result;
    }
}
