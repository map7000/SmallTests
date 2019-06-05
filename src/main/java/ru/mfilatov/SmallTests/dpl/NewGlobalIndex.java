package ru.mfilatov.SmallTests.dpl;

import java.util.Set;
import org.apache.ignite.Ignite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Глобальный индекс нового поколения.
 */
public final class NewGlobalIndex{
    private static final Logger LOGGER = LoggerFactory.getLogger(NewGlobalIndex.class);

    private final GlobalIndexStore globalIndexStore;

    public NewGlobalIndex(final Ignite ignite, String cacheName) {
        this.globalIndexStore = new GlobalIndexStore(ignite, cacheName);
    }

    public void insert(GlobalIndexKey key) {
        insertToIndex(key);
    }

    public Set<Object> get(final String key) {
        return globalIndexStore.get(key);
    }

    public void update(final GlobalIndexKey oldKey, final GlobalIndexKey newKey) {
        removeFromIndex(oldKey);
        insertToIndex(newKey);
    }

    private void insertToIndex(final GlobalIndexKey globalIndexKey) {
        globalIndexStore.insert(globalIndexKey);
    }

    private void removeFromIndex(final GlobalIndexKey globalIndexKey) {
        globalIndexStore.remove(globalIndexKey);
    }
}
