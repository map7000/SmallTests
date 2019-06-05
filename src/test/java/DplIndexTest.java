import java.util.Arrays;

import org.apache.commons.lang3.RandomUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mfilatov.SmallTests.dpl.GlobalIndexKey;
import ru.mfilatov.SmallTests.dpl.GlobalIndexValue;
import ru.mfilatov.SmallTests.dpl.NewGlobalIndex;

public class DplIndexTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DplIndexTest.class);

    @Test
    void DplSelect() {
        final int start = 0;
        final int end = 10;
        Ignite ignite = Ignition.start(getIgniteConfiguration());
        if (!ignite.cluster().active()) {
            ignite.cluster().active(true);
        }
        IgniteCache cache = ignite.getOrCreateCache(getCacheConfiguration());
        ignite.cacheNames().stream().forEach(LOGGER::debug);
        NewGlobalIndex newGlobalIndex = new NewGlobalIndex(ignite, cache.getName());
        for(long i = 0; i < 100; i++) {
            GlobalIndexKey key1 = new GlobalIndexKey(String.valueOf(RandomUtils.nextLong(start,end)), RandomUtils.nextLong(0,100));
            newGlobalIndex.insert(key1);
        }
        for (int i = start; i < end; i++) {
            LOGGER.debug("Key {}: {}", i, newGlobalIndex.get(String.valueOf(i)).toString());
        }
        cache.destroy();
        ignite.cluster().active(false);
        ignite.close();
    }

    public static IgniteConfiguration getIgniteConfiguration(){
        TcpCommunicationSpi tcpSpi = new TcpCommunicationSpi();
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration()
            .setBinaryConfiguration(new BinaryConfiguration()
                .setCompactFooter(true))
            .setCommunicationSpi(tcpSpi)
            .setIgniteInstanceName("TestNode_"+ System.currentTimeMillis())
            .setClientMode(false);
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setLocalPort(47500);
        tcpSpi.setLocalPort(47100);
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
        finder.setAddresses(Arrays.asList("localhost:47500"));
        spi.setIpFinder(finder);
        igniteConfiguration.setDiscoverySpi(spi);
        igniteConfiguration.setPeerClassLoadingEnabled(true);
        return igniteConfiguration;
    }
    public static CacheConfiguration getCacheConfiguration(){
        CacheConfiguration cacheConfiguration = new CacheConfiguration<>()
            .setBackups(3)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setName("jepsen")
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setReadFromBackup(true)
            .setIndexedTypes(GlobalIndexKey.class, GlobalIndexValue.class);
        RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
        affinityFunction.setExcludeNeighbors(true);
        affinityFunction.setPartitions(4);
        cacheConfiguration.setAffinity(affinityFunction);
        return cacheConfiguration;
    }
}
