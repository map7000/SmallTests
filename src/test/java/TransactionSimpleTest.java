import java.util.Arrays;
import java.util.List;
import java.util.Random;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mfilatov.SmallTests.data.Transaction;
import org.apache.commons.lang3.RandomUtils;

public class TransactionSimpleTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionSimpleTest.class);

    @Test
    void BankSqlSelect() {
        Ignite ignite = Ignition.start(getIgniteConfiguration());
        if (!ignite.cluster().active()) {
            ignite.cluster().active(true);
        }
        IgniteCache cache = ignite.getOrCreateCache(getCacheConfiguration());
        ignite.cacheNames().stream().forEach(LOGGER::debug);
        for(long i = 0; i < 100; i++){
            cache.put(Long.valueOf(i), new Transaction.Builder().clientId(RandomUtils.nextLong(0,10)).transactionId(i).value(RandomUtils.nextLong(0,100)).build());
        }
        FieldsQueryCursor fieldsQueryCursor = cache.query(new SqlFieldsQuery("SELECT count(clientId) FROM TRANSACTION WHERE clientId = 2"));
        List list = fieldsQueryCursor.getAll();
        LOGGER.debug(list.toString());
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
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC)
            .setReadFromBackup(false)
            .setIndexedTypes(Long.class, Transaction.class);
        RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
        affinityFunction.setExcludeNeighbors(true);
        affinityFunction.setPartitions(4);
        cacheConfiguration.setAffinity(affinityFunction);
        return cacheConfiguration;
    }
}
