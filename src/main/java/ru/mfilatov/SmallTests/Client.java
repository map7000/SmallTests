package ru.mfilatov.SmallTests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.cache.Cache;
import javax.cache.Cache.Entry;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    IgniteCache<Integer, String> cache;
    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public Client(String igniteConfig) {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(igniteConfig);
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        System.setProperty("IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD", "20000");
        cache = ignite.getOrCreateCache("myCache");
    }

    public int get(int k) {
        System.out.println("GET - k: " + k);
        String cacheValue = cache.get(k);
        if (cacheValue != null) {
            return Integer.parseInt(cacheValue);
        } else {
            return 0;
        }
    }

    public void set(int k, int v) {
        System.out.println("SET - k: " + k + " v: " + v);
        cache.put(k, (String.valueOf(v)));
    }

    public void remove(int k) {
        System.out.println("REMOVE - k: " + k);
        cache.remove(k);
    }

    public static Set<Long> getValuesJava(IgniteCache<Long, Long> cache){
        Set<Long> result = (Set) cache.getAll(
            LongStream.rangeClosed(0,5).collect(HashSet::new, HashSet::add, HashSet::addAll)).values();
        return result;
//        return cache.getAll(LongStream.rangeClosed(0,5).collect(HashSet::new, HashSet::add, HashSet::addAll)).values().stream().collect(Collectors.toSet());
    }
    public static Set<Long> getValuesScan(IgniteCache<Long, Long> cache){
        Set<Long> a = new HashSet<>();
        cache.query(new ScanQuery<Long, Long>(null));
        QueryCursor<Entry<Long, Long>> q = cache.query(new ScanQuery(null));
        Iterator<Entry<Long, Long>> i = q.iterator();
        while (i.hasNext()){
            a.add(i.next().getValue());
        }
        return a;
    }

    public static Map<Long, Long> getValuesScan2(IgniteCache<Long, Long> cache){
        cache.query(new ScanQuery<Long, Long>(null));
        QueryCursor<Cache.Entry<Long, Long>> q = cache.query(new ScanQuery(null));
        Map<Long, Long> a = q.getAll().stream().collect(Collectors.toMap(i -> i.getKey(), i -> i.getValue()));
        return a;
    }

    public static List<Long> getValuesSQL(IgniteCache<Long, Long> cache){
        FieldsQueryCursor q = cache.query(new SqlFieldsQuery("SHOW TABLES"));
        List<Long> result = (List<Long>) q.iterator().next();
        return result;
    }

    public static void casInvoke(IgniteCache<Long, Long> cache, Long key, Long oldValue, Long newValue){
        cache.invoke(key, (entry, object) -> {
            if(entry.getValue() == oldValue) entry.setValue(newValue);
            return null;
        });
    }
    public static void casReplace(IgniteCache<Long, Long> cache, Long key, Long oldValue, Long newValue){
        boolean b =  cache.replace(key, oldValue, newValue);
        System.out.println(b);
    }
    public static void seq(Ignite ignite){
        IgniteAtomicSequence igniteAtomicSequence = ignite.atomicSequence("seq", 0, true);
        for(int i = 0; i < 100; i++){
            igniteAtomicSequence.get();
            System.out.println(igniteAtomicSequence.getAndIncrement());
        }
    }
    public static void jdbc(){
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
            Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
            try(Statement statement = conn.createStatement()){
                statement.execute("CREATE TABLE JEPSEN (ID LONG PRIMARY KEY, VALUE LONG)");
                statement.execute("INSERT INTO JEPSEN (ID, VALUE) VALUES (0L, 0L)");
                statement.execute("INSERT INTO JEPSEN (ID, VALUE) VALUES (1L, 1L)");
                statement.execute("INSERT INTO JEPSEN (ID, VALUE) VALUES (2L, 2L)");
                statement.execute("INSERT INTO JEPSEN (ID, VALUE) VALUES (3L, 3L)");
                statement.execute("INSERT INTO JEPSEN (ID, VALUE) VALUES (4L, 4L)");
                statement.execute("INSERT INTO JEPSEN (ID, VALUE) VALUES (5L, 5L)");
                ResultSet resultSet = statement.executeQuery("SELECT * FROM JEPSEN");
                while (resultSet.next()){
                    System.out.println(resultSet.getLong(2));
                }
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
    public static void createCacheJavaAPI(Ignite ignite, IgniteCache cache){
        try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)){
            tx.timeout(2_000);
            cache.put(0L,1L);
            cache.put(1L,2L);
            cache.put(2L,3L);
            cache.put(3L,4L);
            cache.put(4L,5L);
            cache.put(5L,6L);
            tx.commit();
        }
        catch (CacheException e){
            if(e.getCause() instanceof TransactionTimeoutException &&
                e.getCause().getCause() instanceof TransactionDeadlockException)
                System.out.println(e.getCause().getCause().getMessage());
        }
    }
}
