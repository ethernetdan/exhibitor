package com.netflix.exhibitor.core.config.etcd;

import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.PseudoLock;
import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestEtcdPseudoLock {
    public EtcdClient client;

    @BeforeClass
    public void startEtcd() throws Exception {
        URI localEtcd = URI.create("http://127.0.0.1:4001");
        client = new EtcdClient(localEtcd);
    }

    @BeforeMethod
    public void setUp() throws IOException, TimeoutException, EtcdException {
        EtcdKeysResponse response = client.getAll().send().get();
        if (response.node.nodes != null) {
            for (EtcdKeysResponse.EtcdNode node : response.node.nodes) {
                if (node.dir) {
                    client.deleteDir(node.key).recursive().send();
                } else {
                    client.delete(node.key).send();
                }
            }
        }
    }

    @Test
    public void testBasicLocking() throws Exception {
        Mockito.mock(ActivityLog.class);

        EtcdConfigProvider config1 = new EtcdConfigProvider(client, "test4", new Properties(), "host1");
        EtcdConfigProvider config2 = new EtcdConfigProvider(client, "test4", new Properties(), "host2");

        PseudoLock lock1 = config1.newPseudoLock();
        PseudoLock lock2 = config2.newPseudoLock();

        Assert.assertTrue(lock1.lock(Mockito.mock(ActivityLog.class), 8, TimeUnit.SECONDS));
        Assert.assertFalse(lock2.lock(Mockito.mock(ActivityLog.class), 1, TimeUnit.SECONDS));
        Assert.assertFalse(lock1.lock(Mockito.mock(ActivityLog.class), 8, TimeUnit.SECONDS));
        lock1.unlock();

        Assert.assertTrue(lock2.lock(Mockito.mock(ActivityLog.class), 30, TimeUnit.SECONDS));
    }

    @Test(threadPoolSize = 5, invocationCount = 25,  timeOut = 10000)
    public void testConcurrentAccess() throws Exception {
        EtcdConfigProvider config = new EtcdConfigProvider(client, "test4", new Properties(), "host1");
        PseudoLock lock = config.newPseudoLock();

        ActivityLog log = Mockito.mock(ActivityLog.class);
        long maxWait = 5;
        TimeUnit unit = TimeUnit.SECONDS;
        int iterations = 5;
        for (int i=0; i<iterations; i++) {
            try {
                if (lock.lock(log, maxWait, unit)) {
                    Assert.assertTrue(bool.compareAndSet(false, true));
                    Assert.assertTrue(bool.compareAndSet(true, false));
                    lock.unlock();
                } else {
                    Assert.fail("Lock failed to be acquired");
                }
            } catch (Exception e) {
                throw new RuntimeException("Lock failed", e.getCause());
            }
        }
    }

    private static AtomicBoolean bool = new AtomicBoolean(false);
}