package com.netflix.exhibitor.core.config.etcd;

import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.PseudoLock;
import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

        lock1.lock(Mockito.mock(ActivityLog.class), 8, TimeUnit.SECONDS);
        Assert.assertFalse(lock2.lock(Mockito.mock(ActivityLog.class), 1, TimeUnit.SECONDS));
        lock1.unlock();

        Assert.assertTrue(lock2.lock(Mockito.mock(ActivityLog.class), 30, TimeUnit.SECONDS));
    }
}