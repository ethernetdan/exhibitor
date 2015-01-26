package com.netflix.exhibitor.core.config.etcd;

import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.netflix.exhibitor.core.config.StringConfigs;
import mousio.etcd4j.EtcdClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestEtcdConfigProvider {
    public EtcdClient client;

    @BeforeMethod
    public void startEtcd() throws Exception {
        URI localEtcd = URI.create("http://127.0.0.1:4001");
        client = new EtcdClient(localEtcd);
    }

    @Test
    public void testConcurrentModification() throws Exception {
        EtcdConfigProvider config1 = new EtcdConfigProvider(client, "test2", new Properties(), "host1");
        EtcdConfigProvider config2 = new EtcdConfigProvider(client, "test2", new Properties(), "host1");

        config1.start();
        config2.start();

        Properties properties = new Properties();
        properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA,
                PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "A,BC,C");
        LoadedInstanceConfig loaded1 = config1.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties())
                , -1);

        Thread.sleep(1000);

        LoadedInstanceConfig loaded2 = config2.loadConfig();
        Assert.assertEquals("A,BC,C", loaded2.getConfig().getRootConfig().getString(StringConfigs.ZOO_CFG_EXTRA));

        properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "4,5,6");
        config2.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), loaded2.getVersion());

        Assert.assertNull(config1.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), loaded1.getVersion()));
        LoadedInstanceConfig newLoaded1 = config1.loadConfig();
        Assert.assertNotEquals(loaded1.getVersion(), newLoaded1.getVersion());
    }

    @Test
    public void testBasic() throws Exception {
        EtcdConfigProvider config = new EtcdConfigProvider(client, "test2", new Properties(), "host1");

        config.start();

        config.loadConfig();    // make sure there's no exception

        Properties properties = new Properties();
        properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "1,2,3");
        config.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), 0);

        Thread.sleep(1000);

        LoadedInstanceConfig instanceConfig = config.loadConfig();
        Assert.assertEquals("1,2,3", instanceConfig.getConfig().getRootConfig().getString(StringConfigs.ZOO_CFG_EXTRA));
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