/*
 *    Copyright 2015 Dan Gillespie
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.exhibitor.core.config.etcd;

import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.PseudoLock;
import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.requests.EtcdKeyPutRequest;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EtcdPseudoLock implements PseudoLock {
    private final EtcdClient client;
    private final String lockPath;
    private final String hostname;

    private boolean ownsLock = false;
    private long lockStart;

    private static final String UNLOCKED_NODE = "RELEASED";

    public EtcdPseudoLock(EtcdClient client, String lockPath, String hostname) {
        this.client = client;
        this.lockPath = lockPath;
        this.hostname = hostname;
    }

    @Override
    public boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception {
        if (ownsLock) {
            throw new IllegalStateException("Already locked");
        }
        lockStart = System.currentTimeMillis();
        if (lock(maxWait, unit)) {
            log.add(ActivityLog.Type.ERROR, String.format("Could not acquire lock within %d ms, key: %s"
                    , maxWait, lockPath));
            return false;
        }
        return true;
    }

    @Override
    public void unlock() throws Exception {
        try {
            client.put(lockPath, UNLOCKED_NODE).send().get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to unlock", e.getCause());
        }
    }

    private boolean lock(long maxWait, TimeUnit unit) throws Exception {
        if (timeLeft(maxWait, unit) <= 0) {
            return false;
        }
        try {
            EtcdKeysResponse.EtcdNode node = client.get(lockPath).send().get().node;
            if (!node.value.equals(hostname) || !node.value.equals(UNLOCKED_NODE)) {
                client.get(lockPath).waitForChange().timeout(timeLeft(maxWait, unit), TimeUnit.MILLISECONDS);
            }

            return createLock(node.modifiedIndex, timeLeft(maxWait, unit)) || lock(maxWait, unit);
        } catch (EtcdException e) {
            if (e.errorCode == 100) {
                // lock key does not exist
                return createLock(-1, timeLeft(maxWait, unit)) || lock(maxWait, unit);
            }
            throw new RuntimeException("Lock could not be acquired.", e.getCause());
        } catch (TimeoutException e) {
            return false;
        }
    }

    private boolean createLock(long modifiedIndex, long timeout) throws Exception {
        try {
            EtcdKeyPutRequest req = client.put(lockPath, hostname).timeout(timeout, TimeUnit.MILLISECONDS);
            if (modifiedIndex == -1) {
                // lock key does not exist, attempt to create
                req.prevExist(false).send().get();
            } else {
                req.prevIndex(modifiedIndex).send().get();
            }
            return true;
        }
        catch (EtcdException e) {
            return false;
        }
    }

    private long timeLeft(long duration, TimeUnit unit) {
        long elapsed = System.currentTimeMillis() - lockStart;
        return unit.toMillis(duration) - elapsed;
    }
}