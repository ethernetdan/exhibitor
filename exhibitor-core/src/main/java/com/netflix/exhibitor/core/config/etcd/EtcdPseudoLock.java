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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EtcdPseudoLock implements PseudoLock {
    private final EtcdClient client;
    private final String lockPath;
    private final String hostname;

    private boolean ownsLock = false;
    private long lockStart;
    private long maxWait;
    private TimeUnit unit;

    private static final String UNLOCKED_NODE = "RELEASED";

    public EtcdPseudoLock(EtcdClient client, String lockPath, String hostname) {
        this.client = client;
        this.lockPath = lockPath;
        this.hostname = hostname;
    }

    @Override
    public boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception {
        lockStart = System.currentTimeMillis();
        if (ownsLock) {
            throw new IllegalStateException("Already locked");
        }
        this.maxWait = maxWait;
        this.unit = unit;
        if (!lock()) {
            log.add(ActivityLog.Type.ERROR, String.format("Could not acquire lock within %d ms, key: %s"
                    , maxWait, lockPath));
            return false;
        }
        ownsLock = true;
        return true;
    }

    @Override
    public void unlock() throws Exception {
        try {
            client.put(lockPath, UNLOCKED_NODE).send().get();
            ownsLock = false;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to unlock", e.getCause());
        }
    }

    private boolean lock() throws Exception {
        lockStart = System.currentTimeMillis();
        try {
            client.put(lockPath, hostname)
                    .prevValue(UNLOCKED_NODE)
                    .send().get();
            return true;
        } catch (IOException ie) {
            long timeLeft = timeLeft();
            if (!ie.getCause().getMessage().equals("java.lang.Exception: 412 Precondition Failed")  && timeLeft > 0) {
                try {
                    client.get(lockPath).waitForChange().timeout(timeLeft, TimeUnit.MILLISECONDS).send().get();
                    lock();
                } catch (TimeoutException te) {
                    return false;
                }
            }
            return false;
        } catch (EtcdException e) {
            if (e.errorCode == 100) {
                client.put(lockPath, UNLOCKED_NODE).prevExist(false).send().get();
            }
            return lock();
        }
    }

    private long timeLeft() {
        long elapsed = System.currentTimeMillis() - lockStart;
        return unit.toMillis(maxWait) - elapsed;
    }
}