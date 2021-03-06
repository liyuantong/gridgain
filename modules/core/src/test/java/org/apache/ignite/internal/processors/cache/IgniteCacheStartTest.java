/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheStartTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "c1";

    /** */
    private boolean client;

    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testStartAndNodeJoin() throws Exception {
        Ignite node0 = startGrid(0);

        checkCache(0, CACHE_NAME, false);

        node0.createCache(cacheConfiguration(CACHE_NAME));

        checkCache(0, CACHE_NAME, true);

        startGrid(1);

        checkCache(0, CACHE_NAME, true);
        checkCache(1, CACHE_NAME, true);

        client = true;

        startGrid(2);

        checkCache(0, CACHE_NAME, true);
        checkCache(1, CACHE_NAME, true);
        checkCache(2, CACHE_NAME, false);

        ignite(2).destroyCache(CACHE_NAME);

        checkCache(0, CACHE_NAME, false);
        checkCache(1, CACHE_NAME, false);
        checkCache(2, CACHE_NAME, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartFromJoiningNode1() throws Exception {
        checkStartFromJoiningNode(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartFromJoiningNode2() throws Exception {
        checkStartFromJoiningNode(true);
    }

    /**
     * @param joinClient {@code True} if client node joins.
     * @throws Exception If failed.
     */
    private void checkStartFromJoiningNode(boolean joinClient) throws Exception {
        startGrid(0);
        startGrid(1);

        client = true;

        startGrid(2);

        ccfg = cacheConfiguration(CACHE_NAME);
        client = joinClient;

        startGrid(3);

        checkCache(0, CACHE_NAME, true);
        checkCache(1, CACHE_NAME, true);
        checkCache(2, CACHE_NAME, false);
        checkCache(3, CACHE_NAME, true);

        client = false;
        ccfg = null;

        startGrid(4);

        checkCache(0, CACHE_NAME, true);
        checkCache(1, CACHE_NAME, true);
        checkCache(2, CACHE_NAME, false);
        checkCache(3, CACHE_NAME, true);
        checkCache(4, CACHE_NAME, true);

        client = true;

        startGrid(5);

        checkCache(0, CACHE_NAME, true);
        checkCache(1, CACHE_NAME, true);
        checkCache(2, CACHE_NAME, false);
        checkCache(3, CACHE_NAME, true);
        checkCache(4, CACHE_NAME, true);
        checkCache(5, CACHE_NAME, false);

        ignite(5).destroyCache(CACHE_NAME);

        for (int i = 0; i < 5; i++)
            checkCache(i, CACHE_NAME, false);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        return new CacheConfiguration(cacheName);
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param expCache {@code True} if cache should be created.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCache(int idx, final String cacheName, final boolean expCache) throws IgniteCheckedException {
        final IgniteKernal node = (IgniteKernal)ignite(idx);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expCache == (node.context().cache().cache(cacheName) != null);
            }
        }, 1000));

        assertNotNull(node.context().cache().cache(CU.UTILITY_CACHE_NAME));
    }
}
