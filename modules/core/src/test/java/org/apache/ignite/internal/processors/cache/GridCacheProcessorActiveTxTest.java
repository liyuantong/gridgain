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

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Tests that verify the performance of GridCacheProcessor operations
 * in active transactions.
 */
public class GridCacheProcessorActiveTxTest extends GridCommonAbstractTest {
    /**
     * The format of the exception message.
     *
     * @see GridCacheProcessor#CHECK_EMPTY_TRANSACTIONS_ERROR_MSG_FORMAT
     */
    private static final String CHECK_EMPTY_TRANSACTIONS_ERROR_MSG_FORMAT =
        getFieldValue(GridCacheProcessor.class, "CHECK_EMPTY_TRANSACTIONS_ERROR_MSG_FORMAT");

    /** Node. */
    private static IgniteEx NODE;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        NODE = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Checking the throw exception during the operation "dynamicStartCache".
     */
    @Test
    public void testDynamicSingleCacheStart() {
        opInActiveTx(
            () -> NODE.context().cache().dynamicStartCache(null, DEFAULT_CACHE_NAME, null, false, false, true),
            DEFAULT_CACHE_NAME,
            "dynamicStartCache"
        );
    }

    /**
     * Checking the throw exception during the operation
     * "dynamicStartCachesByStoredConf".
     */
    @Test
    public void testDynamicStartMultipleCaches() {
        List<String> cacheNames = cacheNames();

        List<CacheConfiguration> cacheCfgs =
            cacheNames().stream().map(CacheConfiguration::new).collect(toList());

        opInActiveTx(
            () -> NODE.context().cache().dynamicStartCaches(cacheCfgs, false, true, false),
            cacheNames.toString(),
            "dynamicStartCachesByStoredConf"
        );
    }

    /**
     * Checking the throw exception during the operation "dynamicDestroyCache".
     */
    @Test
    public void testDynamicCacheDestroy() {
        opInActiveTx(
            () -> NODE.context().cache().dynamicDestroyCache(DEFAULT_CACHE_NAME, false, true, false, null),
            DEFAULT_CACHE_NAME,
            "dynamicDestroyCache"
        );
    }

    /**
     * Checking the throw exception during the operation
     * "dynamicDestroyCaches".
     */
    @Test
    public void testDynamicDestroyMultipleCaches() {
        List<String> cacheNames = cacheNames();

        opInActiveTx(
            () -> NODE.context().cache().dynamicDestroyCaches(cacheNames, true),
            cacheNames.toString(),
            "dynamicDestroyCaches"
        );
    }

    /**
     * Checking the throw exception during the operation "dynamicCloseCache".
     */
    @Test
    public void testDynamicCacheClose() {
        GridCacheProcessor cacheProcessor = NODE.context().cache();

        String cacheName = DEFAULT_CACHE_NAME;

        cacheProcessor.addjCacheProxy(cacheName, new IgniteCacheProxyImpl<>());

        opInActiveTx(() -> cacheProcessor.dynamicCloseCache(DEFAULT_CACHE_NAME), cacheName, "dynamicCloseCache");

        ((Map)getFieldValue(cacheProcessor, "jCacheProxies")).clear();
    }

    /**
     * Checking the throw exception during the operation "resetCacheState".
     */
    @Test
    public void testResetCacheState() {
        List<String> cacheNames = cacheNames();

        opInActiveTx(
            () -> NODE.context().cache().resetCacheState(cacheNames),
            cacheNames.toString(),
            "resetCacheState"
        );
    }

    /**
     * Create cache names.
     *
     * @return Cache names.
     */
    private List<String> cacheNames() {
        return range(0, 2).mapToObj(i -> DEFAULT_CACHE_NAME + i).collect(toList());
    }

    /**
     * Performing an operation in an active transaction with a check that an
     * exception will be thrown with a format
     * {@link #CHECK_EMPTY_TRANSACTIONS_ERROR_MSG_FORMAT} message.
     *
     * @param runnableX Operation in an active transaction.
     * @param cacheName Cache name for the exception message.
     * @param operation Operation for the exception message.
     */
    private void opInActiveTx(RunnableX runnableX, String cacheName, String operation) {
        assert nonNull(runnableX);
        assert nonNull(cacheName);
        assert nonNull(operation);

        try (Transaction transaction = NODE.transactions().txStart()) {
            assertThrows(
                log,
                runnableX,
                IgniteException.class,
                format(CHECK_EMPTY_TRANSACTIONS_ERROR_MSG_FORMAT, cacheName, operation)
            );
        }
    }
}
