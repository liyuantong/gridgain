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

import java.util.Collection;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ClusterReadOnlyModeAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 3;

    /** Cache names. */
    protected static final Collection<String> CACHE_NAMES = ClusterReadOnlyModeTestUtils.cacheNames();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        changeClusterReadOnlyMode(false);

        for (String cacheName : CACHE_NAMES)
            grid(0).cache(cacheName).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(ClusterReadOnlyModeTestUtils.cacheConfigurations());

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        return cfg;
    }

    /**
     * Change read only mode on all nodes.
     *
     * @param readOnly Read only.
     */
    protected void changeClusterReadOnlyMode(boolean readOnly) {
        grid(0).cluster().state(readOnly ? ClusterState.ACTIVE_READ_ONLY : ClusterState.ACTIVE);
    }
}
