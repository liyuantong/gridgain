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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;

/**
 *
 */
public abstract class AbstractCacheOperationRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(getCacheConfigurations());
    }

    /**
     * Getting array of cache configurations.
     */
    protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
        };
    }

    /**
     * Getting the key that is contained on primary partition on passed node for {@link #CACHE_NAME} cache.
     *
     * @param ignite Node.
     * @return Key.
     */
    protected Integer prmKey(IgniteEx ignite) {
        return findKeys(ignite.localNode(), ignite.cache(CACHE_NAME), 1, 0, 0)
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(ignite.name() + " isn't primary node for any key."));
    }

    /**
     * Getting the key that is contained on primary partition on passed node for {@link #CACHE_NAME} cache.
     *
     * @param nodeName Node name.
     * @return Key.
     */
    protected Integer prmKey(String nodeName) {
        return prmKey((IgniteEx)G.ignite(nodeName));
    }
}
