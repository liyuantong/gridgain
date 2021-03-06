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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;

/**
 *
 */
public class IgniteCacheGroupsCompareQueryTest extends BaseH2CompareQueryTest {
    /**
     * Creates new cache configuration.
     *
     * @param name Cache name.
     * @param mode Cache mode.
     * @param clsK Key class.
     * @param clsV Value class.
     * @return Cache configuration.
     */
    @Override protected CacheConfiguration cacheConfiguration(String name, CacheMode mode, Class<?> clsK, Class<?> clsV) {
        CacheConfiguration<?,?> cc = super.cacheConfiguration(name, mode, clsK, clsV);

        if (ORG.equals(name) || PERS.equals(name) || PURCH.equals(name))
            cc.setGroupName("group");

        return cc;
    }
}
