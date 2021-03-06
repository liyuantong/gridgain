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

package org.apache.ignite.tests.p2p.startcache;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Organization class.
 */
class Organization1 implements Serializable {
    /** Organization ID (indexed). */
    @QuerySqlField(index = true)
    private UUID id;

    /** Organization name (indexed). */
    @QuerySqlField(index = true)
    private String name;

    /**
     * Create organization.
     *
     * @param name Organization name.
     */
    Organization1(String name) {
        id = UUID.randomUUID();

        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Organization1 [id=" + id + ", name=" + name + ']';
    }
}