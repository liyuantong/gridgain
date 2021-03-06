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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Service configuration key.
 *
 * @deprecated Services internals use messages for deployment management instead of the utility cache, since Ignite 2.8.
 */
@Deprecated
public class GridServiceAssignmentsKey extends GridCacheUtilityKey<GridServiceAssignmentsKey> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private final String name;

    /**
     * @param name Service ID.
     */
    public GridServiceAssignmentsKey(String name) {
        assert name != null;

        this.name = name;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override protected boolean equalsx(GridServiceAssignmentsKey that) {
        return name.equals(that.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceAssignmentsKey.class, this);
    }
}