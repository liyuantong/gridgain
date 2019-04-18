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

package org.apache.ignite.spi.discovery.zk.internal;

/**
 *
 */
class ZkInternalJoinErrorMessage implements ZkInternalMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    transient boolean notifyNode = true;

    /** */
    final long nodeInternalId;

    /** */
    final String err;

    /**
     * @param nodeInternalId Joining node internal ID.
     * @param err Error message.
     */
    ZkInternalJoinErrorMessage(long nodeInternalId, String err) {
        this.nodeInternalId = nodeInternalId;
        this.err = err;
    }
}
