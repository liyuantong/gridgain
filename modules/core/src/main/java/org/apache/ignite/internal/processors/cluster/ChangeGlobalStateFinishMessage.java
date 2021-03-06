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

package org.apache.ignite.internal.processors.cluster;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 *
 */
public class ChangeGlobalStateFinishMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** State change request ID. */
    private final UUID reqId;

    /** New cluster state. */
    @Deprecated
    private final boolean clusterActive;

    /** New cluster state. */
    private final ClusterState state;

    /** State change error. */
    private Boolean transitionRes;

    /**
     * @param reqId State change request ID.
     * @param state New cluster state.
     */
    public ChangeGlobalStateFinishMessage(
        UUID reqId,
        ClusterState state,
        Boolean transitionRes
    ) {
        assert reqId != null;
        assert state != null;

        this.reqId = reqId;
        this.state = state;
        this.clusterActive = ClusterState.active(state);
        this.transitionRes = transitionRes;
    }

    /**
     * @return State change request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return New cluster state.
     * @deprecated Use {@link #state()} instead.
     */
    @Deprecated
    public boolean clusterActive() {
        return clusterActive;
    }

    /**
     * @return Transition success status.
     */
    public boolean success() {
        if (transitionRes == null) {
            if (state != null)
                return ClusterState.active(state);
            else {
                // Backward compatibility.
                return clusterActive;
            }
        }
        return
            transitionRes;
    }

    /**
     * @return New cluster state.
     */
    public ClusterState state() {
        // Backward compatibility.
        return state != null ? state : (clusterActive ? ACTIVE : INACTIVE);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChangeGlobalStateFinishMessage.class, this);
    }
}
