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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.service.ServiceDeploymentActions;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 * Message represent request for change cluster global state.
 */
public class ChangeGlobalStateMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Request ID */
    private UUID reqId;

    /** Initiator node ID. */
    private UUID initiatingNodeId;

    /** If true activate else deactivate. */
    private boolean activate;

    /** Cluster state */
    private ClusterState state;

    /** Configurations read from persistent store. */
    private List<StoredCacheData> storedCfgs;

    /** */
    @Nullable private BaselineTopology baselineTopology;

    /** */
    private boolean forceChangeBaselineTopology;

    /** */
    private long timestamp;

    /** */
    @GridToStringExclude
    private transient ExchangeActions exchangeActions;

    /** Services deployment actions to be processed on services deployment process. */
    @GridToStringExclude
    @Nullable private transient ServiceDeploymentActions serviceDeploymentActions;

    /**
     * @param reqId State change request ID.
     * @param initiatingNodeId Node initiated state change.
     * @param storedCfgs Configurations read from persistent store.
     * @param state New cluster state.
     * @param baselineTopology Baseline topology.
     * @param forceChangeBaselineTopology Force change baseline topology flag.
     * @param timestamp Timestamp.
     */
    public ChangeGlobalStateMessage(
        UUID reqId,
        UUID initiatingNodeId,
        @Nullable List<StoredCacheData> storedCfgs,
        ClusterState state,
        BaselineTopology baselineTopology,
        boolean forceChangeBaselineTopology,
        long timestamp
    ) {
        assert reqId != null;
        assert initiatingNodeId != null;

        this.reqId = reqId;
        this.initiatingNodeId = initiatingNodeId;
        this.storedCfgs = storedCfgs;
        this.activate = ClusterState.active(state);
        this.state = state;
        this.baselineTopology = baselineTopology;
        this.forceChangeBaselineTopology = forceChangeBaselineTopology;
        this.timestamp = timestamp;
    }

    /**
     * @return Configurations read from persistent store..
     */
    @Nullable public List<StoredCacheData> storedCacheConfigurations() {
        return storedCfgs;
    }

    /**
     * @return Cache updates to be executed on exchange. If {@code null} exchange is not needed.
     */
    @Nullable public ExchangeActions exchangeActions() {
        return exchangeActions;
    }

    /**
     * @param exchangeActions Cache updates to be executed on exchange.
     */
    void exchangeActions(ExchangeActions exchangeActions) {
        assert exchangeActions != null && !exchangeActions.empty() : exchangeActions;

        this.exchangeActions = exchangeActions;
    }

    /**
     * @return Services deployment actions to be processed on services deployment process.
     */
    @Nullable public ServiceDeploymentActions servicesDeploymentActions() {
        return serviceDeploymentActions;
    }

    /**
     * @param serviceDeploymentActions Services deployment actions to be processed on services deployment process.
     */
    public void servicesDeploymentActions(ServiceDeploymentActions serviceDeploymentActions) {
        this.serviceDeploymentActions = serviceDeploymentActions;
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
    @Override public DiscoCache createDiscoCache(
        GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache
    ) {
        return mgr.createDiscoCacheOnCacheChange(topVer, discoCache);
    }

    /**
     * @return Node initiated state change.
     */
    public UUID initiatorNodeId() {
        return initiatingNodeId;
    }

    /**
     * @return {@code False} if new cluster state is {@link ClusterState#INACTIVE}, and {@code True} otherwise.
     * @deprecated Use {@link #state()} instead.
     */
    @Deprecated
    public boolean activate() {
        if (state != null)
            return ClusterState.active(state);
        else {
            // Backward compatibility.
            return activate;
        }
    }

    /**
     * @return New cluster state.
     */
    public ClusterState state() {
        if (state != null)
            return state;
        else {
            // Backward compatibility.
            return activate ? ACTIVE : INACTIVE;
        }
    }

    /**
     * @return Force change BaselineTopology flag.
     */
    public boolean forceChangeBaselineTopology() {
        return forceChangeBaselineTopology;
    }

    /**
     * @return Baseline topology.
     */
    @Nullable public BaselineTopology baselineTopology() {
        return baselineTopology;
    }

    /**
     * @return Timestamp.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Sets timestamp.
     * @param timestamp Timestamp.
     */
    public void timestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return State change request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChangeGlobalStateMessage.class, this);
    }
}
