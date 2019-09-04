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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientBooleanResponse;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cluster status request.
 */
public class ClientClusterChangeStateRequest extends ClientClusterRequest {

    /** Next state. */
    private final boolean shouldBeActive;

    /**
     * Constructor.
     *
     * @param reader Reader/
     */
    public ClientClusterChangeStateRequest(BinaryRawReader reader) {
        super(reader);
        shouldBeActive = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override
    public ClientResponse process(ClientConnectionContext ctx) {
        ClientCluster clientCluster = ctx.resources().get(clusterId);
        clientCluster.changeGridState(shouldBeActive);
        return new ClientBooleanResponse(requestId(), clientCluster.isActive());
    }
}
