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

package org.apache.ignite.spi.failover.never;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJobResult;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.failover.GridFailoverTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Never failover SPI test.
 */
@GridSpiTest(spi = NeverFailoverSpi.class, group = "Failover SPI")
@RunWith(JUnit4.class)
public class GridNeverFailoverSpiSelfTest extends GridSpiAbstractTest<NeverFailoverSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAlwaysNull() throws Exception {
        List<ClusterNode> nodes = new ArrayList<>();

        ClusterNode node = new GridTestNode(UUID.randomUUID());

        nodes.add(node);

        assert getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), new GridTestJobResult(node)),
            nodes) == null;
    }
}
