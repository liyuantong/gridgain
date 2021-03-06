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
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** */
public class BaselineAutoAdjustInMemoryTest extends BaselineAutoAdjustTest {
    /** {@inheritDoc} */
    @Override protected boolean isPersistent() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithZeroTimeout() throws Exception {
        startGrids(3);

        startGrid(getConfiguration(UUID.randomUUID().toString()).setClientMode(true));

        stopGrid(2);

        assertEquals(2, grid(0).cluster().currentBaselineTopology().size());

        startGrid(3);

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());

        startGrid(4);

        assertEquals(4, grid(0).cluster().currentBaselineTopology().size());

        stopGrid(1);

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());

        IgniteEx client = startGrid(getConfiguration(UUID.randomUUID().toString()).setClientMode(true));

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());

        stopGrid(client.name());

        assertEquals(3, grid(0).cluster().currentBaselineTopology().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBaselineManually() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        assertEquals(1, ignite0.cluster().currentBaselineTopology().size());

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        IgniteEx ignite1 = startGrid(1);

        assertEquals(1, ignite0.cluster().currentBaselineTopology().size());
        assertEquals(1, ignite1.cluster().currentBaselineTopology().size());

        assertFalse(ignite1.cluster().isBaselineAutoAdjustEnabled());

        ignite0.cluster().setBaselineTopology(ignite0.context().discovery().aliveServerNodes());

        assertEquals(2, ignite0.cluster().currentBaselineTopology().size());
        assertEquals(2, ignite1.cluster().currentBaselineTopology().size());
    }

    /**
     * Tests that cluster with one server and client do not hung when activating
     */
    @Test
    public void testActivatingIsNotHung() throws Exception {
        IgniteEx ig = startGrid(0);

        startClientGrid(1);

        ig.cluster().active(true);
    }
}
