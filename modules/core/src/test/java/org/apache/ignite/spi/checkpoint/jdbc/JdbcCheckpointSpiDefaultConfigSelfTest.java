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

package org.apache.ignite.spi.checkpoint.jdbc;

import org.apache.ignite.spi.checkpoint.GridCheckpointSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.hsqldb.jdbc.jdbcDataSource;

/**
 * Grid jdbc checkpoint SPI default config self test.
 */
@GridSpiTest(spi = JdbcCheckpointSpi.class, group = "Checkpoint SPI")
public class JdbcCheckpointSpiDefaultConfigSelfTest extends
    GridCheckpointSpiAbstractTest<JdbcCheckpointSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(JdbcCheckpointSpi spi) throws Exception {
        jdbcDataSource ds = new jdbcDataSource();

        ds.setDatabase("jdbc:hsqldb:mem:gg_test_" + getClass().getSimpleName());
        ds.setUser("sa");
        ds.setPassword("");

        spi.setDataSource(ds);

        // Default BLOB type is not valid for hsqldb.
        spi.setValueFieldType("longvarbinary");

        super.spiConfigure(spi);
    }
}