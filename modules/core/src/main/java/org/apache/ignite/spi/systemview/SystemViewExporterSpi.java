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

package org.apache.ignite.spi.systemview;

import java.util.function.Predicate;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.systemview.jmx.JmxSystemViewExporterSpi;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Exporter of system view to the external recepient.
 * Expected, that each implementation would support some specific protocol.
 *
 * Implementation of this Spi should work by pull paradigm.
 * So after start SPI should respond to some incoming request.
 * HTTP servlet or JMX bean are good examples of expected implementations.
 *
 * @see ReadOnlySystemViewRegistry
 * @see SystemView
 * @see SystemViewRowAttributeWalker
 * @see JmxSystemViewExporterSpi
 */
public interface SystemViewExporterSpi extends IgniteSpi {
    /**
     * Sets system view registry that SPI should export.
     * This method called before {@link #spiStart(String)}.
     *
     * @param registry System view registry.
     */
    public void setSystemViewRegistry(ReadOnlySystemViewRegistry registry);

    /**
     * Sets export filter.
     * System view that not satisfy {@code filter} shouldn't be exported.
     *
     * @param filter Filter.
     */
    public void setExportFilter(Predicate<SystemView<?>> filter);
}
