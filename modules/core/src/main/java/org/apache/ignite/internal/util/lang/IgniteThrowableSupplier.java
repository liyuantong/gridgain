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

package org.apache.ignite.internal.util.lang;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;

/**
 * Represents a supplier of results. There is no requirement that a new or distinct result be returned each
 * time the supplier is invoked.
 *
 * Also it is able to throw {@link IgniteCheckedException} unlike {@link java.util.function.Supplier}.
 *
 * @param <E> The type of results supplied by this supplier.
 */
@FunctionalInterface
public interface IgniteThrowableSupplier<E> extends Serializable {
    /**
     * Gets a result.
     *
     * @return a result
     * @throws IgniteCheckedException If result calculation failed.
     */
    public E get() throws IgniteCheckedException;
}
