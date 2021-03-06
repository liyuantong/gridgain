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

package org.apache.ignite.tensorflow.core.nativerunning;

import java.io.Serializable;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.tensorflow.util.SerializableSupplier;

/**
 * Native process specification.
 */
public class NativeProcess implements Serializable {
    /** */
    private static final long serialVersionUID = -7056800139746134956L;

    /** Process builder supplier. */
    private final SerializableSupplier<ProcessBuilder> procBuilderSupplier;

    /** Stdin of the process. */
    private final String stdin;

    /** Node identifier. */
    private final UUID nodeId;

    /**
     * Constructs a new instance of native process specification.
     *
     * @param procBuilderSupplier Process builder supplier.
     * @param stdin Stdin of the process.
     * @param nodeId Node identifier.
     */
    public NativeProcess(SerializableSupplier<ProcessBuilder> procBuilderSupplier, String stdin, UUID nodeId) {
        assert procBuilderSupplier != null : "Process builder supplier should not be null";
        assert nodeId != null : "Node identifier should not be null";

        this.procBuilderSupplier = procBuilderSupplier;
        this.stdin = stdin;
        this.nodeId = nodeId;
    }

    /** */
    public Supplier<ProcessBuilder> getProcBuilderSupplier() {
        return procBuilderSupplier;
    }

    /** */
    public String getStdin() {
        return stdin;
    }

    /** */
    public UUID getNodeId() {
        return nodeId;
    }
}
