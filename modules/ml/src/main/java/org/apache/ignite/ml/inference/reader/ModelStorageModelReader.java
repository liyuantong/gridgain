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

package org.apache.ignite.ml.inference.reader;

import org.apache.ignite.Ignition;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;
import org.apache.ignite.ml.inference.util.DirectorySerializer;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

/**
 * Model reader that reads directory or file from model storage and serializes it using {@link DirectorySerializer}.
 */
public class ModelStorageModelReader implements ModelReader {
    /** */
    private static final long serialVersionUID = -5878564742783562872L;

    /** Path to the directory or file. */
    private final String path;

    /** Model storage supplier. */
    private final IgniteSupplier<ModelStorage> mdlStorageSupplier;

    /**
     * Constructs a new instance of model storage inference model builder.
     *
     * @param path Path to the directory or file.
     */
    public ModelStorageModelReader(String path, IgniteSupplier<ModelStorage> mdlStorageSupplier) {
        this.path = path;
        this.mdlStorageSupplier = mdlStorageSupplier;
    }

    /**
     * Constructs a new instance of model storage inference model builder.
     *
     * @param path Path to the directory or file.
     */
    public ModelStorageModelReader(String path) {
        this(path, () -> new ModelStorageFactory().getModelStorage(Ignition.ignite()));
    }

    /** {@inheritDoc} */
    @Override public byte[] read() {
        ModelStorage storage = mdlStorageSupplier.get();

        return storage.getFile(path);
    }
}
