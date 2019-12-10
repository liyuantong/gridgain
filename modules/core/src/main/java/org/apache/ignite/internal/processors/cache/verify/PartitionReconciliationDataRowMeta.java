/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.verify;

import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Data row meta including information about key, value and repair meta within the context of partition reconciliation.
 */
public class PartitionReconciliationDataRowMeta extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Binary and string representation of a versioned key. */
    private PartitionReconciliationKeyMeta keyMeta;

    /** Binary and string representation of a value. */
    private PartitionReconciliationValueMeta valMeta;

    /** Repair meta including:
     * <ul>
     *     <li>boolean flag that indicates whether data was fixed or not;</li>
     *     <li>value that was used to fix entry;</li>
     *     <li>repair algorithm that was used;</li>
     * </ul>
     */
    private PartitionReconciliationRepairMeta repairMeta;

    /**
     * Default constructor for externalization.
     */
    public PartitionReconciliationDataRowMeta() {
    }

    /**
     * Constructor.
     *
     * @param keyMeta Binary and string representation of a versioned key.
     * @param valMeta Binary and string representation of a value.
     */
    public PartitionReconciliationDataRowMeta(
        PartitionReconciliationKeyMeta keyMeta,
        PartitionReconciliationValueMeta valMeta) {
        this.keyMeta = keyMeta;
        this.valMeta = valMeta;
    }

    /**
     * Constructor.
     *
     * @param keyMeta Binary and string representation of a versioned key.
     * @param valMeta Binary and string representation of a value.
     * @param repairMeta Repair meta including:
     *  <ul>
     *      <li>boolean flag that indicates whether data was fixed or not;</li>
     *      <li>value that was used to fix entry;</li>
     *      <li>repair algorithm that was used;</li>
     *  </ul>
     */
    public PartitionReconciliationDataRowMeta(
        PartitionReconciliationKeyMeta keyMeta,
        PartitionReconciliationValueMeta valMeta,
        PartitionReconciliationRepairMeta repairMeta) {
        this.keyMeta = keyMeta;
        this.valMeta = valMeta;
        this.repairMeta = repairMeta;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(keyMeta);
        out.writeObject(valMeta);
        out.writeObject(repairMeta);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException,
        ClassNotFoundException {
        keyMeta = (PartitionReconciliationKeyMeta) in.readObject();
        valMeta = (PartitionReconciliationValueMeta) in.readObject();
        repairMeta = (PartitionReconciliationRepairMeta) in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionReconciliationDataRowMeta.class, this);
    }
}
