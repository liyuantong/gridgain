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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatch;
import org.apache.ignite.internal.processors.continuous.GridContinuousQueryBatch;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Buffer for collecting CQ acknowledges before sending CQ buffer cleanup message to backup nodes.
 */
class CacheContinuousQueryAcknowledgeBackupBuffer {
    /** */
    private int size;

    /** */
    @GridToStringInclude
    private Map<Integer, Long> updateCntrs = new HashMap<>();

    /** */
    @GridToStringInclude
    private Set<AffinityTopologyVersion> topVers = U.newHashSet(1);

    /**
     * @param batch Batch.
     * @return Tuple if acknowledge should be sent to backups.
     */
    @SuppressWarnings("unchecked")
    @Nullable synchronized IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>> onAcknowledged(
        GridContinuousBatch batch) {
        assert batch instanceof GridContinuousQueryBatch;

        size += ((GridContinuousQueryBatch)batch).entriesCount();

        Collection<CacheContinuousQueryEntry> entries = (Collection)batch.collect();

        for (CacheContinuousQueryEntry e : entries)
            addEntry(e);

        return size >= CacheContinuousQueryHandler.BACKUP_ACK_THRESHOLD ? acknowledgeData() : null;
    }

    /**
     * @param e Entry.
     * @return Tuple if acknowledge should be sent to backups.
     */
    @Nullable synchronized IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>>
    onAcknowledged(CacheContinuousQueryEntry e) {
        size++;

        addEntry(e);

        return size >= CacheContinuousQueryHandler.BACKUP_ACK_THRESHOLD ? acknowledgeData() : null;
    }

    /**
     * @param e Entry.
     */
    private void addEntry(CacheContinuousQueryEntry e) {
        topVers.add(e.topologyVersion());

        Long cntr0 = updateCntrs.get(e.partition());

        if (cntr0 == null || e.updateCounter() > cntr0)
            updateCntrs.put(e.partition(), e.updateCounter());
    }

    /**
     * @return Tuple if acknowledge should be sent to backups.
     */
    @Nullable synchronized IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>>
        acknowledgeOnTimeout() {
        return size > 0 ? acknowledgeData() : null;
    }

    /**
     * @return Tuple with acknowledge information.
     */
    private IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>> acknowledgeData() {
        assert size > 0;

        Map<Integer, Long> cntrs = new HashMap<>(updateCntrs);

        IgniteBiTuple<Map<Integer, Long>, Set<AffinityTopologyVersion>> res =
            new IgniteBiTuple<>(cntrs, topVers);

        topVers = U.newHashSet(1);

        updateCntrs.clear();

        size = 0;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheContinuousQueryAcknowledgeBackupBuffer.class, this);
    }
}
