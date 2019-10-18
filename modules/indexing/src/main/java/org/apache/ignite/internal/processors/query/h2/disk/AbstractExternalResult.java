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

package org.apache.ignite.internal.processors.query.h2.disk;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.h2.result.ResultExternal;
import org.h2.result.ResultInterface;

/**
 * Basic class for external result.
 */
@SuppressWarnings({"MissortedModifiers", "WeakerAccess", "ForLoopReplaceableByForEach"})
public abstract class AbstractExternalResult implements ResultExternal {

    /** Logger. */
    protected final IgniteLogger log;

    /** Current size in rows. */
    protected int size;

    /** Memory tracker. */
    protected final H2MemoryTracker memTracker;

    /** Parent result. */
    protected final AbstractExternalResult parent;

    /** Child results count. Parent result is closed only when all children are closed. */
    private int childCnt;

    /** */
    private boolean closed;

    /** File with spilled rows data. */
    protected final ExternalResultData data;

    /**
     * @param ctx Kernal context.
     * @param memTracker Memory tracker
     */
    protected AbstractExternalResult(GridKernalContext ctx, H2MemoryTracker memTracker, boolean useHashIdx, long initSize) {
        this.log = ctx.log(AbstractExternalResult.class);
        this.data = new ExternalResultData(log, ctx.config().getWorkDirectory(), ctx.query().fileIOFactory(),
            ctx.localNodeId(), useHashIdx, initSize);
        this.parent = null;
        this.memTracker = memTracker;
    }

    /**
     * Used for {@link ResultInterface#createShallowCopy(org.h2.engine.SessionInterface)} only.
     * @param parent Parent result.
     */
    protected AbstractExternalResult(AbstractExternalResult parent) {
        log = parent.log;
        size = parent.size;
        data = parent.data.createShallowCopy();
        this.parent = parent;
        // We do not need to keep the tracker because all data in this result is already on disk.
        memTracker = null;
    }


    /**
     * @return {@code True} if it is need to spill rows to disk.
     */
    protected boolean needToSpill() {
        return !memTracker.reserved(0);
    }

    /** */
    protected synchronized void onChildCreated() {
        childCnt++;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (closed)
            return;

        closed = true;

        if (parent == null) {
            if (childCnt == 0)
                onClose();
        }
        else
            parent.closeChild();
    }

    /** */
    protected synchronized void closeChild() {
        if (--childCnt == 0 && closed)
            onClose();
    }

    /** */
    protected void onClose() {
        data.close();
    }
}
