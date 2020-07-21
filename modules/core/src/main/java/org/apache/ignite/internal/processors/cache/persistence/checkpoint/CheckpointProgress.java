/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents information of a progress of a given checkpoint and allows to obtain future to wait for a particular
 * checkpoint state.
 */
public interface CheckpointProgress {
    /**
     * @return Wakeup reason.
     */
    public @Nullable String reason();

    /** */
    boolean inProgress();

    /** */
    GridFutureAdapter futureFor(CheckpointState state);

    /**
     * Mark this checkpoint execution as failed.
     *
     * @param error Causal error of fail.
     */
    void fail(Throwable error);

    /**
     * Changing checkpoint state if order of state is correct.
     *
     * @param newState New checkpoint state.
     */
    void transitTo(@NotNull CheckpointState newState);

    /**
     * @return PartitionDestroyQueue.
     */
    PartitionDestroyQueue getDestroyQueue();

    /**
     * @return Counter for written checkpoint pages. Not <code>null</code> only if checkpoint is running.
     */
    AtomicInteger writtenPagesCounter();

    /**
     * @return Counter for fsynced checkpoint pages. Not  <code>null</code> only if checkpoint is running.
     */
    AtomicInteger syncedPagesCounter();

    /**
     * @return Counter for evicted pages during current checkpoint. Not <code>null</code> only if checkpoint is running.
     */
    AtomicInteger evictedPagesCounter();

    /**
     * @return Number of pages in current checkpoint. If checkpoint is not running, returns 0.
     */
    int currentCheckpointPagesCount();

    /**
     * Sets current checkpoint pages num to store.
     *
     * @param num Pages to store.
     */
    void currentCheckpointPagesCount(int num);

    /** Initialize all counters before checkpoint. */
    void initCounters(int pagesSize);

    /**
     * Update synced pages in checkpoint;
     *
     * @param delta Pages num to update.
     */
    void updateSyncedPages(int delta);

    /**
     * Update written pages in checkpoint;
     *
     * @param delta Pages num to update.
     */
    void updateWrittenPages(int delta);

    /**
     * Update evicted pages in checkpoint;
     *
     * @param delta Pages num to update.
     */
    void updateEvictedPages(int delta);

    /** Clear cp progress counters */
    public void clearCounters();

    /**
     * Invokes a callback closure then a checkpoint reaches specific state.
     * The closure will not be called if an error has happened while transitting to the state.
     *
     * @param state State.
     * @param clo Closure to call.
     */
    public void onStateChanged(CheckpointState state, Runnable clo);
}
