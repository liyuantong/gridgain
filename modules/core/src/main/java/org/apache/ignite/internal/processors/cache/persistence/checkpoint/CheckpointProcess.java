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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_RUNNER_THREAD_PREFIX;

/**
 * Representation of main steps of checkpoint.
 */
public class CheckpointProcess {
    /**
     * Starting from this number of dirty pages in checkpoint, array will be sorted with {@link
     * Arrays#parallelSort(Comparable[])} in case of {@link CheckpointWriteOrder#SEQUENTIAL}.
     */
    private final int parallelSortThreshold = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.CHECKPOINT_PARALLEL_SORT_THRESHOLD, 512 * 1024);

    /** This number of threads will be created and used for parallel sorting. */
    private static final int PARALLEL_SORT_THREADS = Math.min(Runtime.getRuntime().availableProcessors(), 8);

    /** Skip sync. */
    private final boolean skipSync = getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

    /** */
    private final IgniteWriteAheadLogManager wal;

    /** Snapshot manager. */
    private final IgniteCacheSnapshotManager snapshotMgr;

    /** Checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** */
    private final Supplier<Collection<DataRegion>> dataRegions;

    /** */
    private final Supplier<Collection<CacheGroupContext>> cacheGroupsContexts;

    /** */
    private final IgniteLogger log;

    /** Checkpoint metadata directory ("cp"), contains files with checkpoint start and end */
    private final CheckpointStorage checkpointStorage;

    /** Checkpoint write order. */
    private final CheckpointWriteOrder checkpointWriteOrder;

    /** */
    private final Collection<DbCheckpointListener> lsnrs = new CopyOnWriteArrayList<>();

    /** */
    private final String igniteInstanceName;

    /** */
    private final int checkpointCollectInfoThreads;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private volatile IgniteThreadPoolExecutor checkpointCollectInfoPool;

    /** Pointer to a memory recovery record that should be included into the next checkpoint record. */
    private volatile WALPointer memoryRecoveryRecordPtr;

    /**
     * @param logger Logger.
     * @param wal WAL manager.
     * @param snapshotManager Snapshot manager.
     * @param checkpointStorage Checkpoint mark storage.
     * @param checkpointReadWriteLock Checkpoint read write lock.
     * @param checkpointWriteOrder Checkpoint write order.
     * @param dataRegions Regions for checkpointing.
     * @param cacheGroupContexts Cache group context for checkpoint.
     * @param checkpointCollectInfoThreads Number of threads which should collect info for checkpoint.
     * @param igniteInstanceName Ignite instance name.
     */
    CheckpointProcess(
        Function<Class<?>, IgniteLogger> logger,
        IgniteWriteAheadLogManager wal,
        IgniteCacheSnapshotManager snapshotManager,
        CheckpointStorage checkpointStorage,
        CheckpointReadWriteLock checkpointReadWriteLock,
        CheckpointWriteOrder checkpointWriteOrder,
        Supplier<Collection<DataRegion>> dataRegions,
        Supplier<Collection<CacheGroupContext>> cacheGroupContexts,
        int checkpointCollectInfoThreads,
        String igniteInstanceName
    ) {
        this.wal = wal;
        this.snapshotMgr = snapshotManager;
        this.checkpointReadWriteLock = checkpointReadWriteLock;
        this.dataRegions = dataRegions;
        this.cacheGroupsContexts = cacheGroupContexts;
        this.checkpointCollectInfoPool = initializeCheckpointPool();
        this.log = logger.apply(getClass());
        this.checkpointStorage = checkpointStorage;
        this.checkpointWriteOrder = checkpointWriteOrder;
        this.igniteInstanceName = igniteInstanceName;
        this.checkpointCollectInfoThreads = checkpointCollectInfoThreads;
    }

    /**
     * @return Initialized checkpoint page write pool;
     */
    private IgniteThreadPoolExecutor initializeCheckpointPool() {
        if (checkpointCollectInfoThreads > 1)
            return new IgniteThreadPoolExecutor(
                CHECKPOINT_RUNNER_THREAD_PREFIX,
                igniteInstanceName,
                checkpointCollectInfoThreads,
                checkpointCollectInfoThreads,
                30_000,
                new LinkedBlockingQueue<Runnable>()
            );

        return null;
    }

    /**
     * First stage of checkpoint which collects demanded information.
     *
     * @param cpTs Checkpoint start timestamp.
     * @param curr Current checkpoint event info.
     * @param tracker Checkpoint metrics tracker.
     * @param heartbeat Hearbeat callback.
     * @return Checkpoint collected info.
     * @throws IgniteCheckedException if fail.
     */
    public Checkpoint markCheckpointBegin(
        long cpTs,
        CheckpointProgressImpl curr,
        CheckpointMetricsTracker tracker,
        Runnable heartbeat
    ) throws IgniteCheckedException {
        List<DbCheckpointListener> dbLsnrs = new ArrayList<>(lsnrs);

        CheckpointRecord cpRec = new CheckpointRecord(memoryRecoveryRecordPtr);

        memoryRecoveryRecordPtr = null;

        IgniteFuture snapFut = null;

        CheckpointPagesInfoHolder cpPagesHolder;

        int dirtyPagesCount;

        boolean hasPartitionsToDestroy;

        WALPointer cpPtr = null;

        DbCheckpointContextImpl ctx0 = new DbCheckpointContextImpl(
            curr, new PartitionAllocationMap(), checkpointCollectInfoPool, heartbeat
        );

        checkpointReadWriteLock.readLock();

        try {
            for (DbCheckpointListener lsnr : dbLsnrs)
                lsnr.beforeCheckpointBegin(ctx0);

            ctx0.awaitPendingTasksFinished();
        }
        finally {
            checkpointReadWriteLock.readUnlock();
        }

        tracker.onLockWaitStart();

        checkpointReadWriteLock.writeLock();

        try {
            curr.transitTo(LOCK_TAKEN);

            tracker.onMarkStart();

            // Listeners must be invoked before we write checkpoint record to WAL.
            for (DbCheckpointListener lsnr : dbLsnrs)
                lsnr.onMarkCheckpointBegin(ctx0);

            ctx0.awaitPendingTasksFinished();

            tracker.onListenersExecuteEnd();

            if (curr.nextSnapshot())
                snapFut = snapshotMgr.onMarkCheckPointBegin(curr.snapshotOperation(), cpRec, ctx0.partitionStatMap());

            fillCacheGroupState(cpRec);

            //There are allowable to replace pages only after checkpoint entry was stored to disk.
            cpPagesHolder = beginAllCheckpoints(curr.futureFor(MARKER_STORED_TO_DISK));

            curr.currentCheckpointPagesCount(cpPagesHolder.pagesNum());

            dirtyPagesCount = cpPagesHolder.pagesNum();

            hasPartitionsToDestroy = !curr.getDestroyQueue().pendingReqs().isEmpty();

            if (dirtyPagesCount > 0 || curr.nextSnapshot() || hasPartitionsToDestroy) {
                // No page updates for this checkpoint are allowed from now on.
                cpPtr = wal.log(cpRec);

                if (cpPtr == null)
                    cpPtr = CheckpointStatus.NULL_PTR;
            }

            curr.transitTo(PAGE_SNAPSHOT_TAKEN);
        }
        finally {
            checkpointReadWriteLock.writeUnlock();

            tracker.onLockRelease();
        }

        curr.transitTo(LOCK_RELEASED);

        for (DbCheckpointListener lsnr : dbLsnrs)
            lsnr.onCheckpointBegin(ctx0);

        if (snapFut != null) {
            try {
                snapFut.get();
            }
            catch (IgniteException e) {
                U.error(log, "Failed to wait for snapshot operation initialization: " +
                    curr.snapshotOperation(), e);
            }
        }

        if (dirtyPagesCount > 0 || hasPartitionsToDestroy) {
            tracker.onWalCpRecordFsyncStart();

            // Sync log outside the checkpoint write lock.
            wal.flush(cpPtr, true);

            tracker.onWalCpRecordFsyncEnd();

            CheckpointEntry checkpointEntry = checkpointStorage.writeCheckpointEntry(cpTs, cpRec.checkpointId(),
                cpPtr,
                cpRec,
                CheckpointEntryType.START, skipSync);

            curr.transitTo(MARKER_STORED_TO_DISK);

            tracker.onSplitAndSortCpPagesStart();

            GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages =
                splitAndSortCpPagesIfNeeded(cpPagesHolder);

            tracker.onSplitAndSortCpPagesEnd();

            return new Checkpoint(checkpointEntry, cpPages, curr);
        }
        else {
            if (curr.nextSnapshot())
                wal.flush(null, true);

            return new Checkpoint(null, GridConcurrentMultiPairQueue.EMPTY, curr);
        }
    }

    /**
     * Fill cache group state in checkpoint record.
     *
     * @param cpRec Checkpoint record for filling.
     * @throws IgniteCheckedException if fail.
     */
    private void fillCacheGroupState(CheckpointRecord cpRec) throws IgniteCheckedException {
        GridCompoundFuture grpHandleFut = checkpointCollectInfoPool == null ? null : new GridCompoundFuture();

        for (CacheGroupContext grp : cacheGroupsContexts.get()) {
            if (grp.isLocal() || !grp.walEnabled())
                continue;

            Runnable r = () -> {
                ArrayList<GridDhtLocalPartition> parts = new ArrayList<>(grp.topology().localPartitions().size());

                for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                    parts.add(part);

                CacheState state = new CacheState(parts.size());

                for (GridDhtLocalPartition part : parts) {
                    GridDhtPartitionState partState = part.state();

                    if (partState == LOST)
                        partState = OWNING;

                    state.addPartitionState(
                        part.id(),
                        part.dataStore().fullSize(),
                        part.updateCounter(),
                        (byte)partState.ordinal()
                    );
                }

                synchronized (cpRec) {
                    cpRec.addCacheGroupState(grp.groupId(), state);
                }
            };

            if (checkpointCollectInfoPool == null)
                r.run();
            else
                try {
                    GridFutureAdapter<?> res = new GridFutureAdapter<>();

                    checkpointCollectInfoPool.execute(U.wrapIgniteFuture(r, res));

                    grpHandleFut.add(res);
                }
                catch (RejectedExecutionException e) {
                    assert false : "Task should never be rejected by async runner";

                    throw new IgniteException(e); //to protect from disabled asserts and call to failure handler
                }
        }

        if (grpHandleFut != null) {
            grpHandleFut.markInitialized();

            grpHandleFut.get();
        }
    }

    /**
     * @param allowToReplace The sign which allows to replace pages from a checkpoint by page replacer.
     * @return holder of FullPageIds obtained from each PageMemory, overall number of dirty pages, and flag defines at
     * least one user page became a dirty since last checkpoint.
     */
    private CheckpointPagesInfoHolder beginAllCheckpoints(IgniteInternalFuture<?> allowToReplace) {
        Collection<DataRegion> regions = dataRegions.get();

        Collection<Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>>> res =
            new ArrayList<>(regions.size());

        int pagesNum = 0;

        for (DataRegion reg : regions) {
            if (!reg.config().isPersistenceEnabled())
                continue;

            GridMultiCollectionWrapper<FullPageId> nextCpPages = ((PageMemoryEx)reg.pageMemory())
                .beginCheckpoint(allowToReplace);

            pagesNum += nextCpPages.size();

            res.add(new T2<>((PageMemoryEx)reg.pageMemory(), nextCpPages));
        }

        return new CheckpointPagesInfoHolder(res, pagesNum);
    }

    /**
     * Reorders list of checkpoint pages and splits them into appropriate number of sublists according to {@link
     * DataStorageConfiguration#getCheckpointThreads()} and {@link DataStorageConfiguration#getCheckpointWriteOrder()}.
     *
     * @param cpPages Checkpoint pages with overall count and user pages info.
     */
    private GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> splitAndSortCpPagesIfNeeded(
        CheckpointPagesInfoHolder cpPages
    ) throws IgniteCheckedException {
        Set<T2<PageMemoryEx, FullPageId[]>> cpPagesPerRegion = new HashSet<>();

        int realPagesArrSize = 0;

        int totalPagesCnt = cpPages.pagesNum();

        for (Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>> regPages : cpPages.cpPages()) {
            FullPageId[] pages = new FullPageId[regPages.getValue().size()];

            int pagePos = 0;

            for (int i = 0; i < regPages.getValue().collectionsSize(); i++) {
                for (FullPageId page : regPages.getValue().innerCollection(i)) {
                    if (realPagesArrSize++ == totalPagesCnt)
                        throw new AssertionError("Incorrect estimated dirty pages number: " + totalPagesCnt);

                    pages[pagePos++] = page;
                }
            }

            // Some pages may have been already replaced.
            if (pagePos != pages.length)
                cpPagesPerRegion.add(new T2<>(regPages.getKey(), Arrays.copyOf(pages, pagePos)));
            else
                cpPagesPerRegion.add(new T2<>(regPages.getKey(), pages));
        }

        if (checkpointWriteOrder == CheckpointWriteOrder.SEQUENTIAL) {
            Comparator<FullPageId> cmp = Comparator.comparingInt(FullPageId::groupId)
                .thenComparingLong(FullPageId::effectivePageId);

            ForkJoinPool pool = null;

            for (T2<PageMemoryEx, FullPageId[]> pagesPerReg : cpPagesPerRegion) {
                if (pagesPerReg.getValue().length >= parallelSortThreshold)
                    pool = parallelSortInIsolatedPool(pagesPerReg.get2(), cmp, pool);
                else
                    Arrays.sort(pagesPerReg.get2(), cmp);
            }

            if (pool != null)
                pool.shutdown();
        }

        return new GridConcurrentMultiPairQueue<>(cpPagesPerRegion);
    }

    /**
     * Performs parallel sort in isolated fork join pool.
     *
     * @param pagesArr Pages array.
     * @param cmp Cmp.
     * @return ForkJoinPool instance, check {@link ForkJoinTask#fork()} realization.
     */
    private static ForkJoinPool parallelSortInIsolatedPool(
        FullPageId[] pagesArr,
        Comparator<FullPageId> cmp,
        ForkJoinPool pool
    ) throws IgniteCheckedException {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = new ForkJoinPool.ForkJoinWorkerThreadFactory() {
            @Override public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);

                worker.setName("checkpoint-pages-sorter-" + worker.getPoolIndex());

                return worker;
            }
        };

        ForkJoinPool execPool = pool == null ?
            new ForkJoinPool(PARALLEL_SORT_THREADS + 1, factory, null, false) : pool;

        Future<?> sortTask = execPool.submit(() -> Arrays.parallelSort(pagesArr, cmp));

        try {
            sortTask.get();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }
        catch (ExecutionException e) {
            throw new IgniteCheckedException("Failed to perform pages array parallel sort", e.getCause());
        }

        return execPool;
    }

    /**
     * @param chp Checkpoint snapshot.
     */
    void markCheckpointEnd(Checkpoint chp) throws IgniteCheckedException {
        synchronized (this) {
            for (DataRegion memPlc : dataRegions.get()) {
                if (!memPlc.config().isPersistenceEnabled())
                    continue;

                ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();
            }
        }

        if (chp.hasDelta()) {
            checkpointStorage.writeCheckpointEntry(
                chp.cpEntry.timestamp(),
                chp.cpEntry.checkpointId(),
                chp.cpEntry.checkpointMark(),
                null,
                CheckpointEntryType.END, skipSync
            );

            wal.notchLastCheckpointPtr(chp.cpEntry.checkpointMark());
        }

        checkpointStorage.onCheckpointFinished(chp);

        DbCheckpointContextImpl ctx0 = new DbCheckpointContextImpl(
            chp.progress, new PartitionAllocationMap(), checkpointCollectInfoPool, () -> {
        }
        );

        List<DbCheckpointListener> dbLsnrs = new ArrayList<>(lsnrs);

        for (DbCheckpointListener lsnr : dbLsnrs)
            lsnr.afterCheckpointEnd(ctx0);

        ctx0.awaitPendingTasksFinished();

        if (chp.progress != null)
            chp.progress.transitTo(FINISHED);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void finalizeCheckpointOnRecovery(
        long cpTs,
        UUID cpId,
        WALPointer walPtr,
        StripedExecutor exec,
        CheckpointPageWriterFactory checkpointPageWriterFactory
    ) throws IgniteCheckedException {
        assert cpTs != 0;

        long start = System.currentTimeMillis();

        Collection<DataRegion> regions = dataRegions.get();

        CheckpointPagesInfoHolder cpPagesHolder = beginAllCheckpoints(new GridFinishedFuture<>());

        // Sort and split all dirty pages set to several stripes.
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> pages =
            splitAndSortCpPagesIfNeeded(cpPagesHolder);

        // Identity stores set for future fsync.
        Collection<PageStore> updStores = new GridConcurrentHashSet<>();

        AtomicInteger cpPagesCnt = new AtomicInteger();

        // Shared refernce for tracking exception during write pages.
        AtomicReference<Throwable> writePagesError = new AtomicReference<>();

        for (int stripeIdx = 0; stripeIdx < exec.stripes(); stripeIdx++)
            exec.execute(
                stripeIdx,
                checkpointPageWriterFactory.buildRecovery(pages, updStores, writePagesError, cpPagesCnt)
            );

        // Await completion all write tasks.
        awaitApplyComplete(exec, writePagesError);

        long written = U.currentTimeMillis();

        // Fsync all touched stores.
        for (PageStore updStore : updStores)
            updStore.sync();

        long fsync = U.currentTimeMillis();

        for (DataRegion memPlc : regions) {
            if (memPlc.config().isPersistenceEnabled())
                ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();
        }

        checkpointStorage.writeCheckpointEntry(cpTs, cpId, walPtr, null, CheckpointEntryType.END, skipSync);

        if (log.isInfoEnabled())
            log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                    "pagesWrite=%dms, fsync=%dms, total=%dms]",
                cpId,
                cpPagesCnt.get(),
                walPtr,
                written - start,
                fsync - written,
                fsync - start));
    }

    /**
     * @param exec Striped executor.
     * @param applyError Check error reference.
     */
    private void awaitApplyComplete(
        StripedExecutor exec,
        AtomicReference<Throwable> applyError
    ) throws IgniteCheckedException {
        try {
            // Await completion apply tasks in all stripes.
            exec.awaitComplete();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        // Checking error after all task applied.
        Throwable error = applyError.get();

        if (error != null)
            throw error instanceof IgniteCheckedException
                ? (IgniteCheckedException)error : new IgniteCheckedException(error);
    }

    /**
     * @param memoryRecoveryRecordPtr Memory recovery record pointer.
     */
    public void memoryRecoveryRecordPtr(WALPointer memoryRecoveryRecordPtr) {
        this.memoryRecoveryRecordPtr = memoryRecoveryRecordPtr;
    }

    /**
     * @param lsnr Listener.
     */
    public void addCheckpointListener(DbCheckpointListener lsnr) {
        lsnrs.add(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    public void removeCheckpointListener(DbCheckpointListener lsnr) {
        lsnrs.remove(lsnr);
    }

    /**
     * Stop any activity.
     */
    @SuppressWarnings("unused")
    public void stop() {
        IgniteThreadPoolExecutor pool = checkpointCollectInfoPool;

        if (pool != null) {
            pool.shutdownNow();

            try {
                pool.awaitTermination(2, TimeUnit.MINUTES);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }

            checkpointCollectInfoPool = null;
        }

        lsnrs.clear();
    }

    /**
     * Prepare all structure to further work. This object should be fully ready to work after call of this method.
     */
    public void start() {
        if (checkpointCollectInfoPool == null)
            checkpointCollectInfoPool = initializeCheckpointPool();
    }
}
