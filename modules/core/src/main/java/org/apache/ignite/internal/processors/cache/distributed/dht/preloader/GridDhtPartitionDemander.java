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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.WalStateManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.IgniteFeatures.TX_TRACKING_UPDATE_COUNTER;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceStatisticsUtils.availablePrintRebalanceStatistics;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceStatisticsUtils.cacheGroupRebalanceStatistics;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceStatisticsUtils.totalRebalanceStatistic;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;

/**
 * Thread pool for requesting partitions from other nodes and populating local cache.
 */
public class GridDhtPartitionDemander {
    /** */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final IgniteLogger log;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo> preloadPred;

    /** Future for preload mode {@link CacheRebalanceMode#SYNC}. */
    @GridToStringInclude
    private final GridFutureAdapter syncFut = new GridFutureAdapter();

    /** Rebalance future. */
    @GridToStringInclude
    private volatile RebalanceFuture rebalanceFut;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /** Total statistics of rebalance. */
    @GridToStringExclude
    @Nullable private final RebalanceStatistics totalRebStat;

    /**
     * @param grp Ccahe group.
     */
    public GridDhtPartitionDemander(CacheGroupContext grp) {
        assert grp != null;

        this.grp = grp;

        ctx = grp.shared();

        log = ctx.logger(getClass());

        boolean enabled = grp.rebalanceEnabled() && !ctx.kernalContext().clientNode();

        rebalanceFut = new RebalanceFuture(); //Dummy.

        if (!enabled) {
            // Calling onDone() immediately since preloading is disabled.
            rebalanceFut.onDone(true);
            syncFut.onDone();
        }

        totalRebStat = availablePrintRebalanceStatistics() ? new RebalanceStatistics() : null;
    }

    /**
     * Start.
     */
    void start() {
        // No-op.
    }

    /**
     * Stop.
     */
    void stop() {
        try {
            rebalanceFut.cancel();
        }
        catch (Exception ignored) {
            rebalanceFut.onDone(false);
        }

        lastExchangeFut = null;

        lastTimeoutObj.set(null);

        syncFut.onDone();
    }

    /**
     * @return Future for {@link CacheRebalanceMode#SYNC} mode.
     */
    IgniteInternalFuture<?> syncFuture() {
        return syncFut;
    }

    /**
     * @return Rebalance future.
     */
    IgniteInternalFuture<Boolean> rebalanceFuture() {
        return rebalanceFut;
    }

    /**
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * @return Rebalance future.
     */
    IgniteInternalFuture<Boolean> forceRebalance() {
        GridTimeoutObject obj = lastTimeoutObj.getAndSet(null);

        if (obj != null)
            ctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing rebalance event for future: " + exchFut);

            final GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    if (t.error() == null) {
                        IgniteInternalFuture<Boolean> fut0 = ctx.exchange().forceRebalance(exchFut.exchangeId());

                        fut0.listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                            @Override public void apply(IgniteInternalFuture<Boolean> fut1) {
                                try {
                                    fut.onDone(fut1.get());
                                } catch (Exception e) {
                                    fut.onDone(e);
                                }
                            }
                        });
                    }
                    else
                        fut.onDone(t.error());
                }
            });

            return fut;
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force rebalance request (no topology event happened yet).");

        return new GridFinishedFuture<>(true);
    }

    /**
     * Sets last exchange future.
     *
     * @param lastFut Last future to set.
     */
    void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {
        lastExchangeFut = lastFut;
    }

    /**
     * @return Collection of supplier nodes. Value {@code empty} means rebalance already finished.
     */
    Collection<UUID> remainingNodes() {
        return rebalanceFut.remainingNodes();
    }

    /**
     * This method initiates new rebalance process from given {@code assignments} by creating new rebalance
     * future based on them. Cancels previous rebalance future and sends rebalance started event.
     * In case of delayed rebalance method schedules the new one with configured delay based on {@code lastExchangeFut}.
     *
     * @param assignments Assignments to process.
     * @param force {@code True} if preload request by {@link ForceRebalanceExchangeTask}.
     * @param rebalanceId Rebalance id generated from exchange thread.
     * @param next A next rebalance routine in chain.
     * @param forcedRebFut External future for forced rebalance.
     * @param compatibleRebFut Future for waiting for compatible rebalances.
     *
     * @return Rebalancing future or {@code null} to exclude an assignment from a chain.
     */
    @Nullable RebalanceFuture addAssignments(
        final GridDhtPreloaderAssignments assignments,
        boolean force,
        long rebalanceId,
        final RebalanceFuture next,
        @Nullable final GridCompoundFuture<Boolean, Boolean> forcedRebFut,
        GridCompoundFuture<Boolean, Boolean> compatibleRebFut
    ) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assignments);

        assert force == (forcedRebFut != null);

        long delay = grp.config().getRebalanceDelay();

        if ((delay == 0 || force) && assignments != null) {
            final RebalanceFuture oldFut = rebalanceFut;

            if (assignments.cancelled()) { // Pending exchange.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to cancelled assignments.");

                return null;
            }

            if (assignments.isEmpty()) { // Nothing to rebalance.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to empty assignments.");

                if (oldFut.isInitial())
                    oldFut.onDone(true);
                else if (!oldFut.isDone())
                    oldFut.tryCancel();

                ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                return null;
            }

            // Check if ongoing rebalancing is compatible with a new assignment.
            if (!force && (!oldFut.isDone() || oldFut.result()) && oldFut.compatibleWith(assignments)) {
                if (!oldFut.isDone())
                    compatibleRebFut.add(oldFut);

                return null;
            }

            // Cancel ongoing rebalancing.
            if (!oldFut.isDone() && !oldFut.isInitial())
                oldFut.tryCancel();

            // Partition states cannot be changed from now on by previous incompatible rebalancing.
            // Retain only moving partitions. Assignment can become empty as a result.
            // Delayed partition owning happens in the exchange worker as well, so no race with delayed owning here.
            assignments.retainMoving(grp.topology());

            // Skip rebalanced group.
            if (assignments.isEmpty())
                return null;

            final RebalanceFuture fut = new RebalanceFuture(grp, lastExchangeFut, assignments, log, rebalanceId, next, oldFut);

            if (oldFut.isInitial())
                fut.listen(f -> oldFut.onDone(f.result()));

            // Make sure partitions scheduled for full rebalancing are cleared first.
            if (grp.persistenceEnabled()) {
                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assignments.entrySet()) {
                    for (Integer partId : e.getValue().partitions().fullSet()) {
                        GridDhtLocalPartition part = grp.topology().localPartition(partId);

                        assert part.state() == MOVING : part;

                        part.clearAsync();
                    }
                }
            }

            if (forcedRebFut != null)
                forcedRebFut.add(fut);

            rebalanceFut = fut;

            for (final GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.clearRebalanceCounters();

                    for (GridDhtPartitionDemandMessage msg : assignments.values()) {
                        for (Integer partId : msg.partitions().fullSet())
                            metrics.onRebalancingKeysCountEstimateReceived(grp.topology().globalPartSizes().get(partId));

                        CachePartitionPartialCountersMap histMap = msg.partitions().historicalMap();

                        for (int i = 0; i < histMap.size(); i++) {
                            long from = histMap.initialUpdateCounterAt(i);
                            long to = histMap.updateCounterAt(i);

                            metrics.onRebalancingKeysCountEstimateReceived(to - from);
                        }
                    }

                    metrics.startRebalance(0);
                }
            }

            fut.sendRebalanceStartedEvent();

            return fut;
        }
        else if (delay > 0) {
            for (GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.startRebalance(delay);
                }
            }

            GridTimeoutObject obj = lastTimeoutObj.get();

            if (obj != null)
                ctx.time().removeTimeoutObject(obj);

            final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying rebalance process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> f) {
                            ctx.exchange().forceRebalance(exchFut.exchangeId());
                        }
                    });
                }
            };

            lastTimeoutObj.set(obj);

            ctx.time().addTimeoutObject(obj);
        }

        return null;
    }

    /**
     * Enqueues supply message.
     *
     * @param supplyMsg Messqage.
     * @param r Runnable.
     */
    public void registerSupplyMessage(final GridDhtPartitionSupplyMessage supplyMsg, final Runnable r) {
        final RebalanceFuture fut = rebalanceFut;

        if (fut.isActual(supplyMsg.rebalanceId())) {
            for (Integer p : supplyMsg.infos().keySet())
                fut.queued.get(p).increment();

            ctx.kernalContext().getRebalanceExecutorService().execute(r);
        }
    }

    /**
     * Handles supply message, which contains entries to populate partitions.
     * <br/>
     * There is a cyclic process: <br/>
     * Populate rebalancing partitions with entries from supplier. <br/>
     * If not all partitions specified in {@link #rebalanceFut} were
     * rebalanced or marked as missed send new Demand message to request next
     * batch of entries.
     *
     * @param supplierNodeId Supplier node id.
     * @param supplyMsg Supply message.
     */
    public void handleSupplyMessage(
        final UUID supplierNodeId,
        final GridDhtPartitionSupplyMessage supplyMsg
    ) {
        AffinityTopologyVersion topVer = supplyMsg.topologyVersion();

        RebalanceFuture rebalanceFut = this.rebalanceFut;
        RebalanceStatistics rebalanceStat = rebalanceFut.stat;

        ClusterNode supplierNode = ctx.node(supplierNodeId);

        rebalanceFut.cancelLock.readLock().lock();

        try {
            String errMsg = null;

            if (rebalanceFut.isDone())
                errMsg = "rebalance completed";
            else if (isNull(supplierNode))
                errMsg = "supplier has left cluster";
            else if (!rebalanceFut.isActual(supplyMsg.rebalanceId()))
                errMsg = "topology changed";

            if (nonNull(errMsg)) {
                if (log.isDebugEnabled()) {
                    log.debug("Supply message has been ignored (" + errMsg + ") [" +
                        demandRoutineInfo(supplierNodeId, supplyMsg) + ']');
                }

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received supply message [" + demandRoutineInfo(supplierNodeId, supplyMsg) + ']');

            // Check whether there were error during supplying process.
            if (nonNull(supplyMsg.classError()))
                errMsg = supplyMsg.classError().getMessage();
            else if (nonNull(supplyMsg.error()))
                errMsg = supplyMsg.error().getMessage();

            if (nonNull(errMsg)) {
                U.warn(log, "Rebalancing routine has failed [" +
                    demandRoutineInfo(supplierNodeId, supplyMsg) + ", err=" + errMsg + ']');

                rebalanceFut.error(supplierNodeId);

                return;
            }

            if (grp.sharedGroup()) {
                for (GridCacheContext cctx : grp.caches()) {
                    if (cctx.statisticsEnabled()) {
                        long keysCnt = supplyMsg.keysForCache(cctx.cacheId());

                        if (keysCnt != -1)
                            cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(keysCnt);

                        // Can not be calculated per cache.
                        cctx.cache().metrics0().onRebalanceBatchReceived(supplyMsg.messageSize());
                    }
                }
            }
            else {
                GridCacheContext cctx = grp.singleCacheContext();

                if (cctx.statisticsEnabled()) {
                    if (supplyMsg.estimatedKeysCount() != -1)
                        cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(supplyMsg.estimatedKeysCount());

                    cctx.cache().metrics0().onRebalanceBatchReceived(supplyMsg.messageSize());
                }
            }

            GridDhtPartitionTopology top = grp.topology();

            try {
                AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

                //Counters of rebalanced entries and bytes.
                AtomicLong entryCnt = new AtomicLong();
                AtomicLong byteCnt = new AtomicLong();

                // Preload.
                for (Map.Entry<Integer, CacheEntryInfoCollection> e : supplyMsg.infos().entrySet()) {
                    int p = e.getKey();

                    if (aff.get(p).contains(ctx.localNode())) {
                        GridDhtLocalPartition part;

                        try {
                            part = top.localPartition(p, topVer, true);
                        }
                        catch (GridDhtInvalidPartitionException err) {
                            assert !topVer.equals(top.lastTopologyChangeVersion());

                            if (log.isDebugEnabled()) {
                                log.debug("Failed to get partition for rebalancing [" +
                                    "grp=" + grp.cacheOrGroupName() +
                                    ", err=" + err +
                                    ", p=" + p +
                                    ", topVer=" + topVer +
                                    ", lastTopVer=" + top.lastTopologyChangeVersion() + ']');
                            }

                            continue;
                        }

                        assert part != null;

                        boolean last = supplyMsg.last().containsKey(p);

                        if (part.state() == MOVING) {
                            boolean reserved = part.reserve();

                            assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                                ctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                            part.beforeApplyBatch(last);

                            try {
                                CacheEntryInfoCollection entryInfoCol = e.getValue();
                                Iterator<GridCacheEntryInfo> infos = entryInfoCol.infos().iterator();

                                entryCnt.set(0);
                                byteCnt.set(0);

                                if (grp.mvccEnabled())
                                    mvccPreloadEntries(topVer, supplierNode, p, infos, entryCnt, byteCnt);
                                else
                                    preloadEntries(topVer, supplierNode, p, infos, entryCnt, byteCnt);

                                if (nonNull(rebalanceStat)) {
                                    rebalanceStat.update(
                                        supplierNode,
                                        p,
                                        entryInfoCol.historical(),
                                        entryCnt.get(),
                                        byteCnt.get()
                                    );
                                }

                                rebalanceFut.processed.get(p).increment();

                                // If message was last for this partition,
                                // then we take ownership.
                                if (last)
                                    ownPartition(rebalanceFut, part, supplierNodeId, supplyMsg);
                            }
                            finally {
                                part.release();
                            }
                        }
                        else {
                            if (last)
                                rebalanceFut.partitionDone(supplierNodeId, p, false);

                            if (log.isDebugEnabled())
                                log.debug("Skipping rebalancing partition (state is not MOVING): " +
                                    "[" + demandRoutineInfo(supplierNodeId, supplyMsg) + ", p=" + p + ']');
                        }
                    }
                    else {
                        rebalanceFut.partitionDone(supplierNodeId, p, false);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (affinity changed): " +
                                "[" + demandRoutineInfo(supplierNodeId, supplyMsg) + ", p=" + p + ']');
                    }
                }

                // Only request partitions based on latest topology version.
                for (Integer miss : supplyMsg.missed()) {
                    if (aff.get(miss).contains(ctx.localNode()))
                        rebalanceFut.partitionMissed(supplierNodeId, miss);
                }

                for (Integer miss : supplyMsg.missed())
                    rebalanceFut.partitionDone(supplierNodeId, miss, false);

                GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                    supplyMsg.rebalanceId(),
                    supplyMsg.topologyVersion(),
                    grp.groupId());

                d.timeout(grp.preloader().timeout());

                if (!rebalanceFut.isDone()) {
                    // Send demand message.
                    try {
                        ctx.io().sendOrderedMessage(supplierNode, d.topic(),
                            d.convertIfNeeded(supplierNode.version()), grp.ioPolicy(), grp.preloader().timeout());

                        if (log.isDebugEnabled())
                            log.debug("Send next demand message [" +
                                demandRoutineInfo(supplierNodeId, supplyMsg) + "]");
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Supplier has left [" + demandRoutineInfo(supplierNodeId, supplyMsg) +
                                ", errMsg=" + e.getMessage() + ']');
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Will not request next demand message [" +
                            demandRoutineInfo(supplierNodeId, supplyMsg) + ", rebalanceFuture=" + rebalanceFut + ']');
                }
            }
            catch (IgniteSpiException | IgniteCheckedException e) {
                rebalanceFut.error(supplierNodeId);

                LT.error(log, e, "Error during rebalancing [" + demandRoutineInfo(supplierNodeId, supplyMsg) +
                    ", err=" + e + ']');
            }
        }
        finally {
            rebalanceFut.cancelLock.readLock().unlock();

            if (nonNull(rebalanceStat)) {
                if (nonNull(supplierNode))
                    rebalanceStat.end(supplierNode, U.currentTimeMillis());
                else
                    rebalanceStat.end(supplierNodeId, U.currentTimeMillis());
            }
        }
    }

    /**
     * @param fut Future.
     * @param part Partition.
     * @param nodeId Node id.
     * @param supplyMsg Supply message.
     */
    private void ownPartition(
        final RebalanceFuture fut,
        GridDhtLocalPartition part,
        final UUID nodeId,
        final GridDhtPartitionSupplyMessage supplyMsg
    ) {
        fut.cancelLock.readLock().lock();

        try {
            if (!fut.isActual(supplyMsg.rebalanceId()))
                return;

            int id = part.id();

            long queued = fut.queued.get(id).sum();
            long processed = fut.processed.get(id).sum();

            if (processed == queued) {
                if (ctx.kernalContext().txDr().shouldApplyUpdateCounterOnRebalance())
                    part.updateCounter(supplyMsg.last().get(id));

                fut.partitionDone(nodeId, id, true);

                if (log.isDebugEnabled())
                    log.debug("Finished rebalancing partition: " +
                        "[" + demandRoutineInfo(nodeId, supplyMsg) + ", id=" + id + "]");
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Retrying partition owning: " +
                        "[" + demandRoutineInfo(nodeId, supplyMsg) + ", id=" + id +
                        ", processed=" + processed + ", queued=" + queued + "]");

                ctx.kernalContext().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(100) {
                    @Override public void onTimeout() {
                        ctx.kernalContext().getRebalanceExecutorService().execute(() ->
                            ownPartition(fut, part, nodeId, supplyMsg));
                    }
                });
            }
        }
        finally {
            fut.cancelLock.readLock().unlock();
        }
    }

    /**
     * Adds mvcc entries with theirs history to partition p.
     *
     * @param supplierNode Node which sent entry.
     * @param p Partition id.
     * @param infos Entries info for preload.
     * @param topVer Topology version.
     * @param entryCnt Counter of rebalanced entries.
     * @param byteCnt Counter of rebalanced bytes.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void mvccPreloadEntries(
        AffinityTopologyVersion topVer,
        ClusterNode supplierNode,
        int p,
        Iterator<GridCacheEntryInfo> infos,
        AtomicLong entryCnt,
        AtomicLong byteCnt
    ) throws IgniteCheckedException {
        if (!infos.hasNext())
            return;

        List<GridCacheMvccEntryInfo> entryHist = new ArrayList<>();

        GridCacheContext cctx = grp.sharedGroup() ? null : grp.singleCacheContext();

        // Loop through all received entries and try to preload them.
        while (infos.hasNext() || !entryHist.isEmpty()) {
            ctx.database().checkpointReadLock();

            try {
                for (int i = 0; i < 100; i++) {
                    boolean hasMore = infos.hasNext();

                    assert hasMore || !entryHist.isEmpty();

                    GridCacheMvccEntryInfo entry = null;

                    boolean flushHistory;

                    if (hasMore) {
                        entry = (GridCacheMvccEntryInfo)infos.next();

                        GridCacheMvccEntryInfo prev = entryHist.isEmpty() ? null : entryHist.get(0);

                        flushHistory = prev != null && ((grp.sharedGroup() && prev.cacheId() != entry.cacheId())
                            || !prev.key().equals(entry.key()));
                    }
                    else
                        flushHistory = true;

                    if (flushHistory) {
                        assert !entryHist.isEmpty();

                        int cacheId = entryHist.get(0).cacheId();

                        if (grp.sharedGroup() && (cctx == null || cacheId != cctx.cacheId())) {
                            assert cacheId != CU.UNDEFINED_CACHE_ID;

                            cctx = grp.shared().cacheContext(cacheId);
                        }

                        if (cctx != null) {
                            if (!mvccPreloadEntry(cctx, supplierNode, entryHist, topVer, p)) {
                                if (log.isTraceEnabled())
                                    log.trace("Got entries for invalid partition during " +
                                        "preloading (will skip) [p=" + p +
                                        ", entry=" + entryHist.get(entryHist.size() - 1) + ']');

                                return; // Skip current partition.
                            }

                            entryCnt.incrementAndGet();

                            for (GridCacheMvccEntryInfo entryInfo : entryHist)
                                byteCnt.addAndGet(entryInfo.marshalledSize(cctx.cacheObjectContext()));

                            //TODO: IGNITE-11330: Update metrics for touched cache only.
                            for (GridCacheContext ctx : grp.caches()) {
                                if (ctx.statisticsEnabled())
                                    ctx.cache().metrics0().onRebalanceKeyReceived();
                            }
                        }

                        if (!hasMore)
                            return;

                        entryHist.clear();
                    }

                    entryHist.add(entry);
                }
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Adds entries with theirs history to partition p.
     *
     * @param supplierNode Node which sent entry.
     * @param p Partition id.
     * @param infos Entries info for preload.
     * @param topVer Topology version.
     * @param entryCnt Counter of rebalanced entries.
     * @param byteCnt Counter of rebalanced bytes.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void preloadEntries(
        AffinityTopologyVersion topVer,
        ClusterNode supplierNode,
        int p,
        Iterator<GridCacheEntryInfo> infos,
        AtomicLong entryCnt,
        AtomicLong byteCnt
    ) throws IgniteCheckedException {
        GridCacheContext cctx = null;

        // Loop through all received entries and try to preload them.
        while (infos.hasNext()) {
            ctx.database().checkpointReadLock();

            try {
                for (int i = 0; i < 100; i++) {
                    if (!infos.hasNext())
                        break;

                    GridCacheEntryInfo entry = infos.next();

                    if (cctx == null || (grp.sharedGroup() && entry.cacheId() != cctx.cacheId())) {
                        cctx = grp.sharedGroup() ? grp.shared().cacheContext(entry.cacheId()) : grp.singleCacheContext();

                        if (cctx == null)
                            continue;
                        else if (cctx.isNear())
                            cctx = cctx.dhtCache().context();
                    }

                    if (!preloadEntry(supplierNode, p, entry, topVer, cctx)) {
                        if (log.isTraceEnabled())
                            log.trace("Got entries for invalid partition during " +
                                "preloading (will skip) [p=" + p + ", entry=" + entry + ']');

                        return;
                    }

                    entryCnt.incrementAndGet();
                    byteCnt.addAndGet(entry.marshalledSize(cctx.cacheObjectContext()));

                    //TODO: IGNITE-11330: Update metrics for touched cache only.
                    for (GridCacheContext ctx : grp.caches()) {
                        if (ctx.statisticsEnabled())
                            ctx.cache().metrics0().onRebalanceKeyReceived();
                    }
                }
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Adds {@code entry} to partition {@code p}.
     *
     * @param from Node which sent entry.
     * @param p Partition id.
     * @param entry Preloaded entry.
     * @param topVer Topology version.
     * @param cctx Cache context.
     * @return {@code False} if partition has become invalid during preloading.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean preloadEntry(
        ClusterNode from,
        int p,
        GridCacheEntryInfo entry,
        AffinityTopologyVersion topVer,
        GridCacheContext cctx
    ) throws IgniteCheckedException {
        assert ctx.database().checkpointLockIsHeldByThread();

        try {
            GridCacheEntryEx cached = null;

            while (true) {
                try {
                    cached = cctx.cache().entryEx(entry.key(), topVer);

                    if (log.isTraceEnabled()) {
                        log.trace("Rebalancing key [key=" + entry.key() + ", part=" + p + ", fromNode=" +
                            from.id() + ", grpId=" + grp.groupId() + ']');
                    }

                    if (preloadPred == null || preloadPred.apply(entry)) {
                        if (cached.initialValue(
                            entry.value(),
                            entry.version(),
                            cctx.mvccEnabled() ? ((MvccVersionAware)entry).mvccVersion() : null,
                            cctx.mvccEnabled() ? ((MvccUpdateVersionAware)entry).newMvccVersion() : null,
                            cctx.mvccEnabled() ? ((MvccVersionAware)entry).mvccTxState() : TxState.NA,
                            cctx.mvccEnabled() ? ((MvccUpdateVersionAware)entry).newMvccTxState() : TxState.NA,
                            entry.ttl(),
                            entry.expireTime(),
                            true,
                            topVer,
                            cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE,
                            false
                        )) {
                            cached.touch(); // Start tracking.

                            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                                cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(), null,
                                    null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, entry.value(), true, null,
                                    false, null, null, null, true);
                        }
                        else {
                            cached.touch(); // Start tracking.

                            if (log.isTraceEnabled())
                                log.trace("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                                    ", part=" + p + ']');
                        }
                    }
                    else if (log.isTraceEnabled())
                        log.trace("Rebalance predicate evaluated to false for entry (will ignore): " + entry);

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isTraceEnabled())
                        log.trace("Entry has been concurrently removed while rebalancing (will retry) [key=" +
                            cached.key() + ", part=" + p + ']');
                }
                catch (GridDhtInvalidPartitionException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Partition became invalid during rebalancing (will ignore): " + p);

                    return false;
                }
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
                ctx.localNode() + ", node=" + from.id() + ", key=" + entry.key() + ", part=" + p + ']', e);
        }

        return true;
    }

    /**
     * Adds mvcc {@code entry} with it's history to partition {@code p}.
     *
     * @param cctx Cache context.
     * @param from Node which sent entry.
     * @param history Mvcc entry history.
     * @param topVer Topology version.
     * @param p Partition id.
     * @return {@code False} if partition has become invalid during preloading.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean mvccPreloadEntry(
        GridCacheContext cctx,
        ClusterNode from,
        List<GridCacheMvccEntryInfo> history,
        AffinityTopologyVersion topVer,
        int p
    ) throws IgniteCheckedException {
        assert ctx.database().checkpointLockIsHeldByThread();
        assert !history.isEmpty();

        GridCacheMvccEntryInfo info = history.get(0);

        assert info.key() != null;

        try {
            GridCacheEntryEx cached = null;

            try {
                cached = cctx.cache().entryEx(info.key(), topVer);

                if (log.isTraceEnabled())
                    log.trace("Rebalancing key [key=" + info.key() + ", part=" + p + ", node=" + from.id() + ']');

                if (cached.mvccPreloadEntry(history)) {
                    cached.touch(); // Start tracking.

                    if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                        cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(), null,
                            null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, null, true, null,
                            false, null, null, null, true);
                }
                else {
                    cached.touch(); // Start tracking.

                    if (log.isTraceEnabled())
                        log.trace("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                            ", part=" + p + ']');
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isTraceEnabled())
                    log.trace("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                        cached.key() + ", part=" + p + ']');
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition became invalid during rebalancing (will ignore): " + p);

                return false;
            }
        }
        catch (IgniteInterruptedCheckedException | ClusterTopologyCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
                ctx.localNode() + ", node=" + from.id() + ", key=" + info.key() + ", part=" + p + ']', e);
        }

        return true;
    }

    /**
     * String representation of demand routine.
     *
     * @param supplier Supplier.
     * @param supplyMsg Supply message.
     */
    private String demandRoutineInfo(UUID supplier, GridDhtPartitionSupplyMessage supplyMsg) {
        return "grp=" + grp.cacheOrGroupName() + ", topVer=" + supplyMsg.topologyVersion() + ", supplier=" + supplier;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemander.class, this);
    }

    /**
     * Internal states of rebalance future.
     */
    private enum RebalanceFutureState {
        /** Initial state. */
        INIT,

        /** Rebalance future started and requested required partitions. */
        STARTED,

        /** Marked as cancelled. This means partitions will not be requested. */
        MARK_CANCELLED
    }

    /**
     * The future is created for each topology version if some partitions should present by affinity and completed when
     * all partitions are transferred.
     * <p>
     * As soon as a partition was successfully preloaded it's state is switched to OWNING, making it consistent with
     * other copies.
     * <p>
     * To speed up things WAL can be locally disabled until preloading is finished (causing temporary durability loss
     * for a group) , in such a case partitions are owned after a checkpoint has completed.
     * Applicable only for persistent mode.
     *
     * Possible outcomes are:
     * <ul>
     *     <li>{@code True} if a group was fully rebalanced (but some suppliers possibly have failed to provide data
     *     due to unrecoverable error). This triggers completion of sync future.</li>
     *     <li>{@code False} if a group rebalancing was cancelled because topology has changed and new assignment is
     *     incompatible with previous, see {@link RebalanceFuture#compatibleWith(GridDhtPreloaderAssignments)}.</li>
     * </ul>
     */
    public static class RebalanceFuture extends GridFutureAdapter<Boolean> {
        /** State updater. */
        private static final AtomicReferenceFieldUpdater<RebalanceFuture, RebalanceFutureState> STATE_UPD =
            AtomicReferenceFieldUpdater.newUpdater(RebalanceFuture.class, RebalanceFutureState.class, "state");

        /** */
        private final GridCacheSharedContext<?, ?> ctx;

        /** Internal state. */
        @GridToStringExclude
        private volatile RebalanceFutureState state = RebalanceFutureState.INIT;

        /** */
        private final CacheGroupContext grp;

        /** */
        private final IgniteLogger log;

        /** Remaining. */
        private final Map<UUID, IgniteDhtDemandedPartitionsMap> remaining = new HashMap<>();

        /** Collection of missed partitions and partitions that could not be rebalanced from a supplier. */
        private final Map<UUID, Collection<Integer>> missed = new HashMap<>();

        /** Exchange ID. */
        @GridToStringExclude
        private final GridDhtPartitionExchangeId exchId;

        /** Coresponding exchange future. */
        @GridToStringExclude
        private final GridDhtPartitionsExchangeFuture exchFut;

        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Unique (per demander) rebalance id. */
        private final long rebalanceId;

        /** The number of rebalance routines. */
        private final long routines;

        /** Used to order rebalance cancellation and supply message processing, they should not overlap.
         * Otherwise partition clearing could start on still rebalancing partition resulting in eviction of
         * partition in OWNING state. */
        private final ReentrantReadWriteLock cancelLock;

        /** Rebalance statistics. */
        @GridToStringExclude
        @Nullable final RebalanceStatistics stat;

        /** Entries batches queued. */
        private final Map<Integer/* Partition id. */, LongAdder/* Batch count. */> queued = new HashMap<>();

        /** Entries batches processed. */
        private final Map<Integer, LongAdder> processed = new HashMap<>();

        /** Historical rebalance set. */
        private final Set<Integer> historical = new HashSet<>();

        /** Next future in chain. */
        @GridToStringExclude
        private final RebalanceFuture next;

        /** Assigment. */
        private final GridDhtPreloaderAssignments assignments;

        /** Partitions which have been scheduled for rebalance from specific supplier. */
        private final Map<ClusterNode, Set<Integer>> rebalancingParts;

        /**
         * Constructor.
         *
         * @param grp Cache group context.
         * @param exchFut Exchange future.
         * @param assignments Assignments.
         * @param log Logger.
         * @param rebalanceId Rebalance id.
         * @param next Next rebalance future.
         * @param previous Previous rebalance future.
         */
        RebalanceFuture(
            CacheGroupContext grp,
            GridDhtPartitionsExchangeFuture exchFut,
            GridDhtPreloaderAssignments assignments,
            IgniteLogger log,
            long rebalanceId,
            RebalanceFuture next,
            RebalanceFuture previous
        ) {
            assert assignments != null : "Asiignments must not be null.";

            this.rebalancingParts = U.newHashMap(assignments.size());
            this.assignments = assignments;
            exchId = assignments.exchangeId();
            topVer = assignments.topologyVersion();
            this.exchFut = exchFut;
            this.next = next;

            assignments.forEach((k, v) -> {
                assert v.partitions() != null :
                    "Partitions are null [grp=" + grp.cacheOrGroupName() + ", fromNode=" + k.id() + "]";

                remaining.put(k.id(), v.partitions());

                rebalancingParts.put(k, new HashSet<Integer>() {{
                    addAll(v.partitions().historicalSet());
                    addAll(v.partitions().fullSet());
                }});

                historical.addAll(v.partitions().historicalSet());

                Stream.concat(v.partitions().historicalSet().stream(), v.partitions().fullSet().stream())
                    .forEach(
                        p -> {
                            queued.put(p, new LongAdder());
                            processed.put(p, new LongAdder());
                        });
            });

            this.routines = remaining.size();

            this.grp = grp;
            this.log = log;
            this.rebalanceId = rebalanceId;

            ctx = grp.shared();

            cancelLock = new ReentrantReadWriteLock();

            if (!availablePrintRebalanceStatistics())
                stat = null;
            else {
                RebalanceStatistics prevStat = previous.stat;
                stat = new RebalanceStatistics(nonNull(prevStat) ? prevStat.attempt() + 1 : 1);
            }
        }

        /**
         * Dummy future. Will be done by real one.
         */
        RebalanceFuture() {
            this.rebalancingParts = null;
            this.assignments = null;
            this.exchId = null;
            this.topVer = null;
            this.exchFut = null;
            this.ctx = null;
            this.grp = null;
            this.log = null;
            this.rebalanceId = -1;
            this.routines = 0;
            this.cancelLock = new ReentrantReadWriteLock();
            this.next = null;
            this.stat = null;
        }

        /**
         * Asynchronously sends initial demand messages formed from {@code assignments} and initiates supply-demand
         * processes.
         *
         * For each node participating in rebalance process method distributes set of partitions for that node to
         * several stripes (topics). It means that each stripe containing a subset of partitions can be processed in
         * parallel. The number of stripes are controlled by {@link IgniteConfiguration#getRebalanceThreadPoolSize()}
         * property.
         *
         * Partitions that can be rebalanced using only WAL are called historical, others are called full.
         *
         * Before sending messages, method awaits partitions clearing for full partitions.
         */
        public void requestPartitions() {
            if (availablePrintRebalanceStatistics())
                stat.start(U.currentTimeMillis());

            if (!STATE_UPD.compareAndSet(this, RebalanceFutureState.INIT, RebalanceFutureState.STARTED)) {
                cancel();

                return;
            }

            if (!ctx.kernalContext().grid().isRebalanceEnabled()) {
                if (log.isTraceEnabled())
                    log.trace("Cancel partition demand because rebalance disabled on current node.");

                cancel();

                return;
            }

            if (isDone()) {
                assert !result() : "Rebalance future was done, but partitions never requested [grp="
                    + grp.cacheOrGroupName() + ", topVer=" + topVer + "]";

                return;
            }

            final CacheConfiguration cfg = grp.config();

            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assignments.entrySet()) {
                ClusterNode supplierNode = e.getKey();

                GridDhtPartitionDemandMessage d = e.getValue();

                final IgniteDhtDemandedPartitionsMap parts;

                synchronized (this) { // Synchronized to prevent consistency issues in case of parallel cancellation.
                    if (isDone())
                        return;

                    parts = remaining.get(supplierNode.id());
                }

                U.log(log, "Prepared rebalancing [grp=" + grp.cacheOrGroupName()
                    + ", mode=" + cfg.getRebalanceMode() + ", supplier=" + supplierNode.id() +
                    ", partitionsCount=" + parts.size() + ", topVer=" + topVer + "]");

                if (!parts.isEmpty()) {
                    d.rebalanceId(rebalanceId);
                    d.timeout(grp.preloader().timeout());

                    IgniteInternalFuture<?> clearAllFut = clearFullPartitions(d.partitions().fullSet());

                    // Start rebalancing after clearing full partitions is finished.
                    clearAllFut.listen(f -> ctx.kernalContext().closure().runLocalSafe(() -> {
                        if (isDone())
                            return;

                        try {
                            if (log.isInfoEnabled())
                                log.info("Starting rebalance routine [" + grp.cacheOrGroupName() +
                                    ", topVer=" + topVer +
                                    ", supplier=" + supplierNode.id() +
                                    ", fullPartitions=" + S.compact(parts.fullSet()) +
                                    ", histPartitions=" + S.compact(parts.historicalSet()) + "]");

                            if (availablePrintRebalanceStatistics())
                                stat.start(supplierNode, U.currentTimeMillis());

                            ctx.io().sendOrderedMessage(supplierNode, d.topic(),
                                d.convertIfNeeded(supplierNode.version()), grp.ioPolicy(), d.timeout());

                            // Cleanup required in case partitions demanded in parallel with cancellation.
                            synchronized (this) {
                                if (isDone())
                                    cleanupRemoteContexts(supplierNode.id());
                            }
                        }
                        catch (IgniteCheckedException e1) {
                            ClusterTopologyCheckedException cause = e1.getCause(ClusterTopologyCheckedException.class);

                            if (cause != null)
                                log.warning("Failed to send initial demand request to node. " + e1.getMessage());
                            else
                                log.error("Failed to send initial demand request to node.", e1);

                            cancel();
                        }
                        catch (Throwable th) {
                            log.error("Runtime error caught during initial demand request sending.", th);

                            cancel();
                        }
                    }, true));
                }
            }
        }

        /**
         * Creates future which will be completed when all {@code fullPartitions} are cleared.
         *
         * @param fullPartitions Set of full partitions need to be cleared.
         * @return Future which will be completed when given partitions are cleared.
         */
        private IgniteInternalFuture<?> clearFullPartitions(Set<Integer> fullPartitions) {
            final GridFutureAdapter clearAllFuture = new GridFutureAdapter();

            if (fullPartitions.isEmpty()) {
                clearAllFuture.onDone();

                return clearAllFuture;
            }

            for (GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.rebalanceClearingPartitions(fullPartitions.size());
                }
            }

            final AtomicInteger clearingPartitions = new AtomicInteger(fullPartitions.size());

            for (int partId : fullPartitions) {
                if (isDone()) {
                    clearAllFuture.onDone();

                    return clearAllFuture;
                }

                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                if (part != null && part.state() == MOVING) {
                    part.onClearFinished(f -> {
                        if (!isDone()) {
                            // Cancel rebalance if partition clearing was failed.
                            if (f.error() != null) {
                                for (GridCacheContext cctx : grp.caches()) {
                                    if (cctx.statisticsEnabled()) {
                                        final CacheMetricsImpl metrics = cctx.cache().metrics0();

                                        metrics.rebalanceClearingPartitions(0);
                                    }
                                }

                                log.error("Unable to await partition clearing " + part, f.error());

                                cancel();

                                clearAllFuture.onDone(f.error());
                            }
                            else {
                                int remaining = clearingPartitions.decrementAndGet();

                                for (GridCacheContext cctx : grp.caches()) {
                                    if (cctx.statisticsEnabled()) {
                                        final CacheMetricsImpl metrics = cctx.cache().metrics0();

                                        metrics.rebalanceClearingPartitions(remaining);
                                    }
                                }

                                if (log.isDebugEnabled())
                                    log.debug("Partition is ready for rebalance [grp=" + grp.cacheOrGroupName()
                                        + ", p=" + part.id() + ", remaining=" + remaining + "]");

                                if (remaining == 0)
                                    clearAllFuture.onDone();
                            }
                        }
                        else
                            clearAllFuture.onDone();
                    });
                }
                else {
                    int remaining = clearingPartitions.decrementAndGet();

                    for (GridCacheContext cctx : grp.caches()) {
                        if (cctx.statisticsEnabled()) {
                            final CacheMetricsImpl metrics = cctx.cache().metrics0();

                            metrics.rebalanceClearingPartitions(remaining);
                        }
                    }

                    if (remaining == 0)
                        clearAllFuture.onDone();
                }
            }

            return clearAllFuture;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
            if (availablePrintRebalanceStatistics() && !isInitial()) {
                // Avoid race with next rebalancing.
                listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                        printRebalanceStatistics();
                    }
                });
            }

            if (super.onDone(res, err)) {
                if (!isInitial()) {
                    sendRebalanceFinishedEvent();

                    if (log.isInfoEnabled())
                        log.info("Completed rebalance future: " + this);

                    // Complete sync future only if rebalancing was not cancelled.
                    if (res && !grp.preloader().syncFuture().isDone())
                        ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();
                }

                if (next != null)
                    next.requestPartitions(); // Process next group.

                return true;
            }

            return false;
        }

        /**
         * @param topVer Rebalancing topology version.
         */
        public void ownPartitionsAndFinishFuture(AffinityTopologyVersion topVer) {
            // Ignore all client exchanges.
            // Note rebalancing may be started on client topology version if forced reassign was queued after client
            // topology exchange.
            AffinityTopologyVersion rebTopVer = ctx.exchange().lastAffinityChangedTopologyVersion(topVer);
            AffinityTopologyVersion curRebTopVer = ctx.exchange().lastAffinityChangedTopologyVersion(topologyVersion());

            if (!rebTopVer.equals(curRebTopVer)) {
                if (log.isDebugEnabled()) {
                    log.debug("Do not own partitions because the topology is outdated [grp=" +
                        grp.cacheOrGroupName() + ", topVer=" + topVer + ", curTopVer=" + topologyVersion()
                        + ", rebTopVer=" + rebTopVer + ", curRebTopVer=" + curRebTopVer + ']');
                }

                return;
            }

            assert state == RebalanceFutureState.STARTED : this;

            if (onDone(true, null)) {
                grp.localWalEnabled(true, true);

                // Safe to own from exchange worker thread because moving partitions from new assignments
                // cannot appear.
                grp.topology().ownMoving();

                if (log.isDebugEnabled())
                    log.debug("Partitions have been scheduled to resend [reason=" +
                        "Group durability restored, name=" + grp.cacheOrGroupName() + "]");

                ctx.exchange().refreshPartitions(Collections.singleton(grp));
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Do not own partitions because the future has been finished [grp=" +
                        grp.cacheOrGroupName() + ", ver=" + this.topVer + ", result=" + result() + ']');
            }
        }

        /**
         * @return Topology version.
         */
        public AffinityTopologyVersion topologyVersion() {
            return topVer;
        }

        /**
         * @param rebalanceId Rebalance id.
         * @return true in case future created for specified {@code rebalanceId}, false in other case.
         */
        private boolean isActual(long rebalanceId) {
            return this.rebalanceId == rebalanceId && !isDone();
        }

        /**
         * @return Is initial (created at demander creation).
         */
        public boolean isInitial() {
            return topVer == null;
        }

        /**
         * Cancel running future or mark for cancel {@code RebalanceFutureState#MARK_CANCELLED}.
         */
        private void tryCancel() {
            if (STATE_UPD.compareAndSet(this, RebalanceFutureState.INIT, RebalanceFutureState.MARK_CANCELLED))
                return;

            cancel();
        }

        /**
         * Cancels this future and proceeds to a next in the chain.
         *
         * @return {@code True}.
         */
        @Override public boolean cancel() {
            // Cancel lock is needed only for case when some message might be on the fly while rebalancing is
            // cancelled.
            cancelLock.writeLock().lock();

            try {
                synchronized (this) {
                    if (isDone())
                        return true;

                    U.log(log, "Cancelled rebalancing from all nodes [grp=" + grp.cacheOrGroupName() +
                        ", topVer=" + topologyVersion() + "]");

                    if (!ctx.kernalContext().isStopping()) {
                        for (UUID nodeId : remaining.keySet())
                            cleanupRemoteContexts(nodeId);
                    }

                    remaining.clear();

                    checkIsDone(true /* cancelled */);
                }

                return true;
            }
            finally {
                cancelLock.writeLock().unlock();
            }
        }

        /**
         * @param nodeId Node id.
         */
        private synchronized void error(UUID nodeId) {
            if (isDone())
                return;

            cleanupRemoteContexts(nodeId);

            remaining.remove(nodeId);

            checkIsDone(false);
        }

        /**
         * @param nodeId Node id.
         * @param p Partition id.
         */
        private synchronized void partitionMissed(UUID nodeId, int p) {
            if (isDone())
                return;

            IgniteDhtDemandedPartitionsMap parts = remaining.get(nodeId);

            assert parts != null : "Remaining not found [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                ", part=" + p + "]";

            if (parts.historicalMap().contains(p)) {
                // The partition p cannot be historically rebalanced,
                // let's exclude the given nodeId and give a try to full rebalance.
                exchFut.markNodeAsInapplicableForHistoricalRebalance(nodeId);
            }
            else
                exchFut.markNodeAsInapplicableForFullRebalance(nodeId, grp.groupId(), p);

            missed.computeIfAbsent(nodeId, k -> new HashSet<>());

            missed.get(nodeId).add(p);
        }

        /**
         * @param nodeId Node id.
         */
        private void cleanupRemoteContexts(UUID nodeId) {
            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null)
                return;

            GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                // Negative number of id signals that supply context
                // with the same positive id must be cleaned up at the supply node.
                -rebalanceId,
                this.topologyVersion(),
                grp.groupId());

            d.timeout(grp.preloader().timeout());

            try {
                Object rebalanceTopic = GridCachePartitionExchangeManager.rebalanceTopic(0);

                ctx.io().sendOrderedMessage(node, rebalanceTopic,
                    d.convertIfNeeded(node.version()), grp.ioPolicy(), grp.preloader().timeout());
            }
            catch (IgniteCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send failover context cleanup request to node " + nodeId);
            }
        }

        /**
         * @param nodeId Node id.
         * @param p Partition number.
         * @param own {@code True} to own partition if possible.
         */
        private synchronized void partitionDone(UUID nodeId, int p, boolean own) {
            if (own && grp.localWalEnabled())
                grp.topology().own(grp.topology().localPartition(p));

            if (isDone())
                return;

            if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                rebalanceEvent(p, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());

            IgniteDhtDemandedPartitionsMap parts = remaining.get(nodeId);

            assert parts != null : "Remaining not found [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                ", part=" + p + "]";

            boolean rmvd = parts.remove(p);

            assert rmvd : "Partition already done [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                ", part=" + p + ", left=" + parts + "]";

            if (parts.isEmpty()) {
                int remainingRoutines = remaining.size() - 1;

                U.log(log, "Completed " + ((remainingRoutines == 0 ? "(final) " : "") +
                    "rebalancing [grp=" + grp.cacheOrGroupName() +
                    ", supplier=" + nodeId +
                    ", topVer=" + topologyVersion() +
                    ", progress=" + (routines - remainingRoutines) + "/" + routines + "]"));

                remaining.remove(nodeId);
            }

            checkIsDone(false);
        }

        /**
         * @param part Partition.
         * @param type Type.
         * @param discoEvt Discovery event.
         */
        private void rebalanceEvent(int part, int type, DiscoveryEvent discoEvt) {
            assert discoEvt != null;

            grp.addRebalanceEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
        }

        /**
         * @param type Type.
         * @param discoEvt Discovery event.
         */
        private void rebalanceEvent(int type, DiscoveryEvent discoEvt) {
            rebalanceEvent(-1, type, discoEvt);
        }

        /**
         * @param cancelled Is cancelled.
         */
        private void checkIsDone(boolean cancelled) {
            if (remaining.isEmpty()) {
                Collection<Integer> m = new HashSet<>();

                for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                    if (e.getValue() != null && !e.getValue().isEmpty())
                        m.addAll(e.getValue());
                }

                if (!m.isEmpty()) {
                    U.log(log, "Reassigning partitions that were missed [parts=" + m +
                        ", grpId=" + grp.groupId() +
                        ", grpName=" + grp.cacheOrGroupName() +
                        ", topVer=" + topVer + ']');

                    onDone(false); // Finished but has missed partitions, will force dummy exchange

                    // Forced reassigning will cancel all pending rebalance futures.
                    ctx.exchange().forceReassign(exchId, exchFut);

                    return;
                }

                // Delay owning until checkpoint is finished.
                if (grp.persistenceEnabled() && !grp.localWalEnabled() && !cancelled) {
                    if (log.isDebugEnabled()) {
                        log.debug("Delaying partition owning for a group [name=" +
                            grp.cacheOrGroupName() + ", ver=" + topVer + ']');
                    }

                    // Force new checkpoint to make sure owning state is captured.
                    CheckpointProgress cp = ctx.database().forceCheckpoint(WalStateManager.reason(grp.groupId(), topVer));

                    cp.onStateChanged(PAGE_SNAPSHOT_TAKEN, () -> grp.localWalEnabled(true, false));

                    cp.onStateChanged(FINISHED, () -> {
                        ctx.exchange().finishPreloading(topVer, grp.groupId());
                    });
                }
                else {
                    onDone(!cancelled);

                    if (log.isDebugEnabled())
                        log.debug("Partitions have been scheduled to resend [reason=" +
                            "Rebalance is done, grp=" + grp.cacheOrGroupName() + "]");

                    // A group can be partially rebalanced even if rebalancing was cancelled.
                    ctx.exchange().refreshPartitions(Collections.singleton(grp));
                }
            }
        }

        /**
         * @return Collection of supplier nodes. Value {@code empty} means rebalance already finished.
         */
        private synchronized Collection<UUID> remainingNodes() {
            return remaining.keySet();
        }

        /**
         *
         */
        private void sendRebalanceStartedEvent() {
            if (grp.eventRecordable(EVT_CACHE_REBALANCE_STARTED))
                rebalanceEvent(EVT_CACHE_REBALANCE_STARTED, exchId.discoveryEvent());
        }

        /**
         *
         */
        private void sendRebalanceFinishedEvent() {
            if (grp.eventRecordable(EVT_CACHE_REBALANCE_STOPPED))
                rebalanceEvent(EVT_CACHE_REBALANCE_STOPPED, exchId.discoveryEvent());
        }

        /**
         * @param newAssignments New assignments.
         *
         * @return {@code True} when assignments are compatible and future should not be cancelled.
         */
        public boolean compatibleWith(GridDhtPreloaderAssignments newAssignments) {
            if (isInitial() || !allNodesSupports(ctx.kernalContext(), newAssignments.keySet(), TX_TRACKING_UPDATE_COUNTER)
                || ((GridDhtPreloader)grp.preloader()).disableRebalancingCancellationOptimization())
                return false;

            if (ctx.exchange().lastAffinityChangedTopologyVersion(topVer).equals(
                ctx.exchange().lastAffinityChangedTopologyVersion(newAssignments.topologyVersion()))) {
                if (log.isDebugEnabled())
                    log.debug("Rebalancing is forced on the same topology [grp="
                        + grp.cacheOrGroupName() + ", " + "top=" + topVer + ']');

                return false;
            }

            if (newAssignments.affinityReassign()) {
                if (log.isDebugEnabled())
                    log.debug("Some of owned partitions were reassigned by coordinator [grp="
                        + grp.cacheOrGroupName() + ", " + ", init=" + topVer +
                        ", other=" + newAssignments.topologyVersion() + ']');

                return false;
            }

            Set<Integer> p0 = new HashSet<>();
            Set<Integer> p1 = new HashSet<>();

            // Not compatible if a supplier has left.
            for (ClusterNode node : rebalancingParts.keySet()) {
                if (!grp.cacheObjectContext().kernalContext().discovery().alive(node))
                    return false;
            }

            for (Set<Integer> partitions : rebalancingParts.values())
                p0.addAll(partitions);

            for (GridDhtPartitionDemandMessage message : newAssignments.values()) {
                p1.addAll(message.partitions().fullSet());
                p1.addAll(message.partitions().historicalSet());
            }

            // Not compatible if not a subset.
            if (!p0.containsAll(p1))
                return false;

            p1 = Stream.concat(grp.affinity().cachedAffinity(newAssignments.topologyVersion())
                .primaryPartitions(ctx.localNodeId()).stream(), grp.affinity()
                .cachedAffinity(newAssignments.topologyVersion()).backupPartitions(ctx.localNodeId()).stream())
                .collect(toSet());

            NavigableSet<AffinityTopologyVersion> toCheck = grp.affinity().cachedVersions()
                .headSet(newAssignments.topologyVersion(), false);

            if (!toCheck.contains(topVer)) {
                log.warning("History is not enough for checking compatible last rebalance, new rebalance started " +
                    "[grp=" + grp.cacheOrGroupName() + ", lastTop=" + topVer + ']');

                return false;
            }

            for (AffinityTopologyVersion previousTopVer : toCheck.descendingSet()) {
                if (previousTopVer.before(topVer))
                    break;

                if (!ctx.exchange().lastAffinityChangedTopologyVersion(previousTopVer).equals(previousTopVer))
                    continue;

                p0 = Stream.concat(grp.affinity().cachedAffinity(previousTopVer).primaryPartitions(ctx.localNodeId()).stream(),
                    grp.affinity().cachedAffinity(previousTopVer).backupPartitions(ctx.localNodeId()).stream())
                    .collect(toSet());

                // Not compatible if owners are different.
                if (!p0.equals(p1))
                    return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceFuture.class, this, "result", result());
        }

        /**
         * Collect demander per cache groups. For print statistics.
         *
         * @param demanderPred Demander predicate.
         * @return Set demanders.
         */
        private Set<GridDhtPartitionDemander> demanders(Predicate<? super GridDhtPartitionDemander> demanderPred) {
            return ctx.cacheContexts().stream()
                .map(GridCacheContext::preloader)
                .filter(GridDhtPreloader.class::isInstance)
                .map(GridDhtPreloader.class::cast)
                .map(GridDhtPreloader::demander)
                .filter(demanderPred)
                .collect(toSet());
        }

        /**
         * Logging statistics of rebalance. Statistics will be printed for
         * cache group and total if rebalance has ended for all cache groups
         * successfully.
         *
         * @throws IgniteCheckedException If error occurs.
         */
        private void printRebalanceStatistics() {
            assert isDone() : "RebalanceFuture should be done.";
            assert availablePrintRebalanceStatistics();
            assert nonNull(stat);

            RebalanceStatistics totalStat = ((GridDhtPreloader)grp.preloader()).demander().totalRebStat;
            assert nonNull(totalStat);

            stat.end(U.currentTimeMillis());

            if (log.isInfoEnabled())
                log.info(cacheGroupRebalanceStatistics(grp, stat, result(), topVer));

            totalStat.merge(stat);
            stat.reset();

            // Check that rebalance is over for all cache groups successfully.
            for (GridCacheContext<?, ?> cacheCtx : ctx.cacheContexts()) {
                IgniteInternalFuture<Boolean> rebFut = cacheCtx.preloader().rebalanceFuture();

                if (!rebFut.isDone() || !rebFut.result())
                    return;
            }

            // Exclude not rebalanced cache groups.
            Set<GridDhtPartitionDemander> demanders = demanders(d -> !d.rebalanceFut.isInitial());

            Map<CacheGroupContext, RebalanceStatistics> totalStats =
                demanders.stream().collect(toMap(d -> d.grp, d -> d.totalRebStat));

            if (log.isInfoEnabled())
                log.info(totalRebalanceStatistic(totalStats));

            totalStats.forEach((grpCtx, statistics) -> statistics.reset());
            demanders.forEach(d -> d.rebalanceFut.stat.resetAttempt());
        }

        /**
         * Return rebalance statistics.
         *
         * @return Rebalance statistics.
         */
        @Nullable public RebalanceStatistics statistics() {
            return stat;
        }
    }

    /**
     * Return total statistics of rebalance.
     *
     * @return Total statistics of rebalance.
     */
    @Nullable public RebalanceStatistics totalStatistics() {
        return totalRebStat;
    }

    /**
     * @param topVer Topopolog verion.
     */
    void finishPreloading(AffinityTopologyVersion topVer) {
        assert !rebalanceFut.isInitial() : topVer;

        rebalanceFut.ownPartitionsAndFinishFuture(topVer);
    }
}
