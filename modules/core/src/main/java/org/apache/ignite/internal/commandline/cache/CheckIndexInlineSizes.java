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

package org.apache.ignite.internal.commandline.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.commandline.cache.check_indexes_inline_size.CheckIndexInlineSizesResult;
import org.apache.ignite.internal.commandline.cache.check_indexes_inline_size.CheckIndexInlineSizesTask;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.CHECK_INDEX_INLINE_SIZES;

/**
 * Command for check secondary indexes inline size on the different nodes.
 */
public class CheckIndexInlineSizes implements Command<Void> {
    /** Warn message format. */
    public static final String NOT_ALL_NODES_SUPPORT_FEATURE_WARN_MSG_FMT =
        "Indexes inline size were not checked on all nodes because the few nodes aren't support this feature. Skipped nodes: %s";

    /** Success message. */
    public static final String INDEXES_INLINE_SIZE_ARE_SAME =
        "All secondary indexes have the same effective inline size on all cluster nodes.";

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Set<UUID> serverNodes = client.compute().nodes().stream()
                .filter(node -> Objects.equals(node.attribute(ATTR_CLIENT_MODE), false))
                .map(GridClientNode::nodeId)
                .collect(toSet());

            Set<UUID> supportedServerNodes = client.compute().nodes().stream()
                .filter(CheckIndexInlineSizes::checkIndexInlineSizesSupported)
                .map(GridClientNode::nodeId)
                .collect(toSet());

            CheckIndexInlineSizesResult res = client.compute().execute(
                CheckIndexInlineSizesTask.class.getName(),
                new VisorTaskArgument<>(supportedServerNodes, false)
            );

            Set<UUID> unsupportedNodes = serverNodes.stream()
                .filter(n -> !supportedServerNodes.contains(n))
                .collect(toSet());

            analizeResults(log, unsupportedNodes, res);
        }

        return null;
    }

    /** */
    private void analizeResults(
        Logger log,
        Set<UUID> unsupportedNodes,
        CheckIndexInlineSizesResult res
    ) {
        if (!F.isEmpty(unsupportedNodes))
            log.info(String.format(NOT_ALL_NODES_SUPPORT_FEATURE_WARN_MSG_FMT, unsupportedNodes));

        Map<String, Map<Integer, Set<UUID>>> indexToSizeNode = new HashMap<>();

        for (Map.Entry<UUID, Map<String, Integer>> nodeRes : res.inlineSizes().entrySet()) {
            for (Map.Entry<String, Integer> index : nodeRes.getValue().entrySet()) {
                Map<Integer, Set<UUID>> sizeToNodes = indexToSizeNode.computeIfAbsent(index.getKey(), x -> new HashMap<>());

                sizeToNodes.computeIfAbsent(index.getValue(), x -> new HashSet<>()).add(nodeRes.getKey());
            }
        }

        log.info("Found " + indexToSizeNode.size() + " secondary indexes.");

        Map<String, Map<Integer, Set<UUID>>> problems = indexToSizeNode.entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (F.isEmpty(problems))
            log.info(INDEXES_INLINE_SIZE_ARE_SAME);
        else
            printProblemsAndShowRecommendations(problems, log);
    }

    /** */
    private void printProblemsAndShowRecommendations(Map<String, Map<Integer, Set<UUID>>> problems, Logger log) {
        log.info(problems.size() +
            " indexes have different effective inline size on nodes. It can lead to performance degradation in SQL queries.");
        log.info("Indexes:");

        for (Map.Entry<String, Map<Integer, Set<UUID>>> entry : problems.entrySet()) {
            SB sb = new SB();

            sb.a("Full index name: ").a(entry.getKey());

            for (Integer size : entry.getValue().keySet())
                sb.a(" nodes: ").a(entry.getValue().get(size)).a(" inline size: ").a(size).a(",");

            sb.setLength(sb.length() - 1);

            log.info(INDENT + sb);
        }

        log.info("");

        log.info("Recommendations:");
        log.info(INDENT + "Check that value of property " + IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE + " are the same on all nodes.");
        log.info(INDENT + "Recreate indexes with different inline size.");
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        usageCache(
            logger,
            CHECK_INDEX_INLINE_SIZES,
            "Checks that secondary indexes inline size are same on the cluster nodes.",
            null
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CHECK_INDEX_INLINE_SIZES.text().toUpperCase();
    }

    /** */
    private static boolean checkIndexInlineSizesSupported(GridClientNode node) {
        return nodeSupports(node.attribute(ATTR_IGNITE_FEATURES), IgniteFeatures.CHECK_INDEX_INLINE_SIZES);
    }
}
