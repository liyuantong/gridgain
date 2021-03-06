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

package org.apache.ignite.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This handler could be used only with ignite.(sh|bat) script.
 * Process will be terminated using {@link Ignition#restart(boolean)} call.
 */
public class RestartProcessFailureHandler extends AbstractFailureHandler {
    /** {@inheritDoc} */
    @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    U.error(ignite.log(), "Restarting JVM on Ignite failure: [failureCtx=" + failureCtx + ']');

                    G.restart(true);
                }
            },
            "node-restarter"
        ).start();

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RestartProcessFailureHandler.class, this, "super", super.toString());
    }
}
