package org.apache.ignite.glowroot;

import org.glowroot.agent.plugin.api.Agent;
import org.glowroot.agent.plugin.api.MessageSupplier;
import org.glowroot.agent.plugin.api.OptionalThreadContext;
import org.glowroot.agent.plugin.api.TimerName;
import org.glowroot.agent.plugin.api.TraceEntry;
import org.glowroot.agent.plugin.api.weaving.BindParameterArray;
import org.glowroot.agent.plugin.api.weaving.BindReceiver;
import org.glowroot.agent.plugin.api.weaving.BindReturn;
import org.glowroot.agent.plugin.api.weaving.BindThrowable;
import org.glowroot.agent.plugin.api.weaving.BindTraveler;
import org.glowroot.agent.plugin.api.weaving.OnAfter;
import org.glowroot.agent.plugin.api.weaving.OnBefore;
import org.glowroot.agent.plugin.api.weaving.OnReturn;
import org.glowroot.agent.plugin.api.weaving.OnThrow;
import org.glowroot.agent.plugin.api.weaving.Pointcut;
import org.glowroot.agent.plugin.api.weaving.Shim;

/**
 */
public class TransactionAspect {
    @Shim("org.apache.ignite.transactions.Transaction")
    public interface Transaction {
        String label();
    }

    /** Static tl looks safe
     * Thread local not necessary, can extend NearLocalTx and attach trace entry.
     */
    private static ThreadLocal<TraceEntry> ctx = new ThreadLocal<>();

    private static ThreadLocal<long[]> ctx2 = new ThreadLocal<long[]>() {
        @Override protected long[] initialValue() {
            return new long[1];
        }
    };

    /**
     */
    @Pointcut(className = "org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager",
        methodName = "newTx",
        nestingGroup = "ignite",
        methodParameterTypes = {".."},
        timerName = "process_tx")
    public static class TxStartAdvice {
        /** Timer. */
        private static final TimerName timer = Agent.getTimerName(TxStartAdvice.class);

        /**
         * @param ctx Context.
         * @param params Params.
         */
        @OnBefore
        public static TraceEntry onBefore(OptionalThreadContext ctx,
            @BindReceiver Object mgr,
            @BindParameterArray Object[] params) {
            return ctx.startTransaction("Ignite", "ignite-" + Thread.currentThread().getName(),
                MessageSupplier.create("start tx"), // TODO add label
                timer);
        }

        /**
         * @param ret Ret.
         * @param traceEntry Trace entry.
         */
        @OnReturn
        public static void onReturn(@BindReturn Object ret, @BindTraveler TraceEntry traceEntry) {
            ctx.set(traceEntry);
        }

        /**
         * @param throwable Throwable.
         * @param traceEntry Trace entry.
         */
        @OnThrow
        public static void onThrow(@BindThrowable Throwable throwable,
            @BindTraveler TraceEntry traceEntry) {
            traceEntry.endWithError(throwable);
        }
    }

    /**
     */
    @Pointcut(className = "org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl",
        methodName = "commit|rollback",
        methodParameterTypes = {},
        timerName = "finish_tx")
    public static class TxFinishAdvice {
        private static final TimerName timer = Agent.getTimerName(TxFinishAdvice.class);

        /**
         * @param ctx Context.
         */
        @OnBefore
        public static TraceEntry onBefore(OptionalThreadContext ctx, @BindReceiver Transaction proxy) {
            return ctx.startTraceEntry(MessageSupplier.create("commit tx: label={}", proxy.label()),
                timer);
        }

        @OnReturn
        public static void onReturn(@BindTraveler TraceEntry traceEntry) {
            traceEntry.end();
        }

        @OnThrow
        public static void onThrow(@BindThrowable Throwable throwable,
            @BindTraveler TraceEntry traceEntry) {
            traceEntry.endWithError(throwable);
        }

        @OnAfter public static void onAfter() {
            TraceEntry entry = ctx.get();

            if (entry != null) {
                long cntr = ctx2.get()[0];
                ctx2.get()[0] = cntr + 1;

                if (cntr % 1_000 == 0)
                    entry.endWithError("Trace");
                else
                    entry.end();


                ctx.set(null);
            }
        }
    }
}
