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

package org.apache.ignite.internal.util.lang.gridfunc;

import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Predicate that evaluates to {@code true} if each of its component preds evaluates to {@code true}.
 *
 * @param <T> Type of the free variable, i.e. the element the predicate is called on.
 */
public class IsAllPredicate<T> implements IgnitePredicate<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgnitePredicate<? super T>[] preds;

    /**
     * @param preds Passed in predicate. If none provided - always-{@code false} predicate is returned.
     */
    public IsAllPredicate(IgnitePredicate<? super T>... preds) {
        this.preds = preds;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(T t) {
        return GridFunc.isAll(t, preds);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IsAllPredicate.class, this);
    }
}
