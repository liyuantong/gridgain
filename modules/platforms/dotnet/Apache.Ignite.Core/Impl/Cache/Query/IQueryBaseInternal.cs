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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Internal base interface for queries.
    /// </summary>
    internal interface IQueryBaseInternal
    {
        /// <summary>
        /// Writes this instance to the specified writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        void Write(BinaryWriter writer, bool keepBinary);

        /// <summary>
        /// Gets the interop op code.
        /// </summary>
        CacheOp OpId { get; }
    }
}
