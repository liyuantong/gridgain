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

namespace Apache.Ignite.Core.Impl.Binary.Metadata
{
    using System.Collections.Generic;

    /// <summary>
    /// Metadata handler which uses hash set to determine whether field was already written or not.
    /// </summary>
    internal class BinaryTypeHashsetHandler : IBinaryTypeHandler
    {
        /** Empty fields collection. */
        private static readonly IDictionary<string, BinaryField> EmptyFields = new Dictionary<string, BinaryField>();

        /** IDs known when serialization starts. */
        private readonly ICollection<int> _ids;

        /** New fields. */
        private IDictionary<string, BinaryField> _fieldMap;

        /** */
        private readonly bool _newType;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ids">IDs.</param>
        /// <param name="newType">True is metadata for type is not saved.</param>
        public BinaryTypeHashsetHandler(ICollection<int> ids, bool newType)
        {
            _ids = ids;
            _newType = newType;
        }

        /** <inheritdoc /> */
        public void OnFieldWrite(int fieldId, string fieldName, int typeId)
        {
            if (!_ids.Contains(fieldId))
            {
                if (_fieldMap == null)
                    _fieldMap = new Dictionary<string, BinaryField>();

                if (!_fieldMap.ContainsKey(fieldName))
                    _fieldMap[fieldName] = new BinaryField(typeId, fieldId);
            }
        }

        /** <inheritdoc /> */
        public IDictionary<string, BinaryField> OnObjectWriteFinished()
        {
            return _fieldMap ?? (_newType ? EmptyFields : null);
        }
    }
}
