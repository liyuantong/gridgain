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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.command.Parser;

/**
 * DROP INDEX statement.
 */
public class GridSqlDropIndex extends GridSqlStatement {
    /** Index name. */
    private String idxName;

    /** Schema name. */
    private String schemaName;

    /** Attempt to drop the index only if it exists. */
    private boolean ifExists;

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @param idxName Index name.
     */
    public void indexName(String idxName) {
        this.idxName = idxName;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return whether attempt to drop the index should be made only if it exists.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /**
     * @param ifExists whether attempt to drop the index should be made only if it exists.
     */
    public void ifExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + Parser.quoteIdentifier(schemaName, true) + '.' +
            Parser.quoteIdentifier(idxName, true);
    }
}
