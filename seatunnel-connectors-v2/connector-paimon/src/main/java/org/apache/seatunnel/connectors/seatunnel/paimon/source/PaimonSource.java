/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowTypeConverter;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter.convertSqlSelectToPaimonProjectionIndex;
import static org.apache.seatunnel.connectors.seatunnel.paimon.source.converter.SqlToPaimonPredicateConverter.convertToPlainSelect;

/** Paimon connector source class. */
public class PaimonSource
        implements SeaTunnelSource<SeaTunnelRow, PaimonSourceSplit, PaimonSourceState> {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "Paimon";

    private ReadonlyConfig readonlyConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private Table paimonTable;

    private Predicate predicate;

    private int[] projectionIndex;

    private CatalogTable catalogTable;

    public PaimonSource(ReadonlyConfig readonlyConfig, PaimonCatalog paimonCatalog) {
        this.readonlyConfig = readonlyConfig;
        PaimonSourceConfig paimonSourceConfig = new PaimonSourceConfig(readonlyConfig);
        TablePath tablePath =
                TablePath.of(paimonSourceConfig.getNamespace(), paimonSourceConfig.getTable());
        this.catalogTable = paimonCatalog.getTable(tablePath);
        this.paimonTable = paimonCatalog.getPaimonTable(tablePath);

        String filterSql = readonlyConfig.get(PaimonSourceConfig.QUERY_SQL);
        PlainSelect plainSelect = convertToPlainSelect(filterSql);
        RowType paimonRowType = this.paimonTable.rowType();
        String[] filedNames = paimonRowType.getFieldNames().toArray(new String[0]);
        if (!Objects.isNull(plainSelect)) {
            this.projectionIndex = convertSqlSelectToPaimonProjectionIndex(filedNames, plainSelect);
            if (!Objects.isNull(projectionIndex)) {
                this.catalogTable =
                        paimonCatalog.getTableWithProjection(tablePath, projectionIndex);
            }
            this.predicate =
                    SqlToPaimonPredicateConverter.convertSqlWhereToPaimonPredicate(
                            paimonRowType, plainSelect);
        }
        seaTunnelRowType = RowTypeConverter.convert(paimonRowType, projectionIndex);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, PaimonSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new PaimonSourceReader(
                readerContext, paimonTable, seaTunnelRowType, predicate, projectionIndex);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> createEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext) throws Exception {
        return new PaimonSourceSplitEnumerator(
                enumeratorContext, paimonTable, predicate, projectionIndex);
    }

    @Override
    public SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<PaimonSourceSplit> enumeratorContext,
            PaimonSourceState checkpointState)
            throws Exception {
        return new PaimonSourceSplitEnumerator(
                enumeratorContext, paimonTable, checkpointState, predicate, projectionIndex);
    }
}
