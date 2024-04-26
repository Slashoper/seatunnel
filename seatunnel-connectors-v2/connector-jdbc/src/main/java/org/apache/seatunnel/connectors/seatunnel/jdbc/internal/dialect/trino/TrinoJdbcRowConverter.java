package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.trino;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

public class TrinoJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "Trino";
    }
}
