package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.trino;

import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TrinoTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================

    private static final String TRINO_UNKNOWN = "UNKNOWN";
    private static final String TRINO_BOOLEAN = "BOOLEAN";

    // -------------------------number----------------------------
    private static final String TRINO_TINYINT = "TINYINT";
    private static final String TRINO_SMALLINT = "SMALLINT";
    private static final String TRINO_INTEGER = "INTEGER";
    private static final String TRINO_BIGINT = "BIGINT";
    private static final String TRINO_DECIMAL = "DECIMAL";
    private static final String TRINO_FLOAT = "FLOAT";
    private static final String TRINO_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    private static final String TRINO_CHAR = "CHAR";
    private static final String TRINO_VARCHAR = "VARCHAR";
    private static final String TRINO_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String TRINO_DATE = "DATE";
    private static final String TRINO_DATETIME = "DATETIME";
    private static final String TRINO_TIME = "TIME";
    private static final String TRINO_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String TRINO_TINYBLOB = "TINYBLOB";
    private static final String TRINO_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String TRINO_BLOB = "BLOB";
    private static final String TRINO_LONGBLOB = "LONGBLOB";
    private static final String TRINO_BINARY = "BINARY";
    private static final String TRINO_VARBINARY = "VARBINARY";
    private static final String TRINO_GEOMETRY = "GEOMETRY";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String trinoType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (trinoType) {
            case TRINO_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TRINO_TINYINT:
            case TRINO_SMALLINT:
            case TRINO_INTEGER:
            case TRINO_BIGINT:
                return BasicType.LONG_TYPE;
            case TRINO_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", TRINO_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);

            case TRINO_FLOAT:
                return BasicType.FLOAT_TYPE;

            case TRINO_DOUBLE:
                return BasicType.DOUBLE_TYPE;

            case TRINO_CHAR:
            case TRINO_VARCHAR:
            case TRINO_JSON:
                return BasicType.STRING_TYPE;
            case TRINO_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TRINO_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TRINO_DATETIME:
            case TRINO_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case TRINO_TINYBLOB:
            case TRINO_MEDIUMBLOB:
            case TRINO_BLOB:
            case TRINO_LONGBLOB:
            case TRINO_VARBINARY:
            case TRINO_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case TRINO_GEOMETRY:
            case TRINO_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new JdbcConnectorException(
                        CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR,
                        String.format(
                                "Doesn't support Trino type '%s' on column '%s'  yet.",
                                trinoType, jdbcColumnName));
        }
    }
}
