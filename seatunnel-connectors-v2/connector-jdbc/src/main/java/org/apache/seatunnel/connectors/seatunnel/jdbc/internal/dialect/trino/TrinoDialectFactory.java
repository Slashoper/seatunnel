package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.trino;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;

@AutoService(JdbcDialectFactory.class)
public class TrinoDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:trino:");
    }

    @Override
    public JdbcDialect create() {
        return new MysqlDialect();
    }
}
