package org.example.source.clickhouse;

import java.io.Serializable;

public class ClickHouseConnection implements Serializable {
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final String database;
    private final String table;

    public ClickHouseConnection(String hostname, int port, String username, String password, String database, String table) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
    }

    public String getEndpoint() {
        StringBuilder builder = new StringBuilder();
        builder.append("http://")
                .append(this.hostname)
                .append(":")
                .append(this.port)
                .append("/")
                .append(this.database)
                .append("?user=")
                .append(username);
        if(!"".equals(password)) {
            builder.append("&password=")
                    .append(password);
        }
        return builder.toString();
    }

    public String getTable() {
        return table;
    }
}
