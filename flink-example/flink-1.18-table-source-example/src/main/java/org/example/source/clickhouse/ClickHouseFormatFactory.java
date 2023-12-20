package org.example.source.clickhouse;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Collections;
import java.util.Set;

public class ClickHouseFormatFactory implements DeserializationFormatFactory {
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {

        // 返回 DecodingFormat<DeserializationSchema<RowData>> 的实现
        return new ClickHouseFormat();
    }

    @Override
    public String factoryIdentifier() {
        return "clickhouse-row";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}

