package org.example.source.clickhouse;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class ClickHouseDynamicTableFactory implements DynamicTableSourceFactory {
    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .defaultValue(8123);

    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .defaultValue("default");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .defaultValue("");

    public static final ConfigOption<String> DATABASE = ConfigOptions.key("database")
            .stringType()
            .defaultValue("default");
    public static final ConfigOption<String> TABLE = ConfigOptions.key("table")
            .stringType()
            .noDefaultValue();

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 内置验证工具
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        helper.validate();

        // 获取已经验证的参数
        final ReadableConfig options = helper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final int port = options.get(PORT);
        final String username = options.get(USERNAME);
        final String password = options.get(PASSWORD);
        final String database = options.get(DATABASE);
        final String table = options.get(TABLE);

        ClickHouseConnection clickHouseConnection = new ClickHouseConnection(hostname, port, username, password, database, table);

        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();


        // 返回 DynamicTableSource
        return new ClickHouseDynamicTableSource(clickHouseConnection, decodingFormat, producedDataType);
    }

    @Override
    public String factoryIdentifier() {
        return "clickhouse";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(TABLE);
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE);
        options.add(TABLE);
        return options;
    }

}
