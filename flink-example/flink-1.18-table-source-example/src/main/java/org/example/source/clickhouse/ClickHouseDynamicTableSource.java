package org.example.source.clickhouse;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class ClickHouseDynamicTableSource implements ScanTableSource {

    private final ClickHouseConnection clickHouseConnection;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public ClickHouseDynamicTableSource(
            ClickHouseConnection clickHouseConnection,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType) {
        this.clickHouseConnection = clickHouseConnection;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // 发送到集群的运行时上下文
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                runtimeProviderContext,
                producedDataType);
        DataStructureConverter converter = runtimeProviderContext.createDataStructureConverter(producedDataType);

        // 创建 SourceFunction<RowData>
        final SourceFunction<RowData> sourceFunction = new ClickHouseSourceFunction(
                clickHouseConnection,
                deserializer,
                converter);

        // 第二个参数设置是否是有界流
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public DynamicTableSource copy() {
        // 实现拷贝
        return new ClickHouseDynamicTableSource(clickHouseConnection, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse Table Source";
    }
}
