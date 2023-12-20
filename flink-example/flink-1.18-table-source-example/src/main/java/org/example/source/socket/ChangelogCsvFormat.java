package org.example.source.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private final String columnDelimiter;

    public ChangelogCsvFormat(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(
                producedDataType);
        final DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(producedDataType);
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();
        // 返回 DeserializationSchema<RowData> 的实现
        return new ChangelogCsvDeserializer(parsingTypes, converter, producedTypeInfo, columnDelimiter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                // 批处理不能添加除 INSERT 之外的其他操作
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
