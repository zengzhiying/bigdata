package org.example.source.clickhouse;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class ClickHouseDeserializer implements DeserializationSchema<RowData> {
    private final DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;

    public ClickHouseDeserializer(
            DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo) {
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        converter.open(RuntimeConverter.Context.create(ClickHouseDeserializer.class.getClassLoader()));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
