package org.example.source.clickhouse;


import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class ClickHouseSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    private final ClickHouseConnection clickHouseConnection;
    private final DeserializationSchema<RowData> deserializer;
    private final DynamicTableSource.DataStructureConverter converter;

    private volatile boolean isRunning = true;

    public ClickHouseSourceFunction(ClickHouseConnection clickHouseConnection, DeserializationSchema<RowData> deserializer, DynamicTableSource.DataStructureConverter converter) {
        this.clickHouseConnection = clickHouseConnection;
        this.deserializer = deserializer;
        this.converter = converter;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }


    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        String endpoint = clickHouseConnection.getEndpoint();
        String table = clickHouseConnection.getTable();
        ClickHouseNode clickHouseNode = ClickHouseNode.of(endpoint);
        while (isRunning) {
            try (ClickHouseClient client = ClickHouseClient.newInstance(clickHouseNode.getProtocol())) {
                ClickHouseResponse response = client.read(endpoint)
                        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                        .query("select name, score, user_id from " + table)
                        .executeAndWait();
                for(ClickHouseRecord record : response.records()) {
                    Row row = new Row(RowKind.INSERT, record.size());
                    row.setField(0, record.getValue("name").asString());
                    row.setField(1, record.getValue("score").asInteger());
                    row.setField(2, record.getValue("user_id").asBinary());
                    ctx.collect((RowData) converter.toInternal(row));
                }
                response.close();
                cancel();
            } catch (Throwable t) {
                t.printStackTrace(); // print and continue
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
