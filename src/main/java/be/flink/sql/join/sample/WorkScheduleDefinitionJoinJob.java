package be.flink.sql.join.sample;

import be.flink.sql.join.sample.database.Queries;
import be.flink.sql.join.sample.database.Tables;
import be.flink.sql.join.sample.database.WorkScheduleDefinitionRow;
import be.flink.sql.join.sample.functions.WorkScheduleDefinitionRowMapFunction;
import be.flink.sql.join.sample.io.LocalSource;
import be.flink.sql.join.sample.io.PulsarSink;
import be.flink.sql.join.sample.io.PulsarSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

public class WorkScheduleDefinitionJoinJob extends FlinkJobBase {

    public static void main(String[] args) throws Exception {
        new WorkScheduleDefinitionJoinJob(args)
                .execute();
    }

    public WorkScheduleDefinitionJoinJob(String[] args) throws Exception {
        super(5000L, WorkScheduleDefinitionJoinJob.class, args);
    }

    private void execute() throws Exception {
        DataStream<Row> inputStream = createJoinedTable();
        DataStream<WorkScheduleDefinitionRow> mappedStream = inputStream
                .map(new WorkScheduleDefinitionRowMapFunction());
        mappedStream.print();
        environment.execute("WorkScheduleDefinition Joining Job " + runId);
    }

    public DataStream<Row> createJoinedTable(){
        tableEnvironment.executeSql(Tables.getWorkScheduleDefinitionTable(
                LocalSource.createForTable(Tables.WORK_SCHEDULE_DEFINITION_TABLE)));
        tableEnvironment.executeSql(Tables.getWorkScheduleDefinitionStateTable(
                LocalSource.createForTable(Tables.WORK_SCHEDULE_DEFINITION_STATE_TABLE)));
        tableEnvironment.executeSql(Tables.getWorkScheduleDefinitionElementTable(
                LocalSource.createForTable(Tables.WORK_SCHEDULE_DEFINITION_ELEMENT_TABLE)));
        tableEnvironment.executeSql(Tables.getWorkScheduleDefinitionDescriptionTable(
               LocalSource.createForTable(Tables.WORK_SCHEDULE_DEFINITION_DESCRIPTION_TABLE)));
        Table joinedTable = tableEnvironment.sqlQuery(Queries.JOIN_WORK_SCHEDULE_DEFINITIONS);
        return tableEnvironment.toChangelogStream(joinedTable, Schema.newBuilder()
                .column("id", "STRING NOT NULL")
                .column("description", "STRING")
                .column("typeCode", "CHAR(1) NOT NULL")
                .column("employerId", "STRING")
                .column("companyOrganisationNumber", "STRING")
                .column("historyFromDate", "BIGINT")
                .column("historyUntilDate", "BIGINT")
                .column("referenceStartDate", "BIGINT")
                .column("active", "STRING")
                .column("hoursPerDay", "DECIMAL(5, 2)")
                .column("dayNumber", "INT NOT NULL")
                .column("sequenceInDay", "INT NOT NULL")
                .column("hourQuantity", "DECIMAL(5, 2)")
                .column("performanceCode", "STRING")
                .column("costCode", "STRING")
                .column("shiftCode", "STRING")
                .column("createdBy", "STRING")
                .column("createdTimeStamp", "BIGINT")
                .column("updatedBy", "STRING")
                .column("updatedTimeStamp", "BIGINT")
                .build(), ChangelogMode.upsert());
    }

}
