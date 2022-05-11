package be.flink.sql.join.sample.functions;

import be.flink.sql.join.sample.database.WorkScheduleDefinitionRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;

public class WorkScheduleDefinitionRowMapFunction implements MapFunction<Row, WorkScheduleDefinitionRow> {

    @Override
    public WorkScheduleDefinitionRow map(Row row){
        return new WorkScheduleDefinitionRow()
                .setRowKind(row.getKind())
                .setId(StringUtils.trim(row.getFieldAs("id")))
                .setDescription(StringUtils.trim(row.getFieldAs("description")))
                .setTypeCode(StringUtils.trim(row.getFieldAs("typeCode")))
                .setEmployerId(StringUtils.trim(row.getFieldAs("employerId")))
                .setCompanyOrganisationNumber(StringUtils.trim(row.getFieldAs("companyOrganisationNumber")))
                .setHistoryFromDate(Math.toIntExact(row.getFieldAs("historyFromDate")))
                .setHistoryUntilDate(Math.toIntExact(row.getFieldAs("historyUntilDate")))
                .setReferenceStartDate(Math.toIntExact(row.getFieldAs("referenceStartDate")))
                .setActive(row.getFieldAs("active") != "0")
                .setHoursPerDay(row.getFieldAs("hoursPerDay") != null ? row.getFieldAs("hoursPerDay") : new BigDecimal(0))
                .setDayNumber(row.getFieldAs("dayNumber"))
                .setSequenceInDay(row.getFieldAs("sequenceInDay"))
                .setHourQuantity(row.getFieldAs("hourQuantity") != null ? row.getFieldAs("hourQuantity") : new BigDecimal(0))
                .setPerformanceCode(StringUtils.trim(row.getFieldAs("performanceCode")))
                .setCostCode(StringUtils.trim(row.getFieldAs("costCode")))
                .setShiftCode(StringUtils.trim(row.getFieldAs("shiftCode")))
                .setCreatedBy(StringUtils.trim(row.getFieldAs("createdBy")))
                .setCreatedTimeStamp(row.getFieldAs("createdTimeStamp"))
                .setUpdatedBy(StringUtils.trim(row.getFieldAs("updatedBy")))
                .setUpdatedTimeStamp(row.getFieldAs("updatedTimeStamp"));
                //.setCdcEventTime(((LocalDateTime) row.getFieldAs("cdcEventTime")).toInstant(ZoneOffset.UTC).toEpochMilli());
    }
}
