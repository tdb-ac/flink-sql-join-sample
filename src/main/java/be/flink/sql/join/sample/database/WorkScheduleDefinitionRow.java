package be.flink.sql.join.sample.database;

import be.flink.sql.join.sample.io.PulsarSerializable;
import org.apache.avro.reflect.AvroSchema;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class WorkScheduleDefinitionRow implements PulsarSerializable {

    private RowKind rowKind;
    private String id;
    private String description;
    private String typeCode;
    private String employerId;
    private String companyOrganisationNumber;
    @AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
    private int historyFromDate;
    @AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
    private int historyUntilDate;
    @AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
    private int referenceStartDate;
    private boolean active;
    private int dayNumber;
    private int sequenceInDay;
    @AvroSchema("{ \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 5, \"scale\": 2 }")
    private BigDecimal hourQuantity;
    private String performanceCode;
    private String costCode;
    private String shiftCode;
    @AvroSchema("{ \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 5, \"scale\": 2 }")
    private BigDecimal hoursPerDay;
    private String createdBy;
    @AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
    private long createdTimeStamp;
    private String updatedBy;
    @AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
    private long updatedTimeStamp;
    @AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
    private long cdcEventTime;

    @Override
    public byte[] getMessageKey() {
        return getWindowKey().getBytes(StandardCharsets.UTF_8);
    }

    public String getWindowKey() {
        return String.format("%s-%s", id, historyFromDate);
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public WorkScheduleDefinitionRow setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
        return this;
    }

    public String getId() {
        return id;
    }

    public WorkScheduleDefinitionRow setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public WorkScheduleDefinitionRow setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getTypeCode() {
        return typeCode;
    }

    public WorkScheduleDefinitionRow setTypeCode(String typeCode) {
        this.typeCode = typeCode;
        return this;
    }

    public String getEmployerId() {
        return employerId;
    }

    public WorkScheduleDefinitionRow setEmployerId(String employerId) {
        this.employerId = employerId;
        return this;
    }

    public String getCompanyOrganisationNumber() {
        return companyOrganisationNumber;
    }

    public WorkScheduleDefinitionRow setCompanyOrganisationNumber(String companyOrganisationNumber) {
        this.companyOrganisationNumber = companyOrganisationNumber;
        return this;
    }

    public int getHistoryFromDate() {
        return historyFromDate;
    }

    public WorkScheduleDefinitionRow setHistoryFromDate(int historyFromDate) {
        this.historyFromDate = historyFromDate;
        return this;
    }

    public int getHistoryUntilDate() {
        return historyUntilDate;
    }

    public WorkScheduleDefinitionRow setHistoryUntilDate(int historyUntilDate) {
        this.historyUntilDate = historyUntilDate;
        return this;
    }

    public int getReferenceStartDate() {
        return referenceStartDate;
    }

    public WorkScheduleDefinitionRow setReferenceStartDate(int referenceStartDate) {
        this.referenceStartDate = referenceStartDate;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public WorkScheduleDefinitionRow setActive(boolean active) {
        this.active = active;
        return this;
    }

    public int getDayNumber() {
        return dayNumber;
    }

    public WorkScheduleDefinitionRow setDayNumber(int dayNumber) {
        this.dayNumber = dayNumber;
        return this;
    }

    public int getSequenceInDay() {
        return sequenceInDay;
    }

    public WorkScheduleDefinitionRow setSequenceInDay(int sequenceInDay) {
        this.sequenceInDay = sequenceInDay;
        return this;
    }

    public BigDecimal getHourQuantity() {
        return hourQuantity;
    }

    public WorkScheduleDefinitionRow setHourQuantity(BigDecimal hourQuantity) {
        this.hourQuantity = hourQuantity;
        return this;
    }

    public String getPerformanceCode() {
        return performanceCode;
    }

    public WorkScheduleDefinitionRow setPerformanceCode(String performanceCode) {
        this.performanceCode = performanceCode;
        return this;
    }

    public String getCostCode() {
        return costCode;
    }

    public WorkScheduleDefinitionRow setCostCode(String costCode) {
        this.costCode = costCode;
        return this;
    }

    public String getShiftCode() {
        return shiftCode;
    }

    public WorkScheduleDefinitionRow setShiftCode(String shiftCode) {
        this.shiftCode = shiftCode;
        return this;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public WorkScheduleDefinitionRow setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
        return this;
    }

    public long getCreatedTimeStamp() {
        return createdTimeStamp;
    }

    public WorkScheduleDefinitionRow setCreatedTimeStamp(long createdTimeStamp) {
        this.createdTimeStamp = createdTimeStamp;
        return this;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public WorkScheduleDefinitionRow setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
        return this;
    }

    public long getUpdatedTimeStamp() {
        return updatedTimeStamp;
    }

    public WorkScheduleDefinitionRow setUpdatedTimeStamp(long updatedTimeStamp) {
        this.updatedTimeStamp = updatedTimeStamp;
        return this;
    }

    public long getCdcEventTime() {
        return cdcEventTime;
    }

    public WorkScheduleDefinitionRow setCdcEventTime(long cdcEventTime) {
        this.cdcEventTime = cdcEventTime;
        return this;
    }

    public BigDecimal getHoursPerDay() {
        return hoursPerDay;
    }

    public WorkScheduleDefinitionRow setHoursPerDay(BigDecimal hoursPerDay) {
        this.hoursPerDay = hoursPerDay;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkScheduleDefinitionRow that = (WorkScheduleDefinitionRow) o;
        return getSequenceInDay() == that.getSequenceInDay() && getRowKind() == that.getRowKind() && Objects.equals(getId(), that.getId()) && Objects.equals(getEmployerId(), that.getEmployerId()) && Objects.equals(getCreatedBy(), that.getCreatedBy()) && Objects.equals(getCreatedTimeStamp(), that.getCreatedTimeStamp()) && Objects.equals(getUpdatedBy(), that.getUpdatedBy()) && Objects.equals(getUpdatedTimeStamp(), that.getUpdatedTimeStamp()) && Objects.equals(getCdcEventTime(), that.getCdcEventTime());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRowKind(), getId(), getEmployerId(), getSequenceInDay(), getCreatedBy(), getCreatedTimeStamp(), getUpdatedBy(), getUpdatedTimeStamp(), getCdcEventTime());
    }

}
