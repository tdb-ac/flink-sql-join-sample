package be.flink.sql.join.sample.database;

public class Queries {

    public static final String JOIN_WORK_SCHEDULE_DEFINITIONS = "" +
            "SELECT PAA30.WRK_SDL_DEF_NO as id,\n" +
            "       PAA33.DSB_TX as description,\n" +
            "       '1' as typeCode,\n" +
            "       PAA30.PTY_NO as employerId,\n" +
            "       PAA30.CPN_ORG_NO as companyOrganisationNumber,\n" +
            "       PAA31.HTR_FROM_DT as historyFromDate,\n" +
            "       PAA31.HTR_UNTL_DT as historyUntilDate,\n" +
            "       PAA31.STT_DT as referenceStartDate,\n" +
            "       PAA31.ATV_IC as active,\n" +
            "       PAA31.HOUR_DY_QT as hoursPerDay,\n" +
            "       PAA32.DY_NO as dayNumber,\n" +
            "       PAA32.SEQ_NO as sequenceInDay,\n" +
            "       PAA32.HOUR_QT as hourQuantity,\n" +
            "       PAA32.PST_AST_CD as performanceCode,\n" +
            "       PAA32.CTCR_CD as costCode,\n" +
            "       PAA32.SHF_CD as shiftCode,\n" +
            "       PAA30.REG_USER_CD as createdBy,\n" +
            "       PAA30.REG_TS as createdTimeStamp,\n" +
            "       PAA30.MUT_USER_CD as updatedBy,\n" +
            "       PAA30.MUT_TS as updatedTimeStamp,\n" +
            "       PAA30.event_time as cdcEventTime\n" +
            "FROM PAA30\n" +
            "INNER JOIN PAA31 ON PAA30.WRK_SDL_DEF_NO = PAA31.WRK_SDL_DEF_NO\n" +
            "INNER JOIN PAA33 ON PAA30.WRK_SDL_DEF_NO = PAA33.WRK_SDL_DEF_NO AND PAA33.LGG_CD = 'NL'\n" +
            "INNER JOIN PAA32 ON PAA30.WRK_SDL_DEF_NO = PAA32.WRK_SDL_DEF_NO";

    private Queries() {
        // enforce static usage
    }

}
