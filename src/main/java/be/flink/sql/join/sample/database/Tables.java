package be.flink.sql.join.sample.database;

import be.flink.sql.join.sample.io.LocalSource;

public class Tables {

    public static final String WORK_SCHEDULE_DEFINITION_TABLE = "GBTPAA30";
    private static final String WORK_SCHEDULE_DEFINITION_TABLE_DEFINITION =
            "CREATE TABLE IF NOT EXISTS PAA30 (\n" +
                    //"    event_time         TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL," +
                    "    WRK_SDL_DEF_NO     STRING,\n" +
                    "    WRK_SDL_DEF_NM     STRING,\n" +
                    "    LOG_DLT_IC         STRING,\n" +
                    "    SRC_NO             STRING,\n" +
                    "    PTY_NO             STRING,\n" +
                    "    CPN_ORG_NO         STRING,\n" +
                    "    REG_USER_CD        STRING,\n" +
                    "    REG_TS             BIGINT,\n" +
                    "    MUT_USER_CD        STRING,\n" +
                    "    MUT_TS             BIGINT,\n" +
                    "    PRIMARY KEY (WRK_SDL_DEF_NO) NOT ENFORCED\n" +
                    ") WITH (%s)";

    public static final String WORK_SCHEDULE_DEFINITION_STATE_TABLE = "GBTPAA31";
    private static final String WORK_SCHEDULE_DEFINITION_STATE_TABLE_DEFINITION =
            "CREATE TABLE IF NOT EXISTS PAA31 (\n" +
                    "    WRK_SDL_DEF_NO     STRING,\n" +
                    "    HTR_FROM_DT        BIGINT,\n" +
                    "    HTR_UNTL_DT        BIGINT,\n" +
                    "    WEEK_QT            DECIMAL(2),\n" +
                    "    STT_DT             BIGINT,\n" +
                    "    ATV_IC             STRING,\n" +
                    "    HOUR_DY_QT         DECIMAL(5, 2),\n" +
                    "    WRK_RGM_DY_QT      DECIMAL(3, 2),\n" +
                    "    WK_FTM_HOUR_QT     DECIMAL(5, 2),\n" +
                    "    EPM_FTN_DNM_QT     DECIMAL(6, 3),\n" +
                    "    EPM_FTN_NMR_QT     DECIMAL(6, 3),\n" +
                    "    REG_CD             STRING,\n" +
                    "    REG_USER_CD        STRING,\n" +
                    "    REG_TS             BIGINT,\n" +
                    "    MUT_USER_CD        STRING,\n" +
                    "    MUT_TS             BIGINT,\n" +
                    "    PRIMARY KEY (WRK_SDL_DEF_NO) NOT ENFORCED\n" +
                    ") WITH (%s)";

    public static final String WORK_SCHEDULE_DEFINITION_ELEMENT_TABLE = "GBTPAA32";
    private static final String WORK_SCHEDULE_DEFINITION_ELEMENT_TABLE_DEFINITION =
            "CREATE TABLE IF NOT EXISTS PAA32 (\n" +
                    "    WRK_SDL_DEF_NO     STRING,\n" +
                    "    HTR_FROM_DT        BIGINT,\n" +
                    "    DY_NO              INT,\n" +
                    "    SEQ_NO             INT,\n" +
                    "    ATV_IC             STRING,\n" +
                    "    HOUR_QT            DECIMAL(5, 2),\n" +
                    "    PST_AST_CD         STRING,\n" +
                    "    CTCR_CD            STRING,\n" +
                    "    SHF_CD             STRING,\n" +
                    "    REG_USER_CD        STRING,\n" +
                    "    REG_TS             BIGINT,\n" +
                    "    MUT_USER_CD        STRING,\n" +
                    "    MUT_TS             BIGINT,\n" +
                    "    PRIMARY KEY (WRK_SDL_DEF_NO, HTR_FROM_DT, DY_NO, SEQ_NO) NOT ENFORCED\n" +
                    ") WITH (%s)";

    public static final String WORK_SCHEDULE_DEFINITION_DESCRIPTION_TABLE = "GBTPAA33";
    private static final String WORK_SCHEDULE_DEFINITION_DESCRIPTION_TABLE_DEFINITION =
            "CREATE TABLE IF NOT EXISTS PAA33 (\n" +
                    "    WRK_SDL_DEF_NO     STRING,\n" +
                    "    LGG_CD             STRING,\n" +
                    "    DSB_TX             STRING,\n" +
                    "    REG_USER_CD        STRING,\n" +
                    "    REG_TS             BIGINT,\n" +
                    "    MUT_USER_CD        STRING,\n" +
                    "    MUT_TS             BIGINT,\n" +
                    "    PRIMARY KEY (WRK_SDL_DEF_NO, LGG_CD) NOT ENFORCED\n" +
                    ") WITH (%s)";

    public static String getWorkScheduleDefinitionTable(LocalSource source) {
        return String.format(WORK_SCHEDULE_DEFINITION_TABLE_DEFINITION, source.build());
    }

    public static String getWorkScheduleDefinitionStateTable(LocalSource source) {
        return String.format(WORK_SCHEDULE_DEFINITION_STATE_TABLE_DEFINITION, source.build());
    }

    public static String getWorkScheduleDefinitionElementTable(LocalSource source) {
        return String.format(WORK_SCHEDULE_DEFINITION_ELEMENT_TABLE_DEFINITION, source.build());
    }

    public static String getWorkScheduleDefinitionDescriptionTable(LocalSource source) {
        return String.format(WORK_SCHEDULE_DEFINITION_DESCRIPTION_TABLE_DEFINITION, source.build());
    }
}
