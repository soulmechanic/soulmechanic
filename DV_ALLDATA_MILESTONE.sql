SELECT
     
    PA.PUBLISHED_DATE,
    CONCAT(
        AUPM.PROJECT_CODE,
        CASE AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME WHEN 'POC Study Start' THEN 'PSS' WHEN 'Pivotal Program Start (PPS)' THEN 'Pivotal Start' ELSE AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME END,
        TO_CHAR(AUPM.PLAN_FINISH, 'YYYY')
    ) AS PRESENTATION_IDENTIFIER,
    PA.PROJECT_SHORT_NAME AS PROJECT_NAME,
    AUPM.PROJECT_CODE,
    
    AUPM.PROJ_TYPE,
    
    PA.ZONE AS "DIVISION",
    CASE 
    WHEN CONTAINS(LOWER(PA.PRIMARY_INDICATIONS), 'myeloma') THEN 'Oncology-Heme'
    WHEN CONTAINS(LOWER(PA.PRIMARY_INDICATIONS),'leukemia') THEN 'Oncology-Heme' 
    WHEN CONTAINS(LOWER(PA.PRIMARY_INDICATIONS), 'leukaemia') THEN 'Oncology-Heme'
    WHEN CONTAINS(LOWER(PA.PRIMARY_INDICATIONS), 'myelodysplastic') THEN 'Oncology-Heme' 
    WHEN CONTAINS(LOWER(PA.PRIMARY_INDICATIONS), 'mds') THEN 'Oncology-Heme' 
    WHEN CONTAINS(LOWER(PA.PRIMARY_INDICATIONS), 'lymphoma') THEN 'Oncology-Heme' 
    WHEN CONTAINS(LOWER(PA.PRIMARY_INDICATIONS), 'nhl') THEN 'Oncology-Heme' 
    WHEN PA.CATEGORY = 'Oncology' THEN 'Oncology-Solid' ELSE PA.CATEGORY END AS "BUSINESS_CATEOGRY",
    PA.UNIT,
    PA.STATUS,
    CASE PA.BINNED_PHARMA_CATEGORY 
    WHEN 'Biologic' THEN 'Bio' 
    WHEN 'Vaccine' THEN 'Vx' 
    WHEN 'Small Organic Molecule' THEN 'SM' ELSE PA.BINNED_PHARMA_CATEGORY END AS "COMPOUND_TYPE",
    PA.PRIMARY_INDICATIONS,
    PA.PROJECT_TYPE AS "CANDIDATE_TYPE",
    IFNULL(PA.STAGE, 'N/A') AS STAGE,
    CASE AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME WHEN 'POC Study Start' THEN 'PSS' WHEN 'Pivotal Program Start (PPS)' THEN 'Pivotal Start' 
    ELSE AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME END AS "MILESTONE",
    TO_CHAR(AUPM.PLAN_START, 'YYYY-MM-DD') AS "TARGET_DATE",
    CTD.NCO,
    CAST(AUPM.PCT_COMPLETE AS INTEGER) AS "PERCENT_COMPLETE",
    CASE AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME WHEN 'FIH' THEN FFRISK.PC_PTS 
    WHEN 'PSS' THEN FFRISK.PC_PTS*FFRISK.P1_PTS 
    WHEN 'POC' THEN FFRISK.PC_PTS*FFRISK.P1_PTS*FFRISK.P2A_PTS*FFRISK.P2B_PTS END AS "ENRICH_PTS"
    ,CASE AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME WHEN 'LD' THEN MT.LD_TYP WHEN 'CS' THEN MT."CS_TYP" WHEN 'FIH' THEN MT.FIH_TYP ELSE WTYP.TYP_NUM END AS "INTEL_TYPE"
FROM
    "VAW_AMER_DEV_PUB"."PDANEXUSANALYTICS"."CV_ALLDATA_UNPIVOTED_MILESTONES_SF" AUPM
    INNER JOIN (
        SELECT
            PROJECT_CODE,
            CANDIDATE_MILESTONE_DISPLAY_NAME
        ,MIN(PLAN_FINISH) AS MIN_PLAN_FINISH
        FROM
            "VAW_AMER_DEV_PUB"."PDANEXUSANALYTICS"."CV_ALLDATA_UNPIVOTED_MILESTONES_SF"
        where
            DATE_PART(YEAR, PLAN_START) BETWEEN 2023
            AND 2028
        GROUP BY
            PROJECT_CODE,
            CANDIDATE_MILESTONE_DISPLAY_NAME
    ) INAUPM ON AUPM.PROJECT_CODE = INAUPM.PROJECT_CODE
    AND AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME = INAUPM.CANDIDATE_MILESTONE_DISPLAY_NAME
    AND AUPM.PLAN_FINISH = INAUPM.MIN_PLAN_FINISH
    left join "VAW_AMER_DEV_PUB"."PDANEXUSANALYTICS"."CV_PORTFOLIO_ALL_DATA" PA on AUPM.PROJECT_CODE = PA.PROJECT_CODE
    LEFT JOIN (
        SELECT
            OSCTD.Candidate_Code AS "PROJECT_CODE",
            OSCTD.Candidate_Task_Pcnt_Comp AS "NCO"
        FROM
            "VAW_AMER_DEV_PUB"."PDANEXUSREPORTING"."OS_SRC_CV_CANDIDATE_TASK_DATA" OSCTD
        WHERE
            OSCTD.Candidate_Task_Core_Code = '7565'
    ) CTD ON AUPM.PROJECT_CODE = CTD.PROJECT_CODE
    LEFT JOIN (
        SELECT
            PORTFOLIO_ID,
            CANDIDATE_CODE,
            PTSPRECLIN as PC_PTS,
            "PTSPHI" as P1_PTS,
            "PTSPHIIA" as P2A_PTS,
            "PTSPHIIB" as P2B_PTS
        FROM
            "VAW_AMER_DEV_PUB"."PDANEXUSPIPELINE"."OS_STG_CV_ENRICH_FF_RISK"
        WHERE
            PORTFOLIO_ID = 517.0
    ) FFRISK ON AUPM.PROJECT_CODE = FFRISK.CANDIDATE_CODE
    LEFT JOIN "PDANEXUSANALYTICS"."CV_MEDSCI_TYP" MT ON AUPM.PROJECT_CODE = MT.PROJECT_TRACKING_CODE
    LEFT JOIN "PDANEXUSANALYTICS"."CV_ALLDATA_WRD_TYP" WTYP ON AUPM.PROJECT_CODE = WTYP.CODE AND MILESTONE = WTYP.GOAL
WHERE
    AUPM.CANDIDATE_MILESTONE_DISPLAY_NAME IN (
        'CS',
        'ESoE',
        'FIH',
        'LD',
        'POC',
        'POM',
        'POC Study Start',
        'SDS',
        'SOCA',
        'Pivotal Program Start (PPS)'
    )
    AND (
        PA.STAGE IN (
            'Preclinical',
            'Phase I',
            'ESD',
            'LD',
            'SDS',
            'N/A',
            'Phase IIFD',
            'Registration',
            'Phase IIED',
            'Phase III',
            'Approved'
        )
        OR PA.STAGE IS NULL
    )
    AND (
        PA.STATUS IN ('Ongoing', 'Placeholder')
        OR PA.CURRENT_YR_ACHIEVEMENTS IS NOT NULL
    )
    AND DATE_PART(YEAR, AUPM.PLAN_START) BETWEEN 2023
    AND 2028
    AND PA.UNIT NOT IN ('Dev Japan', 'Dev China')
    AND NOT CONTAINS(PA.PROJECT_SHORT_NAME, 'Tgt')
    AND PA.PROJECT_TYPE <> 'Non-Submission' --AND SM.PORTFOLIO_ID =1873.0
