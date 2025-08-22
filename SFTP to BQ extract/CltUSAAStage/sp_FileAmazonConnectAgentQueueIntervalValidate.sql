CREATE OR REPLACE PROCEDURE CltUSAAStage.sp_FileAmazonConnectAgentQueueIntervalValidate(IN iParentSystemId INT64, IN iSystemId INT64, OUT oMinDate DATETIME, OUT oMaxDate DATETIME, OUT oRejectedCount INT64)
OPTIONS(
  strict_mode=false)
BEGIN
    DECLARE vSqlValidate STRING;
    DECLARE vSqlSetReturnValues STRING;
    DECLARE vSqlUpdateFileEffectiveDate STRING;

    SET vSqlValidate =
        FORMAT('''
            UPDATE `CltUSAAStage.AmazonConnect-AgentQueueInterval-FileStage-%d` t
            SET InactiveInd = TRUE
               ,InactiveDate = CURRENT_TIMESTAMP
               ,InactiveReason = 'Validation failure: ' || ARRAY_TO_STRING(s.ValidationErrors, ', ')
            FROM (
                SELECT FileLoadAuditId, LineNum
                   ,[
						CASE
						WHEN `CoSA` IS NULL
						THEN 'CoSA missing value' END
						,CASE
						WHEN `Agent` IS NULL
						THEN 'Agent missing value' END
						,CASE
						WHEN `RoutingProfile` IS NULL
						THEN 'RoutingProfile missing value' END
						,CASE
						WHEN `StartInterval` IS NULL
						THEN 'StartInterval missing value'
						WHEN `StartInterval` IS NOT NULL AND SAFE_CAST(TIMESTAMP(`StartInterval`) AS DATETIME) IS NULL
						THEN 'StartInterval parse error' END
						,CASE
						WHEN `EndInterval` IS NULL
						THEN 'EndInterval missing value'
						WHEN `EndInterval` IS NOT NULL AND SAFE_CAST(TIMESTAMP(`EndInterval`) AS DATETIME) IS NULL
						THEN 'EndInterval parse error' END
						,CASE
						WHEN `Aftercontactworktime` IS NOT NULL AND SAFE_CAST(`Aftercontactworktime` AS NUMERIC) IS NULL
						THEN 'Aftercontactworktime parse error' END
						,CASE
						WHEN `Agentinteractiontime` IS NOT NULL AND SAFE_CAST(`Agentinteractiontime` AS NUMERIC) IS NULL
						THEN 'Agentinteractiontime parse error' END
						,CASE
						WHEN `Averageaftercontactworktime` IS NOT NULL AND SAFE_CAST(`Averageaftercontactworktime` AS NUMERIC) IS NULL
						THEN 'Averageaftercontactworktime parse error' END
						,CASE
						WHEN `Averageagentinteractiontime` IS NOT NULL AND SAFE_CAST(`Averageagentinteractiontime` AS NUMERIC) IS NULL
						THEN 'Averageagentinteractiontime	 parse error' END
						,CASE
						WHEN `Averagehandletime` IS NOT NULL AND SAFE_CAST(`Averagehandletime` AS NUMERIC) IS NULL
						THEN 'Averagehandletime parse error' END
						,CASE
						WHEN `Contacthandletime` IS NOT NULL AND SAFE_CAST(`Contacthandletime` AS NUMERIC) IS NULL
						THEN 'Contacthandletime parse error' END
						,CASE
						WHEN `Contactsagenthungupfirst` IS NOT NULL AND SAFE_CAST(CAST(`Contactsagenthungupfirst` AS NUMERIC) AS INT64) IS NULL
						THEN 'Contactsagenthungupfirst parse error' END
						,CASE
						WHEN `Contactshandled` IS NOT NULL AND SAFE_CAST(CAST(`Contactshandled` AS NUMERIC) AS INT64) IS NULL
						THEN 'Contactshandled parse error' END
						,CASE
						WHEN `Contactstransferredout` IS NOT NULL AND SAFE_CAST(CAST(`Contactstransferredout` AS NUMERIC) AS INT64) IS NULL
						THEN 'Contactstransferredout parse error' END
                    ] AS ValidationErrors
                FROM `CltUSAAStage.AmazonConnect-AgentQueueInterval-FileStage-%d`
            ) s
            WHERE s.FileLoadAuditId = t.FileLoadAuditId
                AND s.LineNum = t.LineNum
                AND LENGTH(ARRAY_TO_STRING(s.ValidationErrors, ', ')) > 0
            ;'''
           ,iParentSystemId, iParentSystemId
        );
    EXECUTE IMMEDIATE vSqlValidate;

    SET vSqlSetReturnValues =
        FORMAT('''
            SELECT MIN(IF(InactiveInd, NULL, SAFE_CAST(StartInterval AS DATETIME)))
               ,MAX(IF(InactiveInd, NULL, SAFE_CAST(StartInterval AS DATETIME)))
               ,COUNT(IF(InactiveInd,1,NULL)) AS RejectedCount
            FROM `CltUSAAStage.AmazonConnect-AgentQueueInterval-FileStage-%d`;'''
           ,iParentSystemId
        );
    EXECUTE IMMEDIATE vSqlSetReturnValues INTO oMinDate, oMaxDate, oRejectedCount;
END;