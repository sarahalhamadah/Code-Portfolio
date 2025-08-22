CREATE OR REPLACE PROCEDURE CltUSAAStage.sp_FileAmazonConnectAgentQueueDailyValidate(IN iParentSystemId INT64, IN iSystemId INT64, OUT oMinDate DATETIME, OUT oMaxDate DATETIME, OUT oRejectedCount INT64)
OPTIONS(
  strict_mode=false)
BEGIN
    DECLARE vSqlValidate STRING;
    DECLARE vSqlSetReturnValues STRING;
    DECLARE vSqlUpdateFileEffectiveDate STRING;

    SET vSqlValidate =
        FORMAT('''
            UPDATE `CltUSAAStage.AmazonConnect-AgentQueueDaily-FileStage-%d` t
            SET InactiveInd = TRUE
               ,InactiveDate = CURRENT_TIMESTAMP
               ,InactiveReason = 'Validation failure: ' || ARRAY_TO_STRING(s.ValidationErrors, ', ')
            FROM (
                SELECT FileLoadAuditId, LineNum
                   ,[
						CASE
						WHEN `Cosa` IS NULL
						THEN 'Cosa missing value' END
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
						WHEN `AgentFirstName` IS NULL
						THEN 'AgentFirstName missing value' END
						,CASE
						WHEN `AgentLastName` IS NULL
						THEN 'AgentLastName missing value' END
						,CASE
						WHEN `AfterContactWorkTime` IS NOT NULL AND SAFE_CAST(`AfterContactWorkTime` AS NUMERIC) IS NULL
						THEN 'AfterContactWorkTime parse error' END
						,CASE
						WHEN `AgentInteractionTime` IS NOT NULL AND SAFE_CAST(`AgentInteractionTime` AS NUMERIC) IS NULL
						THEN 'AgentInteractionTime parse error' END
						,CASE
						WHEN `AverageAfterContactWorkTime` IS NOT NULL AND SAFE_CAST(`AverageAfterContactWorkTime` AS NUMERIC) IS NULL
						THEN 'AverageAfterContactWorkTime parse error' END
						,CASE
						WHEN `AverageAgentInteractionTime` IS NOT NULL AND SAFE_CAST(`AverageAgentInteractionTime` AS NUMERIC) IS NULL
						THEN 'AverageAgentInteractionTime parse error' END
						,CASE
						WHEN `AverageHandleTime` IS NOT NULL AND SAFE_CAST(`AverageHandleTime` AS NUMERIC) IS NULL
						THEN 'AverageHandleTime parse error' END
						,CASE
						WHEN `ContactHandleTime` IS NOT NULL AND SAFE_CAST(`ContactHandleTime` AS NUMERIC) IS NULL
						THEN 'ContactHandleTime parse error' END
						,CASE
						WHEN `ContactsAgentHungUpFirst` IS NOT NULL AND SAFE_CAST(CAST(`ContactsAgentHungUpFirst` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsAgentHungUpFirst parse error' END
						,CASE
						WHEN `ContactsHandled` IS NOT NULL AND SAFE_CAST(CAST(`ContactsHandled` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsHandled parse error' END
						,CASE
						WHEN `ContactsTransferredOut` IS NOT NULL AND SAFE_CAST(CAST(`ContactsTransferredOut` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsTransferredOut parse error' END
                    ] AS ValidationErrors
                FROM `CltUSAAStage.AmazonConnect-AgentQueueDaily-FileStage-%d`
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
            FROM `CltUSAAStage.AmazonConnect-AgentQueueDaily-FileStage-%d`;'''
           ,iParentSystemId
        );
    EXECUTE IMMEDIATE vSqlSetReturnValues INTO oMinDate, oMaxDate, oRejectedCount;
END;