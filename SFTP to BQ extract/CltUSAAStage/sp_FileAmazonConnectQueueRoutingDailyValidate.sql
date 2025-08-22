CREATE OR REPLACE PROCEDURE CltUSAAStage.sp_FileAmazonConnectQueueRoutingDailyValidate(IN iParentSystemId INT64, IN iSystemId INT64, OUT oMinDate DATETIME, OUT oMaxDate DATETIME, OUT oRejectedCount INT64)
OPTIONS(
  strict_mode=false)
BEGIN
    DECLARE vSqlValidate STRING;
    DECLARE vSqlSetReturnValues STRING;
    DECLARE vSqlUpdateFileEffectiveDate STRING;

    SET vSqlValidate =
        FORMAT('''
            UPDATE `CltUSAAStage.AmazonConnect-QueueRoutingDaily-FileStage-%d` t
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
						WHEN `RoutingProfile` IS NULL
						THEN 'RoutingProfile missing value' END
						,CASE
						WHEN `StartInterval` IS NULL
						THEN 'StartInterval missing value'
						WHEN `StartInterval` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`StartInterval` AS TIMESTAMP) AS DATETIME) IS NULL
						THEN 'StartInterval parse error' END
						,CASE
						WHEN `EndInterval` IS NULL
						THEN 'EndInterval missing value'
						WHEN `EndInterval` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`EndInterval` AS TIMESTAMP) AS DATETIME) IS NULL
						THEN 'EndInterval parse error' END
						,CASE
						WHEN `AgentAnswerRate` IS NOT NULL AND SAFE_CAST(`AgentAnswerRate` AS NUMERIC) IS NULL
						THEN 'AgentAnswerRate parse error' END
						,CASE
						WHEN `ContactsMissed` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsMissed` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsMissed parse error' END
						,CASE
						WHEN `ContactsAbandonedIn15Seconds` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsAbandonedIn15Seconds` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsAbandonedIn15Seconds parse error' END
						,CASE
						WHEN `ContactsAnsweredIn30Seconds` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsAnsweredIn30Seconds` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsAnsweredIn30Seconds parse error' END
						,CASE
						WHEN `ContactsAnsweredIn60Seconds` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsAnsweredIn60Seconds` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsAnsweredIn60Seconds parse error' END
						,CASE
						WHEN `ServiceLevel30Seconds` IS NOT NULL AND SAFE_CAST(`ServiceLevel30Seconds` AS NUMERIC) IS NULL
						THEN 'ServiceLevel30Seconds parse error' END
						,CASE
						WHEN `ServiceLevel60Seconds` IS NOT NULL AND SAFE_CAST(`ServiceLevel60Seconds` AS NUMERIC) IS NULL
						THEN 'ServiceLevel60Seconds parse error' END
						,CASE
						WHEN `AfterContactWorkTime` IS NOT NULL AND SAFE_CAST(`AfterContactWorkTime` AS NUMERIC) IS NULL
						THEN 'AfterContactWorkTime parse error' END
						,CASE
						WHEN `AgentInteractionTime` IS NOT NULL AND SAFE_CAST(`AgentInteractionTime` AS NUMERIC) IS NULL
						THEN 'AgentInteractionTime parse error' END
						,CASE
						WHEN `AverageAfterContactWorkTime` IS NOT NULL AND SAFE_CAST(`AverageAftercontactWorkTime` AS NUMERIC) IS NULL
						THEN 'AverageAfterContactWorkTime parse error' END
						,CASE
						WHEN `AverageAgentInteractionTime` IS NOT NULL AND SAFE_CAST(`AverageAgentInteractionTime` AS NUMERIC) IS NULL
						THEN 'AverageAgentInteractionTime parse error' END
						,CASE
						WHEN `AverageHandleTime` IS NOT NULL AND SAFE_CAST(`AverageHandleTime` AS NUMERIC) IS NULL
						THEN 'AverageHandleTime parse error' END
						,CASE
						WHEN `AverageQueueAbandonTime` IS NOT NULL AND SAFE_CAST(`AverageQueueAbandonTime` AS NUMERIC) IS NULL
						THEN 'AverageQueueAbandonTime parse error' END
						,CASE
						WHEN `AverageQueueAnswerTime` IS NOT NULL AND SAFE_CAST(`AverageQueueAnswerTime` AS NUMERIC) IS NULL
						THEN 'AverageQueueAnswerTime parse error' END
						,CASE
						WHEN `ContactHandleTime` IS NOT NULL AND SAFE_CAST(`ContactHandleTime` AS NUMERIC) IS NULL
						THEN 'ContactHandleTime parse error' END
						,CASE
						WHEN `ContactsAbandoned` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsAbandoned` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsAbandoned parse error' END
						,CASE
						WHEN `ContactsAgentHungUpFirst` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsAgentHungUpFirst` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsAgentHungUpFirst parse error' END
						,CASE
						WHEN `ContactsHandled` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsHandled` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsHandled parse error' END
						,CASE
						WHEN `ContactsQueued` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsQueued` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsQueued parse error' END
						,CASE
						WHEN `ContactsTransferredIn` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsTransferredIn` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsTransferredIn parse error' END
						,CASE
						WHEN `ContactsTransferredOut` IS NOT NULL AND SAFE_CAST(SAFE_CAST(`ContactsTransferredOut` AS NUMERIC) AS INT64) IS NULL
						THEN 'ContactsTransferredOut parse error' END
						,CASE
						WHEN `MaximumQueuedTime` IS NOT NULL AND SAFE_CAST(`MaximumQueuedTime` AS NUMERIC) IS NULL
						THEN 'MaximumQueuedTime parse error' END
                    ] AS ValidationErrors
                FROM `CltUSAAStage.AmazonConnect-QueueRoutingDaily-FileStage-%d`
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
            FROM `CltUSAAStage.AmazonConnect-QueueRoutingDaily-FileStage-%d`;'''
           ,iParentSystemId
        );
    EXECUTE IMMEDIATE vSqlSetReturnValues INTO oMinDate, oMaxDate, oRejectedCount;
END;