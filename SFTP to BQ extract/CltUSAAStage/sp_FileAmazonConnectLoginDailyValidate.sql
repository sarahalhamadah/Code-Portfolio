CREATE OR REPLACE PROCEDURE CltUSAAStage.sp_FileAmazonConnectLoginDailyValidate(IN iParentSystemId INT64, IN iSystemId INT64, OUT oRejectedCount INT64)
BEGIN
    DECLARE vSqlValidate STRING;
    DECLARE vSqlSetReturnValues STRING;
    DECLARE vSqlUpdateFileEffectiveDate STRING;

    SET vSqlValidate =
        FORMAT('''
            UPDATE `CltUSAAStage.AmazonConnect-LoginDaily-FileStage-%d` t
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
						WHEN `Login` IS NULL
						THEN 'Login missing value'
						END
						,CASE
						WHEN `Duration` IS NOT NULL AND SAFE_CAST(`Duration` AS NUMERIC) IS NULL
						THEN 'Duration parse error' END
                    ] AS ValidationErrors
                FROM `CltUSAAStage.AmazonConnect-LoginDaily-FileStage-%d`
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
            SELECT COUNT(IF(InactiveInd,1,NULL)) AS RejectedCount
            FROM `CltUSAAStage.AmazonConnect-LoginDaily-FileStage-%d`;'''
           ,iParentSystemId
        );
    EXECUTE IMMEDIATE vSqlSetReturnValues INTO oRejectedCount;
END;