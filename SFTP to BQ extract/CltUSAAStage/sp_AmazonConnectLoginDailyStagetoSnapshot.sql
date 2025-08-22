CREATE OR REPLACE PROCEDURE CltUSAAStage.sp_AmazonConnectLoginDailyStagetoSnapshot(IN iParentSystemId INT64)
BEGIN

	DECLARE debugMsg STRING;
	DECLARE vSql STRING;

	SET debugMsg = 'Insert data into Snapshot';
	
	
	SET vSql =
    FORMAT('''
		INSERT INTO `CltUSAAOds.AmazonConnect-LoginDaily-FileSnapshot-%d` 
		(	
			ParentSystemId,
			SystemId,
			Cosa,
			Company,
			Agent,
			FirstName,
			LastName,
			RoutingProfile,
			Login,
			Logout,
			Duration,
			LoadBy,
			LoadDate,
			LoadProcess,
			CreateBy,
			CreateDate,
			CreateProcess,
			UpdateBy,
			UpdateDate,
			UpdateProcess,
			FileLoadAuditId,
			LineNum,
			FileEffectiveDate,
			InactiveInd,
			InactiveDate,
			InactiveReason
		) 
		SELECT 
			s.ParentSystemId,
			s.SystemId,
			s.Cosa,
			s.Company,
			s.Agent,
			s.FirstName,
			s.LastName,
			s.RoutingProfile,
			s.Login,
			s.LogOut,
			s.Duration,
			s.LoadBy,
			s.LoadDate,
			s.LoadProcess,
			SESSION_USER() as CreateBy,
			CURRENT_TIMESTAMP() as CreateDate,
			'CltUSAAStage.sp_AmazonConnectLoginDailyStagetoSnapshot_%d' as CreateProcess,
			SESSION_USER() as UpdateBy,
			CURRENT_TIMESTAMP() as UpdateDate,
			'CltUSAAStage.sp_AmazonConnectLoginDailyStagetoSnapshot_%d' as UpdateProcess,
			s.FileLoadAuditId,
			s.LineNum,
			s.FileEffectiveDate,
			false as InactiveInd,
			TIMESTAMP(NULL) as InactiveDate,
			'' as InactiveReason
		FROM `CltUSAAStage.AmazonConnect-LoginDaily-FileStage-%d`  s
	''', iParentSystemId, iParentSystemId, iParentSystemId, iParentSystemId);
	EXECUTE IMMEDIATE vSql;

	EXCEPTION WHEN ERROR THEN
	RAISE USING MESSAGE= CONCAT(
		IFNULL('Failed while ' || debugMsg || '.\n', '')
		,@@error.message, '\n'
		,@@error.statement_text, '\n'
		,@@error.formatted_stack_trace);
END;