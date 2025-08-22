CREATE OR REPLACE PROCEDURE CltUSAAStage.sp_AmazonConnectAgentQueueIntervalStagetoSnapshot(IN iParentSystemId INT64)
BEGIN


	DECLARE debugMsg STRING;
	DECLARE vSql STRING;

	SET debugMsg = 'Insert data into Snapshot';
	
	
	SET vSql =
    FORMAT('''
		INSERT INTO `CltUSAAOds.AmazonConnect-AgentQueueInterval-FileSnapshot-%d` 
		(	
			ParentSystemId,
			SystemId,
			Cosa,
			Company,
			Agent,
			RoutingProfile,
			Queue,
			StartInterval,
			EndInterval,
			AgentFirstName,
			AgentLastName,
			AfterContactWorkTime,
			AgentInteractionTime,
			AverageAfterContactWorkTime,
			AverageAgentInteractionTime,
			AverageHandleTime,
			ContactHandleTime,
			ContactsAgentHungUpFirst,
			ContactsHandled,
			ContactsTransferredOut,
			SnapshotRowDateTime,
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
			s.RoutingProfile,
			s.Queue,
			s.StartInterval,
			s.EndInterval,
			s.AgentFirstName,
			s.AgentLastName,
			s.AfterContactWorkTime,
			s.AgentInteractionTime,
			s.AverageAfterContactWorkTime,
			s.AverageAgentInteractionTime,
			s.AverageHandleTime,
			s.ContactHandleTime,
			s.ContactsAgentHungUpFirst,
			s.ContactsHandled,
			s.ContactsTransferredOut,
			DATETIME(SAFE.PARSE_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%E*S', SUBSTR(s.StartInterval, 1, 19))) SnapshotRowDateTime,
			s.LoadBy,
			s.LoadDate,
			s.LoadProcess,
			SESSION_USER() as CreateBy,
			CURRENT_TIMESTAMP() as CreateDate,
			'CltUSAAStage.sp_AmazonConnectAgentQueueIntervalStagetoSnapshot_%d' as CreateProcess,
			SESSION_USER() as UpdateBy,
			CURRENT_TIMESTAMP() as UpdateDate,
			'CltUSAAStage.sp_AmazonConnectAgentQueueIntervalStagetoSnapshot_%d' as UpdateProcess,
			s.FileLoadAuditId,
			s.LineNum,
			s.FileEffectiveDate,
			false as InactiveInd,
			TIMESTAMP(NULL) as InactiveDate,
			'' as InactiveReason
		FROM `CltUSAAStage.AmazonConnect-AgentQueueInterval-FileStage-%d`  s;
	''', iParentSystemId, iParentSystemId, iParentSystemId, iParentSystemId);
	EXECUTE IMMEDIATE vSql;

	EXCEPTION WHEN ERROR THEN
	RAISE USING MESSAGE= CONCAT(
		IFNULL('Failed while ' || debugMsg || '.\n', '')
		,@@error.message, '\n'
		,@@error.statement_text, '\n'
		,@@error.formatted_stack_trace);
END;