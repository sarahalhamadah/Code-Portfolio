CREATE OR REPLACE PROCEDURE CltUSAAStage.sp_AmazonConnectQueueRoutingIntervalStagetoSnapshot(IN iParentSystemId INT64)
BEGIN

	DECLARE debugMsg STRING;
	DECLARE vSql STRING;

	SET debugMsg = 'Insert data into Snapshot';
	
	
	SET vSql =
    FORMAT('''
		INSERT INTO `CltUSAAOds.AmazonConnect-QueueRoutingInterval-FileSnapshot-%d` 
		(	
			ParentSystemId,
			SystemId,
			Cosa,
			Company,
			Queue,
			RoutingProfile,
			StartInterval,
			EndInterval,
			AgentAnswerRate,
			ContactsMissed,
			ContactsAbandonedIn15Seconds,
			ContactsAnsweredIn30Seconds,
			ContactsAnsweredIn60Seconds,
			ServiceLevel30Seconds,
			ServiceLevel60Seconds,
			AfterContactWorkTime,
			AgentInteractionTime,
			AverageAftercontactWorkTime,
			AverageAgentInteractionTime,
			AverageHandleTime,
			AverageQueueAbandonTime,
			AverageQueueAnswerTime,
			ContactHandleTime,
			ContactsAbandoned,
			ContactsAgentHungUpFirst,
			ContactsHandled,
			ContactsQueued,
			ContactsTransferredIn,
			ContactsTransferredOut,
			MaximumQueuedTime,
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
			s.Queue,
			s.RoutingProfile,
			s.StartInterval,
			s.EndInterval,
			s.AgentAnswerRate,
			s.ContactsMissed,
			s.ContactsAbandonedIn15Seconds,
			s.ContactsAnsweredIn30Seconds,
			s.ContactsAnsweredIn60Seconds,
			s.ServiceLevel30Seconds,
			s.ServiceLevel60Seconds,
			s.AfterContactWorkTime,
			s.AgentInteractionTime,
			s.AverageAfterContactWorkTime,
			s.AverageAgentInteractionTime,
			s.AverageHandleTime,
			s.AverageQueueAbandonTime,
			s.AverageQueueAnswerTime,
			s.ContactHandleTime,
			s.ContactsAbandoned,
			s.ContactsAgentHungUpFirst,
			s.ContactsHandled,
			s.ContactsQueued,
			s.ContactsTransferredIn,
			s.ContactsTransferredOut,
			s.MaximumQueuedTime,
			DATETIME(SAFE.PARSE_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%E*S', SUBSTR(s.StartInterval, 1, 19))) SnapshotRowDateTime,
			s.LoadBy,
			s.LoadDate,
			s.LoadProcess,
			SESSION_USER() as CreateBy,
			CURRENT_TIMESTAMP() as CreateDate,
			'CltUSAAStage.sp_AmazonConnectQueueRoutingIntervalStagetoSnapshot_%d' as CreateProcess,
			SESSION_USER() as UpdateBy,
			CURRENT_TIMESTAMP() as UpdateDate,
			'CltUSAAStage.sp_AmazonConnectQueueRoutingIntervalStagetoSnapshot_%d' as UpdateProcess,
			s.FileLoadAuditId,
			s.LineNum,
			s.FileEffectiveDate,
			false as InactiveInd,
			TIMESTAMP(NULL) as InactiveDate,
			'' as InactiveReason 
		FROM `CltUSAAStage.AmazonConnect-QueueRoutingInterval-FileStage-%d` s;
	''', iParentSystemId, iParentSystemId, iParentSystemId, iParentSystemId);
	EXECUTE IMMEDIATE vSql;

	EXCEPTION WHEN ERROR THEN
	RAISE USING MESSAGE= CONCAT(
		IFNULL('Failed while ' || debugMsg || '.\n', '')
		,@@error.message, '\n'
		,@@error.statement_text, '\n'
		,@@error.formatted_stack_trace);
END;