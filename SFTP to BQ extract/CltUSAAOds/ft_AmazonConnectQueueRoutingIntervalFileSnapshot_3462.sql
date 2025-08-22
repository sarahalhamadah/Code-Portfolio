CREATE OR REPLACE TABLE FUNCTION CltUSAAOds.ft_AmazonConnectQueueRoutingIntervalFileSnapshot_3462(iStartDate DATETIME, iEndDate DATETIME)

AS
SELECT 
	ParentSystemId,
	SystemId,
	`Cosa`,
	`Company`,
	`Queue`,
	`RoutingProfile`,
	DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', SUBSTR(`StartInterval`, 1, 19)))  AS `StartInterval`,
	DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', SUBSTR(`EndInterval`, 1, 19))) AS `EndInterval`,
	CAST(TIMESTAMP(`StartInterval`) AS DATETIME) AS `UtcStartInterval`,
	CAST(TIMESTAMP(`EndInterval`) AS DATETIME) AS `UtcEndInterval`,
	CAST(`AgentAnswerRate` AS NUMERIC) AS `AgentAnswerRate`,
	CAST(CAST(`ContactsMissed` AS NUMERIC) AS INT64) AS `ContactsMissed`,
	CAST(CAST(`ContactsAbandonedIn15Seconds` AS NUMERIC) AS INT64) AS `ContactsAbandonedIn15Seconds`,
	CAST(CAST(`ContactsAnsweredIn30Seconds` AS NUMERIC) AS INT64) AS `ContactsAnsweredIn30Seconds`,
	CAST(CAST(`ContactsAnsweredIn60Seconds` AS NUMERIC) AS INT64) AS `ContactsAnsweredIn60Seconds`,
	CAST(`ServiceLevel30Seconds` AS NUMERIC) AS `ServiceLevel30Seconds`,
	CAST(`ServiceLevel60Seconds` AS NUMERIC) AS `ServiceLevel60Seconds`,
	CAST(`AfterContactWorkTime` AS NUMERIC) AS `AfterContactWorkTime`,
	CAST(`AgentInteractionTime` AS NUMERIC) AS `AgentInteractionTime`,
	CAST(`AverageAfterContactWorkTime` AS NUMERIC) AS `AverageAfterContactWorkTime`,
	CAST(`AverageAgentInteractionTime` AS NUMERIC) AS `AverageAgentInteractionTime`,
	CAST(`AverageHandleTime` AS NUMERIC) AS `AverageHandleTime`,
	CAST(`AverageQueueAbandonTime` AS NUMERIC) AS `AverageQueueAbandonTime`,
	CAST(`AverageQueueAnswerTime` AS NUMERIC) AS `AverageQueueAnswerTime`,
	CAST(`ContactHandleTime` AS NUMERIC) AS `ContactHandleTime`,
	CAST(CAST(`ContactsAbandoned` AS NUMERIC) AS INT64) AS `ContactsAbandoned`,
	CAST(CAST(`ContactsAgentHungUpFirst` AS NUMERIC) AS INT64) AS `ContactsAgentHungUpFirst`,
	CAST(CAST(`ContactsHandled` AS NUMERIC) AS INT64) AS `ContactsHandled`,
	CAST(CAST(`ContactsQueued` AS NUMERIC) AS INT64) AS `ContactsQueued`,
	CAST(CAST(`ContactsTransferredIn` AS NUMERIC) AS INT64) AS `ContactsTransferredIn`,
	CAST(CAST(`ContactsTransferredOut` AS NUMERIC) AS INT64) AS `ContactsTransferredOut`,
	CAST(`MaximumQueuedTime` AS NUMERIC) AS `MaximumQueuedTime`,
	FileEffectiveDate,
	FileLoadAuditId,
	InactiveInd
FROM `CltUSAAOds.AmazonConnect-QueueRoutingInterval-FileSnapshot-3462`
WHERE NOT InactiveInd
AND SnapshotRowDateTime >=iStartDate
AND SnapshotRowDateTime <=iEndDate
QUALIFY ROW_NUMBER() OVER(PARTITION BY StartInterval,EndInterval,Cosa,Company,Queue,RoutingProfile ORDER BY FileEffectiveDate DESC, CreateDate DESC) = 1;