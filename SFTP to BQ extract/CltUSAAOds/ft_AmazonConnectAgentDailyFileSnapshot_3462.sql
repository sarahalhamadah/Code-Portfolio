CREATE OR REPLACE TABLE FUNCTION CltUSAAOds.ft_AmazonConnectAgentDailyFileSnapshot_3462(iStartDate DATETIME, iEndDate DATETIME)
AS
SELECT 
	ParentSystemId,
	SystemId,
	`Cosa`,
	`Company`,
	`Agent`,
	`RoutingProfile`,
	DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', SUBSTR(`StartInterval`, 1, 19)))  AS `StartInterval`,
	DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', SUBSTR(`EndInterval`, 1, 19))) AS `EndInterval`,
	CAST(TIMESTAMP(`StartInterval`) AS DATETIME) AS `UtcStartInterval`,
	CAST(TIMESTAMP(`EndInterval`) AS DATETIME) AS `UtcEndInterval`,
	`AgentFirstName`,
	`AgentLastName`,
	CAST(`AgentIdleTime` AS NUMERIC) AS `AgentIdleTime`,
	CAST(`AgentOnContactTime` AS NUMERIC) AS `AgentOnContactTime`,
	CAST(`NonproductiveTime` AS NUMERIC) AS `NonproductiveTime`,
	CAST(`Occupancy` AS NUMERIC) AS `Occupancy`,
	CAST(`OnlineTime` AS NUMERIC) AS `OnlineTime`,
	CAST(`BackOffSptInsSptIntradiemTime` AS NUMERIC) AS `BackOffSptInsSptIntradiemTime`,
	CAST(`DevelopmentLabActivitiesTime` AS NUMERIC) AS `DevelopmentLabActivitiesTime`,
	CAST(`CATClaimsOnlyTime` AS NUMERIC) AS `CATClaimsOnlyTime`,
	CAST(`ComputerITIssuesHelpDeskTime` AS NUMERIC) AS `ComputerITIssuesHelpDeskTime`,
	CAST(`PersonalUnscheduledBreakTime` AS NUMERIC) AS `PersonalUnscheduledBreakTime`,
	CAST(`OutboundCallingTime` AS NUMERIC) AS `OutboundCallingTime`,
	CAST(`MemberSupportTime` AS NUMERIC) AS `MemberSupportTime`,
	CAST(`MeetingTime` AS NUMERIC) AS `MeetingTime`,
	CAST(`CoachingTime` AS NUMERIC) AS `CoachingTime`,
	CAST(`AfterContactWorkTime` AS NUMERIC) AS `AfterContactWorkTime`,
	CAST(`AgentInteractionTime` AS NUMERIC) AS `AgentInteractionTime`,
	CAST(`AverageAfterContactWorkTime` AS NUMERIC) AS `AverageAfterContactWorkTime`,
	CAST(`AverageAgentInteractionTime` AS NUMERIC) AS `AverageAgentInteractionTime`,
	CAST(`AverageHandleTime` AS NUMERIC) AS `AverageHandleTime`,
	CAST(`ContactHandleTime` AS NUMERIC) AS `ContactHandleTime`,
	CAST(CAST(`ContactsAgentHungUpFirst` AS NUMERIC) AS INT64) AS `ContactsAgentHungUpFirst`,
	CAST(CAST(`ContactsHandled` AS NUMERIC) AS INT64) AS `ContactsHandled`,
	CAST(CAST(`ContactsTransferredOut` AS NUMERIC) AS INT64) AS `ContactsTransferredOut`,
	FileEffectiveDate,
	FileLoadAuditId,
	InactiveInd
FROM `CltUSAAOds.AmazonConnect-AgentDaily-FileSnapshot-3462`
WHERE NOT InactiveInd
AND SnapshotRowDateTime >=iStartDate
AND SnapshotRowDateTime <=iEndDate
QUALIFY ROW_NUMBER() OVER (PARTITION BY StartInterval,EndInterval,Cosa,Company,Agent,RoutingProfile ORDER BY FileEffectiveDate DESC, CreateDate DESC) = 1;