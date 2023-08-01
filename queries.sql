DROP VIEW IssuesByMonth;	
CREATE VIEW IssuesByMonth AS	
SELECT A.Month, CumulativeApps, Fixed, New, Existing, Reopen, Total, 	
 B.ActiveAppsWithinMonth, Apps as DistinctApps	
FROM (	
select  SUBSTR(FirstSeenDate,1,7)||'-01' as Month,	
count(distinct Application||Microservice) as Apps,	
SUM(IIF(Status='New', 1, 0)) as New,	
SUM(IIF(Status='Existing', 1, 0)) as Existing,	
SUM(IIF(Status='Fix Validated', 1, 0)) as Fixed,	
SUM(IIF(Status='Reopen', 1, 0)) as Reopen,	
SUM(1) as Total	
from issues	
group by 1	
) A LEFT JOIN AppsByMonth B ON A.Month=B.Month	
	
	
DROP VIEW AppsByMonth;	
CREATE VIEW AppsByMonth AS	
Select A.Month, count(B.ApplicationID) as CumulativeApps, sum(IIF(A.month <= B.LastScan, 1,0)) as ActiveAppsWithinMonth	
From	
(	
    select  SUBSTR(CompletedDate,1,7)||'-01' as Month, count(1) as scans	
    from scans	
    where Completed is not NULL	
    group by 1 	
) A LEFT JOIN (	
    select  Application, ApplicationID, 	
    SUBSTR(date(min(Completed)/1000,'unixepoch') ,1,7)||'-01' as FirstScan,	
    SUBSTR(date(max(Completed)/1000,'unixepoch') ,1,7)||'-01' as LastScan	
    from scans	
    where Completed is not NULL	
    group by Application, ApplicationID	
) B on A.month >= B.FirstScan 	
group by A.month	
	
SELECT * FROM AppsByMonth	
SELECT * FROM IssuesByMonth	


DROP VIEW ScansByMonth;
CREATE VIEW ScansByMonth AS
SELECT A.*, B.CumulativeApps, B.ActiveAppsWithinMonth
FROM (
SELECT SUBSTR(CompletedDate,1,7)||'-01' as Month, count(1) as Scans, sum(IFNULL(Subscription,0)) as SubscriptionScans, 


FROM scans
WHERE Completed IS NOT NULL
GROUP BY 1
) A LEFT JOIN AppsByMonth B ON A.Month=B.Month

SELECT * FROM ScansByMonth