--insert into OdsLAProID values ('zc_amsch_seed', 50000000)
--update ODSLAProID set identityvalue = 50000000 where seedname = 'zc_amsch_seed'
--dbcc checkident('LAPro.ZC_AMSCH_EXPORT', RESEED, 50000000)

if object_ID('tempdb..#zas') is not null
	drop table #zas
create table #zas
(
	[ID] [int] not null,
	[LoanID] [varchar](15) NULL,
	[PaymentDate] [datetime] NULL,
	[PrincipalPayment] [float] NULL,
	[InterestPayment] [float] NULL,
	[LateFeePayment] [float] NULL,
	[NSFPayment] [float] NULL,
	[TotalPayment] [float] NULL,
	--[sk] [int] IDENTITY(1, 1) NOT FOR REPLICATION NOT NULL,
	[HashValue] [binary](16) NULL,
	[CreatedAt] [smalldatetime] NOT NULL,
	[UpdatedAt] [smalldatetime] NOT NULL
)

insert into #zas
select -1 as ID,
	loan__loan_account_external_id__c as [LoanID]
    ,loan__due_date__c as [PaymentDate]
    ,loan__due_principal__c as [PrincipalPayment]
    ,loan__due_interest__c as [InterestPayment]
    ,0 as [LateFeePayment]
    ,0 as [NSFPayment]
    ,isnull(loan__due_principal__c,0) + isnull(loan__due_interest__c,0) as [TotalPayment]
	,null as HashValue
    ,A.createddate as [CreatedAt]
    ,A.lastmodifieddate as [UpdatedAt]
from [CLS].[cls_migration].[public].[sf_loan__repayment_schedule__c] A
left join [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC on A.loan__loan_account__c = SC.id
where A.delete_flag = 'N' and A.isdeleted = 'false' and SC.delete_flag = 'N' and SC.isdeleted = 'false'
order by loanID, PaymentDate, PrincipalPayment desc

update #zas set HashValue = HASHBYTES('MD5', ISNULL(RTRIM(LoanID),'')+ISNULL(RTRIM(CONVERT(varchar(10),PaymentDate,112)),'')+ISNULL(RTRIM(PrincipalPayment),'')+
                                                 ISNULL(RTRIM(InterestPayment),'')+ISNULL(RTRIM(LateFeePayment),'')+ISNULL(RTRIM(NSFPayment),'')+ISNULL(RTRIM(TotalPayment),''))
if object_ID('tempdb..#zasfillsk') is not null
	drop table #zasfillsk
create table #zasfillsk
(
	[ID] [int] identity (1,1) NOT NULL
	,[HashValue] [binary] (16)
)

declare @zc_amsch_seed int
select @zc_amsch_seed = identityvalue from OdsLAProID where seedname = 'zc_amsch_seed'
dbcc checkident('#zasfillsk', RESEED, @zc_amsch_seed)

update A set A.id = B.id from #zas A, [LAPro].[ZC_AMSCH_EXPORT] B where A.HashValue = B.HashValue 

insert into #zasfillsk
select a.HashValue from #zas A left outer join [LAPro].[ZC_AMSCH_EXPORT] B on A.HashValue = B.HashValue where B.HashValue is null


update A set A.ID = B.ID from #zas A, #zasfillsk B where A.HashValue = B.HashValue

begin tran UpdateInfo;

	--select * from #zas
	DECLARE @ActionResult TABLE (MergeAction VARCHAR(20))

	MERGE INTO LAPro.ZC_AMSCH_EXPORT WITH (HOLDLOCK) AS trg    -- clusterd on ID, non-clustered on LoanID
		USING (SELECT ID
					   ,LoanID
					   ,PaymentDate
					   ,PrincipalPayment
					   ,InterestPayment
					   ,LateFeePayment
					   ,NSFPayment
					   ,TotalPayment
					   ,HashValue
					   ,CreatedAt
					   ,UpdatedAt
				 FROM #zas
		) AS src ON (src.HashValue=trg.HashValue)
		WHEN MATCHED   -- this is a "no change"
			THEN UPDATE SET trg.ID = src.ID   -- src ID changes even though no data has changed
		WHEN NOT MATCHED BY TARGET   -- in src, not trg
			THEN INSERT ( ID
					   ,LoanID
					   ,PaymentDate
					   ,PrincipalPayment
					   ,InterestPayment
					   ,LateFeePayment
					   ,NSFPayment
					   ,TotalPayment
					   ,HashValue
					   ,CreatedAt
					   ,UpdatedAt
					  )
			   VALUES (src.ID
					  ,src.LoanID
					  ,src.PaymentDate
					  ,src.PrincipalPayment
					  ,src.InterestPayment
					  ,src.LateFeePayment
					  ,src.NSFPayment
					  ,src.TotalPayment
					  ,src.HashValue
					  ,src.CreatedAt
					  ,src.UpdatedAt
					  )
		WHEN NOT MATCHED BY SOURCE and trg.id >= 50000000   -- in trg, not src
			THEN DELETE

		OUTPUT $action INTO @ActionResult;

	select @zc_amsch_seed = IDENT_CURRENT ('#zasfillsk') + 1
	update OdsLAProID set identityvalue = @zc_amsch_seed where seedname = 'zc_amsch_seed'

COMMIT TRAN UpdateInfo;
	                                                                                             -- note: update is delete & insert
    SELECT InsertCnt  = IsNULL(SUM(CASE WHEN MergeAction ='INSERT' THEN 1 ELSE 0 END),0)  -- inserts & updates fall under this "INSERT" action
             -- ,@UpdateCnt  = IsNULL(SUM(CASE WHEN MergeAction ='UPDATE' THEN 1 ELSE 0 END),0)  -- these aren't true updates
                ,DeleteCnt  = IsNULL(SUM(CASE WHEN MergeAction ='DELETE' THEN 1 ELSE 0 END),0)  -- deletes & updates fall under this "DELETE" action
          FROM @ActionResult

--delete from  LAPro.ZC_AMSCH_EXPORT 
--truncate table LAPro.ZC_AMSCH_EXPORT 
select * from LAPro.ZC_AMSCH_EXPORT 
