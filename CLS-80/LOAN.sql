if object_ID('tempdb..#LOAN') is not null
	drop table #LOAN
create table #LOAN
(
	[LoanID] [varchar](15) NOT NULL,
	[CustomerID] [varchar](15) NULL,
	[AddressID] [varchar](20) NULL,
	[DateOfLoan] [datetime] NULL,
	[PortfolioID] [varchar](15) NULL,
	[LoanOfficer] [varchar](64) NULL,
	[DateOfAdjustment] [datetime] NULL,
	[RefinanceDate] [datetime] NULL,
	[MaturityDate] [datetime] NULL,
	[Rate] [float] NULL,
	[OriginalAmount] [money] NULL,
	[BillCycleCode2] [varchar](15) NULL,
	[Classification] [varchar](64) NULL,
	[CurrencyCode] [varchar](3) NULL,
	[PrincipalBalance] [money] NULL,
	[PaidOffDate] [datetime] NULL,
	[DueDay] [int] NULL,
	[BillingAmt] [money] NULL,
	[DebitBillAutomatic] [varchar](50) NULL,
	[ACHAccountNumber] [varchar](50) NULL,
	[RoutingNumber] [varchar](50) NULL,
	[AccountType] [varchar](10) NULL,
	[LastPaymentEffectiveDate] [datetime] NULL,
	[InterestReceivable] [money] NULL,
	[CutoffDay] [int] NULL,
	[PAInterestRate] [float] NULL,
	[HashValue] [binary](16) NULL,
	[CreatedAt] [smalldatetime] NULL,
	[UpdatedAt] [smalldatetime] NULL,
	[NextDueDate] [datetime] NULL,
	[NextPrintDate] [datetime] NULL,
	[LoanSource] [varchar](10) NOT NULL
)

insert into #LOAN
select 
	coalesce(application_external_id__c,SC.id) as [LoanID]
	,account_external_id__c as [CustomerID]
	,Sa.id as [AddressID]
	,loan__application_date__c as [DateOfLoan]
    ,portfolioid_migration__c as [PortfolioID]
	,coalesce(SGA.payoff_uid__c, newid()) as [LoanOfficer]
	,null as [DateOfAdjustment]
	,null as [RefinanceDate]
--,[DateOfAdjustment]
--,[RefinanceDate]
    ,loan__rate_change_dt_next__c as [MaturityDate]
    ,SC.loan__interest_rate__c as [Rate]
    ,loan__loan_amount__c as [OriginalAmount]
    ,'Monthly' as [BillCycleCode2] -- hardcoded.
	,null as [Classification]
--,[Classification]
    ,'USD' as [CurrencyCode] -- hardcoded.
    ,loan__principal_remaining__c as [PrincipalBalance]
	,null as [PaidOffDate]
	,null as [DueDay]
--,[PaidOffDate]
--,[DueDay]
    ,loan__Pmt_Amt_Cur__c as [BillingAmt]
	,case when SC.loan__payment_mode__c = 'a2dg0000003g1aDAAQ' then 'Off' else 'Ach On' end as [DebitBillAutomatic]
	,loan__bank_account_number__c as [ACHAccountNumber]
    ,loan__routing_number__c as [RoutingNumber]
    ,SLBA.loan__account_type__c as [AccountType]
	,null as [LastPaymentEffectiveDate]
--,[LastPaymentEffectiveDate]
    ,loan__interest_remaining__c as [InterestReceivable]
    ,loan__due_day__c as [CutoffDay]
    ,SC.loan__interest_rate__c as [PAInterestRate]
	,null as [HashValue]
    ,SC.createddate as [CreatedAt]
    ,SC.lastmodifieddate as [UpdatedAt]
    ,loan__next_installment_date__c as [NextDueDate]
    ,loan__next_due_generation_date__c as [NextPrintDate]
	,'CLS' AS [LoanSource]
 from [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC 
	left join [CLS].[cls_migration].[public].[sf_genesis__applications__c] SGA on SGA.id = SC.application__c and SGA.delete_flag = 'N' and SGA.isdeleted = 'false'
	left join [CLS].[cls_migration].[public].[sf_account] SA on SA.id  = SGA.genesis__account__c and SA.delete_flag = 'N' and SA.isdeleted = 'false'
	left join [CLS].[cls_migration].[public].[sf_loan__bank_account__c] SLBA on SLBA.loan__account__c = SA.id and SLBA.delete_flag = 'N' and SLBA.isdeleted = 'false'
	where SC.delete_flag = 'N' and SC.isdeleted = 'false' 
	and sa.id is not null
	order by SGA.application_external_id__c 


update #LOAN set HashValue = HASHBYTES('MD5',ISNULL(RTRIM(LoanID),'')+ISNULL(RTRIM(CustomerID),'')+ISNULL(RTRIM(AddressID),'')+ISNULL(RTRIM(CONVERT(varchar(10),DateOfLoan,112)),'')+
                                              ISNULL(RTRIM(PortfolioID),'')+ISNULL(RTRIM(LoanOfficer),'')+ISNULL(RTRIM(CONVERT(varchar(10),DateOfAdjustment,112)),'')+ISNULL(RTRIM(CONVERT(varchar(10),RefinanceDate,112)),'')+
                                              ISNULL(RTRIM(CONVERT(varchar(10),MaturityDate,112)),'')+ISNULL(RTRIM(Rate),'')+ISNULL(RTRIM(OriginalAmount),'')+ISNULL(RTRIM(BillCycleCode2),'')+ISNULL(RTRIM(Classification),'')+
                                              ISNULL(RTRIM(CurrencyCode),'')+ISNULL(RTRIM(PrincipalBalance),'')+ISNULL(RTRIM(CONVERT(varchar(10),PaidOffDate,112)),'')+ISNULL(RTRIM(CONVERT(varchar(10),DueDay)),'')+
                                              ISNULL(RTRIM(BillingAmt),'')+ISNULL(RTRIM(DebitBillAutomatic),'')+ISNULL(RTRIM(ACHAccountNumber),'')+ISNULL(RTRIM(RoutingNumber),'')+ISNULL(RTRIM(AccountType),'')+
                                              ISNULL(RTRIM(CONVERT(varchar(10),LastPaymentEffectiveDate,112)),'')+ISNULL(RTRIM(InterestReceivable),'')+ISNULL(RTRIM(CONVERT(varchar(10),CutoffDay)),'')+
											  ISNULL(RTRIM(PAInterestRate),'')+ISNULL(RTRIM(CONVERT(varchar(10), NextDueDate, 112)), '')+ISNULL(RTRIM(CONVERT(varchar(10), NextPrintDate, 112)), ''))
                                              --ISNULL(RTRIM(PAInterestRate),'')+ISNULL(RTRIM(CONVERT(varchar(10), NextDueDate, 112)), '')+ISNULL(RTRIM(CONVERT(varchar(10), NextPrintDate, 112)), '')+ISNULL(RTRIM(CONVERT(varchar(1), InBankruptcy)), ''))

DECLARE @ActionResult TABLE (MergeAction VARCHAR(20))

      MERGE INTO LAPro.LOAN WITH (HOLDLOCK) AS trg    -- clusterd on ID, non-clustered on LoanID
      USING (SELECT LoanID
                   ,CustomerID
                   ,AddressID
                   ,DateOfLoan
                   ,PortfolioID
                   ,LoanOfficer
                   ,DateOfAdjustment
                   ,RefinanceDate
                   ,MaturityDate
                   ,Rate
                   ,OriginalAmount
                   ,BillCycleCode2
                   ,Classification
                   ,CurrencyCode
                   ,PrincipalBalance
                   ,PaidOffDate
                   ,DueDay
                   ,BillingAmt
                   ,DebitBillAutomatic
                   ,ACHAccountNumber
                   ,RoutingNumber
                   ,AccountType
                   ,LastPaymentEffectiveDa
				   te
                   ,InterestReceivable
                   ,CutoffDay
                   ,PAInterestRate
				   ,NextDueDate
				   ,NextPrintDate
				   --,InBankruptcy
                   ,HashValue
				   ,CreatedAt
				   ,UpdatedAt
				   ,LoanSource
             FROM #LOAN
             ) AS src ON (src.LoanID=trg.LoanID)

      WHEN MATCHED AND trg.HashValue <> src.HashValue
      THEN UPDATE SET trg.CustomerID              = src.CustomerID
                     ,trg.AddressID               = src.AddressID
                     ,trg.DateOfLoan              = src.DateOfLoan
                     ,trg.PortfolioID             = src.PortfolioID
                     ,trg.LoanOfficer             = src.LoanOfficer
                     ,trg.DateOfAdjustment        = src.DateOfAdjustment
                     ,trg.RefinanceDate           = src.RefinanceDate
                     ,trg.MaturityDate            = src.MaturityDate
                     ,trg.Rate                    = src.Rate
                     ,trg.OriginalAmount          = src.OriginalAmount
                     ,trg.BillCycleCode2          = src.BillCycleCode2
                     ,trg.Classification          = src.Classification
                     ,trg.CurrencyCode            = src.CurrencyCode
                     ,trg.PrincipalBalance        = src.PrincipalBalance
                     ,trg.PaidOffDate             = src.PaidOffDate
                     ,trg.DueDay                  = src.DueDay
                     ,trg.BillingAmt              = src.BillingAmt
                     ,trg.DebitBillAutomatic      = src.DebitBillAutomatic
                     ,trg.ACHAccountNumber        = src.ACHAccountNumber
                     ,trg.RoutingNumber           = src.RoutingNumber
                     ,trg.AccountType             = src.AccountType
                     ,trg.LastPaymentEffectiveDate= src.LastPaymentEffectiveDate
                     ,trg.InterestReceivable      = src.InterestReceivable
                     ,trg.CutoffDay               = src.CutoffDay
                     ,trg.PAInterestRate          = src.PAInterestRate
					 ,trg.NextDueDate			  = src.NextDueDate
					 ,trg.NextPrintDate			  = src.NextPrintDate
					 --,trg.InBankruptcy			  = src.InBankruptcy
                     ,trg.HashValue               = src.HashValue
                     ,trg.UpdatedAt               = src.UpdatedAt

      WHEN NOT MATCHED BY TARGET   -- in src, not trg
      THEN INSERT ( LoanID
                   ,CustomerID
                   ,AddressID
                   ,DateOfLoan
                   ,PortfolioID
                   ,LoanOfficer
                   ,DateOfAdjustment
                   ,RefinanceDate
                   ,MaturityDate
                   ,Rate
                   ,OriginalAmount
                   ,BillCycleCode2
                   ,Classification
                   ,CurrencyCode
                   ,PrincipalBalance
                   ,PaidOffDate
                   ,DueDay
                   ,BillingAmt
                   ,DebitBillAutomatic
                   ,ACHAccountNumber
                   ,RoutingNumber
                   ,AccountType
                   ,LastPaymentEffectiveDate
                   ,InterestReceivable
                   ,CutoffDay
                   ,PAInterestRate
				   ,NextDueDate
				   ,NextPrintDate
				   --,InBankruptcy
				   ,LoanSource
                   ,HashValue
                   ,CreatedAt
                   ,UpdatedAt
                  )
           VALUES (src.LoanID
                  ,src.CustomerID
                  ,src.AddressID
                  ,src.DateOfLoan
                  ,src.PortfolioID
                  ,src.LoanOfficer
                  ,src.DateOfAdjustment
                  ,src.RefinanceDate
                  ,src.MaturityDate
                  ,src.Rate
                  ,src.OriginalAmount
                  ,src.BillCycleCode2
                  ,src.Classification
                  ,src.CurrencyCode
                  ,src.PrincipalBalance
                  ,src.PaidOffDate
                  ,src.DueDay
                  ,src.BillingAmt
                  ,src.DebitBillAutomatic
                  ,src.ACHAccountNumber
                  ,src.RoutingNumber
                  ,src.AccountType
                  ,src.LastPaymentEffectiveDate
                  ,src.InterestReceivable
                  ,src.CutoffDay
                  ,src.PAInterestRate
				  ,src.NextDueDate
				  ,src.NextPrintDate
				  --,src.InBankruptcy
				  ,src.LoanSource
                  ,src.HashValue
                  ,src.CreatedAt
                  ,src.UpdatedAt
                  )

      WHEN NOT MATCHED BY SOURCE   -- in trg, not src
      THEN DELETE

      OUTPUT $action INTO @ActionResult;

SELECT InsertCnt  = IsNULL(SUM(CASE WHEN MergeAction ='INSERT' THEN 1 ELSE 0 END),0)  -- inserts & updates fall under this "INSERT" action
            -- ,@UpdateCnt  = IsNULL(SUM(CASE WHEN MergeAction ='UPDATE' THEN 1 ELSE 0 END),0)  -- these aren't true updates
            ,DeleteCnt  = IsNULL(SUM(CASE WHEN MergeAction ='DELETE' THEN 1 ELSE 0 END),0)  -- deletes & updates fall under this "DELETE" action
        FROM @ActionResult

select * from LAPro.LOAN
--truncate table LAPro.LOAN