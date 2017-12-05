if object_ID('tempdb..#LOAN2') is not null
	drop table #LOAN2
create table #LOAN2
(
	[LoanID] [varchar](15) NOT NULL,
	[U_APR] [float] NULL,
	[U_OriginationDate] [datetime] NULL,
	[InterestEarnedMTD] [money] NULL,
	[HashValue] [binary](16) NULL,
	[CreatedAt] [smalldatetime] NOT NULL CONSTRAINT [df_LOAN2_CreatedAt]  DEFAULT (getdate()),
	[UpdatedAt] [smalldatetime] NOT NULL CONSTRAINT [df_LOAN2_UpdatedAt]  DEFAULT (getdate()),
	[U_OriginalPortfolioID] [int] NULL,
	[U_PortMoveCode] [varchar](20) NULL,
	[U_LoanModRate] [float] NULL,
	[U_LoanModRateLength] [int] NULL,
	[ETL_LoanModRateDt] [datetime] NULL,
	[U_LoanModRateEffDt] [datetime] NULL,
	[ETL_LoanModRateEndDt] [datetime] NULL,
	[U_LoanModTermExt] [int] NULL,
	[U_LoanModExtDt] [datetime] NULL,
	[U_LoanModForbearance] [int] NULL,
	[U_LoanModForbearDt] [datetime] NULL,
	[ETL_LoanModForbearEffDt] [datetime] NULL,
	[ETL_LoanModForbearEndDt] [datetime] NULL,
	[U_LoanModBalanceChg] [money] NULL,
	[U_OrigMatDate] [datetime] NULL,
--	[CMP_LoanMod]  AS (case when [U_LoanModRateEffDt] IS NOT NULL OR [U_LoanModExtDt] IS NOT NULL OR [U_LoanModForbearDt] IS NOT NULL then 'Y' else 'N' end),
	[U_LoanPremiumAmount] [money] NULL,
	[U_OriginationIntAmt] [money] NULL,
	[U_LoanPremiumPrice] [money] NULL,
	[U_BKFilingDate] [datetime] NULL,
	[U_DeferredAmount] [money] NULL,
	[U_DeferredDate] [datetime] NULL)

insert into #LOAN2
select 
	coalesce(application_external_id__c,SC.id) as [LoanID]
	,genesis__APR__c as [U_APR]
	,loan__application_date__c as [U_OriginationDate]
	,null as [InterestEarnedMTD]
	--,[InterestEarnedMTD]
	,null as [HashValue]
	,sc.createddate as [CreatedAt]
	,sc.lastmodifieddate as [UpdatedAt]
	,null as [U_OriginalPortfolioID] -- hardcode
    ,null as [U_PortMoveCode] -- hardcode
    ,null as [U_LoanModRate]
    ,null as [U_LoanModRateLength]
    ,null as [ETL_LoanModRateDt]
    ,null as [U_LoanModRateEffDt]
    ,null as [ETL_LoanModRateEndDt]
    ,null as [U_LoanModTermExt]
    ,null as [U_LoanModExtDt]
    ,null as [U_LoanModForbearance]
    ,null as [U_LoanModForbearDt]
    ,null as [ETL_LoanModForbearEffDt]
    ,null as [ETL_LoanModForbearEndDt]
    ,null as [U_LoanModBalanceChg]
    ,null as [U_OrigMatDate]
    --,null as [CMP_LoanMod]
    ,null as [U_LoanPremiumAmount]
    ,null as [U_OriginationIntAmt]
    ,null as [U_LoanPremiumPrice]
	,null as [U_BKFillingDate]
	,null as [U_DeferredAmount]
	,null as [U_DeferredDate]
	from [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC 
	left join [CLS].[cls_migration].[public].[sf_genesis__applications__c] SGA on SGA.id = SC.application__c and SGA.delete_flag = 'N' and SGA.isdeleted = 'false'
	left join [CLS].[cls_migration].[public].[sf_account] SA on SA.id  = SGA.genesis__account__c and SA.delete_flag = 'N' and SA.isdeleted = 'false'
	left join [CLS].[cls_migration].[public].[sf_loan__bank_account__c] SLBA on SLBA.loan__account__c = SA.id and SLBA.delete_flag = 'N' and SLBA.isdeleted = 'false'
	where SC.delete_flag = 'N' and SC.isdeleted = 'false' 
	and sa.id is not null
	order by SGA.application_external_id__c 

update #LOAN2 set HashValue = HASHBYTES('MD5',ISNULL(RTRIM(LoanID),'')+ISNULL(RTRIM(U_APR),'')+ISNULL(RTRIM(CONVERT(varchar(10),U_OriginationDate,112)),'')+ISNULL(RTRIM(InterestEarnedMTD),'')+
                                                ISNULL(RTRIM(CONVERT(varchar(10),U_OriginalPortfolioID)),'')+ISNULL(RTRIM(U_PortMoveCode),'')+ISNULL(RTRIM(U_LoanModRate),'')+
                                                ISNULL(RTRIM(CONVERT(varchar(10),U_LoanModRateLength)),'')+ISNULL(RTRIM(CONVERT(varchar(10),U_LoanModRateEffDt,112)),'')+
                                                ISNULL(RTRIM(CONVERT(varchar(10),U_LoanModTermExt)),'')+ISNULL(RTRIM(CONVERT(varchar(10),U_LoanModExtDt,112)),'')+
                                                ISNULL(RTRIM(CONVERT(varchar(10),U_LoanModForbearance)),'')+ISNULL(RTRIM(CONVERT(varchar(10),U_LoanModForbearDt,112)),'')+
												ISNULL(RTRIM(U_LoanModBalanceChg),'')+ISNULL(RTRIM(CONVERT(varchar(10),U_OrigMatDate,112)),'')
												+ISNULL(RTRIM(U_LoanPremiumAmount),'')+ISNULL(RTRIM(U_OriginationIntAmt), '')+ISNULL(RTRIM(U_LoanPremiumPrice), '')
												+ISNULL(RTRIM(CONVERT(varchar(10),U_BKFilingDate,112)),'')
												+ISNULL(RTRIM(U_DeferredAmount), '')
												+ISNULL(RTRIM(CONVERT(varchar(10),U_DeferredDate,112)),''))

DECLARE @ActionResult TABLE (MergeAction VARCHAR(20))

      MERGE INTO LAPro.LOAN2 WITH (HOLDLOCK) AS trg    -- clusterd on ID, non-clustered on LoanID
      USING (SELECT LOANID
                   ,U_APR
                   ,U_OriginationDate
                   ,InterestEarnedMTD
				   ,U_OriginalPortfolioID
				   ,U_PortMoveCode
				   ,U_LoanModRate
				   ,ETL_LoanModRateDt    = DATEADD(M, 1, U_LoanModRateEffDt)                        -- ODSLaPro only, not for hash
				   ,U_LoanModRateEffDt

				   -- note: Debt Settlement when U_LoanModRateLength=999
				   ,ETL_LoanModRateEndDt = CASE WHEN U_LoanModRateLength IS NULL THEN NULL
				                                WHEN U_LoanModRateLength = 999   THEN DATEADD(M, U_LoanModTermExt, U_OrigMatDate)
				 							    ELSE DATEADD(M, U_LoanModRateLength, U_LoanModRateEffDt) -1   -- ODSLaPro only, not for hash
                                          END
				   ,U_LoanModRateLength
				   ,U_LoanModTermExt
				   ,U_LoanModExtDt
				   ,U_LoanModForbearance
				   ,U_LoanModForbearDt
				   ,ETL_LoanModForbearEffDt = DATEADD(M, -1, U_LoanModForbearDt)                                        -- ODSLaPro only, not for hash
				   ,ETL_LoanModForbearEndDt = DATEADD(M, U_LoanModForbearance, DATEADD(M, -1, U_LoanModForbearDt) ) -1  -- ODSLaPro only, not for hash
				   ,U_LoanModBalanceChg
				   ,U_OrigMatDate
				   ,U_LoanPremiumAmount 
				   ,U_OriginationIntAmt
				   ,U_LoanPremiumPrice
				   ,U_BKFilingDate
				   ,U_DeferredAmount
				   ,U_DeferredDate
                   ,HashValue 
				   ,CreatedAt
                   ,UpdatedAt
             FROM #LOAN2 
             ) AS src ON (src.LoanID=trg.LoanID)

      WHEN MATCHED AND trg.HashValue <> src.HashValue
      THEN UPDATE SET trg.U_APR                  = src.U_APR
                     ,trg.U_OriginationDate      = src.U_OriginationDate
                     ,trg.InterestEarnedMTD      = src.InterestEarnedMTD
                     ,trg.U_OriginalPortfolioID  = src.U_OriginalPortfolioID
                     ,trg.U_PortMoveCode         = src.U_PortMoveCode
                     ,trg.U_LoanModRate          = src.U_LoanModRate
					 ,trg.ETL_LoanModRateDt      = src.ETL_LoanModRateDt
                     ,trg.U_LoanModRateEffDt     = src.U_LoanModRateEffDt
					 ,trg.ETL_LoanModRateEndDt   = src.ETL_LoanModRateEndDt
                     ,trg.U_LoanModRateLength    = src.U_LoanModRateLength
                     ,trg.U_LoanModTermExt       = src.U_LoanModTermExt
                     ,trg.U_LoanModExtDt         = src.U_LoanModExtDt
                     ,trg.U_LoanModForbearance   = src.U_LoanModForbearance
                     ,trg.U_LoanModForbearDt     = src.U_LoanModForbearDt
					 ,trg.ETL_LoanModForbearEffDt= src.ETL_LoanModForbearEffDt
					 ,trg.ETL_LoanModForbearEndDt= src.ETL_LoanModForbearEndDt
                     ,trg.U_LoanModBalanceChg    = src.U_LoanModBalanceChg
                     ,trg.U_OrigMatDate          = src.U_OrigMatDate
					 ,trg.U_LoanPremiumAmount	 = src.U_LoanPremiumAmount
					 ,trg.U_OriginationIntAmt    = src.U_OriginationIntAmt
					 ,trg.U_BKFilingDate	     = src.U_BKFilingDate
					 ,trg.U_DeferredAmount       = src.U_DeferredAmount
					 ,trg.U_DeferredDate         = src.U_DeferredDate
                     ,trg.HashValue              = src.HashValue
					 ,trg.U_LoanPremiumPrice     = src.U_LoanPremiumPrice

      WHEN NOT MATCHED BY TARGET   -- in src, not trg
      THEN INSERT ( LoanID
                   ,U_APR
                   ,U_OriginationDate
                   ,InterestEarnedMTD
				   ,U_OriginalPortfolioID
				   ,U_PortMoveCode
				   ,U_LoanModRate
				   ,ETL_LoanModRateDt          -- ODSLaPro only, not for hash
				   ,U_LoanModRateEffDt
				   ,ETL_LoanModRateEndDt       -- ODSLaPro only, not for hash
				   ,U_LoanModRateLength
				   ,U_LoanModTermExt
				   ,U_LoanModExtDt
				   ,U_LoanModForbearance
				   ,U_LoanModForbearDt
				   ,ETL_LoanModForbearEffDt    -- ODSLaPro only, not for hash
				   ,ETL_LoanModForbearEndDt    -- ODSLaPro only, not for hash
				   ,U_LoanModBalanceChg
				   ,U_OrigMatDate
				   ,U_LoanPremiumAmount
				   ,U_OriginationIntAmt
				   ,U_LoanPremiumPrice
				   ,U_BKFilingDate
				   ,U_DeferredAmount
				   ,U_DeferredDate
                   ,HashValue
                   ,CreatedAt
                   ,UpdatedAt
                  )
           VALUES (src.LoanID
                  ,src.U_APR
                  ,src.U_OriginationDate
                  ,src.InterestEarnedMTD
				  ,src.U_OriginalPortfolioID
				  ,src.U_PortMoveCode
				  ,src.U_LoanModRate
				  ,src.ETL_LoanModRateDt
				  ,src.U_LoanModRateEffDt
				  ,src.ETL_LoanModRateEndDt
				  ,src.U_LoanModRateLength
				  ,src.U_LoanModTermExt
				  ,src.U_LoanModExtDt
				  ,src.U_LoanModForbearance
				  ,src.U_LoanModForbearDt
				  ,src.ETL_LoanModForbearEffDt
				  ,src.ETL_LoanModForbearEndDt
				  ,src.U_LoanModBalanceChg
				  ,src.U_OrigMatDate
				  ,src.U_LoanPremiumAmount
				  ,src.U_OriginationIntAmt
				  ,src.U_LoanPremiumPrice
				  ,src.U_BKFilingDate
				  ,src.U_DeferredAmount
				  ,src.U_DeferredDate
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

select * from LAPro.LOAN2
--truncate table LAPro.LOAN