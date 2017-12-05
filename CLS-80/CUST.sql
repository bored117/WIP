if object_ID('tempdb..#CUST') is not null
	drop table #CUST
create table #CUST
(
	[CustomerID] [varchar](15) NOT NULL,
	[PortfolioID] [varchar](15) NULL,
	[U_FICOScore] [int] NULL,
	[U_NDI] [money] NULL,
	[U_MosSinceDerogatoryPubRec] [int] NULL,
	[PrimaryAddressID] [varchar](20) NULL,
	[Prefix] [varchar](10) NULL,
	[FirstName] [varchar](30) NULL,
	[Suffix] [varchar](10) NULL,
	[BusinessName] [varchar](50) NULL,
	[U_DTI] [float] NULL,
	[U_AnnualIncome] [money] NULL,
	[U_HousingInformation] [varchar](20) NULL,
	[U_EmploymentStatus] [varchar](20) NULL,
	[HashValue] [binary](16) NULL,
	[CreatedAt] [smalldatetime] NOT NULL CONSTRAINT [df_CUST_CreatedAt]  DEFAULT (getdate()),
	[UpdatedAt] [smalldatetime] NOT NULL CONSTRAINT [df_CUST_UpdatedAt]  DEFAULT (getdate()),
	[DateOfBirth] [datetime] NULL,
	[Identification] [varchar](15) NULL
)

insert into #CUST
select  
	account_external_id__c as [CustomerID], 
	null as [PortfolioID], -- this need to be addressed from loan.
	SHCP.fico__c as [U_FICOScore],
	net_disposable_income_ndi__c as [U_NDI],
	null as [U_MosSinceDerogatoryPubRec], -- need to be addressed from somewhere?
	SA.id as [PrimaryAddressID], 
	SA.[name] as [Prefix], --???
	SA.[name] as [FirstName], --???
	SA.[name] as [Suffix], --???
	SA.[name] as [BusinessName], --???
	debt_to_income_dti__c as [U_DTI],
	income__c as [U_AnnualIncome],
	null as [U_HousingInformation], -- ???
	null as [U_EmploymentStatus], -- ???
	null as [HashValue],
	SA.createddate as [CreatedAt],
	SA.lastmodifieddate as [UpdatedAt],
	bureau_date_of_birth__c as [DateOfBirth],
	bureau_social_security__c as [Identification]
from [CLS].[cls_migration].[public].[sf_account] SA
	left outer join [CLS].[cls_migration].[public].[sf_hard_credit_pull__c] SHCP
		on SA.id = SHCP.account__c and SHCP.delete_flag = 'N' and SHCP.isdeleted = 'false'	
where SA.delete_flag = 'N' and SA.isdeleted = 'false'
	and SA.account_external_id__C is not null -- shouldn't need this 

update #CUST set HashValue = HASHBYTES('MD5',ISNULL(RTRIM(CustomerID),'')+ISNULL(RTRIM(PortfolioID),'')+ISNULL(RTRIM(CONVERT(varchar(10),U_FICOScore)),'')+ISNULL(RTRIM(U_NDI),'')+ ISNULL(RTRIM(CONVERT(varchar(10), DateOfBirth, 112)), '') +
                                                ISNULL(RTRIM(CONVERT(varchar(10),U_MosSinceDerogatoryPubRec)),'')+ISNULL(RTRIM(PrimaryAddressID),'')+ISNULL(RTRIM(Prefix),'')+ISNULL(RTRIM(FirstName),'')+
                                                ISNULL(RTRIM(Suffix),'')+ISNULL(RTRIM(BusinessName),'')+ISNULL(RTRIM(Identification), '') +ISNULL(RTRIM(U_EmploymentStatus),'')+ISNULL(RTRIM(U_AnnualIncome),'')+ISNULL(RTRIM(U_DTI),'')+
                                                ISNULL(RTRIM(U_HousingInformation),''))

DECLARE @ActionResult TABLE (MergeAction VARCHAR(20))

      MERGE INTO LAPro.CUST WITH (HOLDLOCK) AS trg    -- clusterd on ID, non-clustered on LoanID
      USING (SELECT CustomerID
                   ,PortfolioID
				   ,DateOfBirth
                   ,U_FICOScore
                   ,U_NDI
                   ,U_MosSinceDerogatoryPubRec
                   ,PrimaryAddressID
                   ,Prefix
                   ,FirstName
                   ,Suffix
                   ,BusinessName
				   ,Identification
                   ,U_EmploymentStatus
                   ,U_AnnualIncome
                   ,U_DTI
                   ,U_HousingInformation
                   ,HashValue
				   ,CreatedAt
				   ,UpdatedAt
             FROM #CUST
             ) AS src ON (src.CustomerID=trg.CustomerID)

      WHEN MATCHED AND trg.HashValue <> src.HashValue
      THEN UPDATE SET trg.PortfolioID                = src.PortfolioID
					 ,trg.DateOfBirth				 = src.DateOfBirth
                     ,trg.U_FICOScore                = src.U_FICOScore
                     ,trg.U_NDI                      = src.U_NDI
                     ,trg.U_MosSinceDerogatoryPubRec = src.U_MosSinceDerogatoryPubRec
                     ,trg.PrimaryAddressID           = src.PrimaryAddressID
                     ,trg.Prefix                     = src.Prefix
                     ,trg.FirstName                  = src.FirstName
                     ,trg.Suffix                     = src.Suffix
                     ,trg.BusinessName               = src.BusinessName
					 ,trg.Identification			 = src.Identification
                     ,trg.U_EmploymentStatus         = src.U_EmploymentStatus
                     ,trg.U_AnnualIncome             = src.U_AnnualIncome
                     ,trg.U_DTI                      = src.U_DTI
                     ,trg.U_HousingInformation       = src.U_HousingInformation
                     ,trg.HashValue                  = src.HashValue
                     ,trg.UpdatedAt                  = src.UpdatedAt

      WHEN NOT MATCHED BY TARGET   -- in src, not trg
      THEN INSERT ( CustomerID
                   ,PortfolioID
				   ,DateOfBirth
                   ,U_FICOScore
                   ,U_NDI
                   ,U_MosSinceDerogatoryPubRec
                   ,PrimaryAddressID
                   ,Prefix
                   ,FirstName
                   ,Suffix
                   ,BusinessName
				   ,Identification
                   ,U_EmploymentStatus
                   ,U_AnnualIncome
                   ,U_DTI
                   ,U_HousingInformation
                   ,HashValue
                   ,CreatedAt
                   ,UpdatedAt
                  )
           VALUES (src.CustomerID
                  ,src.PortfolioID
				  ,src.DateOfBirth
                  ,src.U_FICOScore
                  ,src.U_NDI
                  ,src.U_MosSinceDerogatoryPubRec
                  ,src.PrimaryAddressID
                  ,src.Prefix
                  ,src.FirstName
                  ,src.Suffix
                  ,src.BusinessName
				  ,src.Identification
                  ,src.U_EmploymentStatus
                  ,src.U_AnnualIncome
                  ,src.U_DTI
                  ,src.U_HousingInformation
                  ,src.HashValue
                  ,src.CreatedAt
                  ,src.UpdatedAt
                  )

/*
      WHEN NOT MATCHED BY SOURCE   -- in trg, not src
      THEN DELETE
*/
      OUTPUT $action INTO @ActionResult;

SELECT InsertCnt  = IsNULL(SUM(CASE WHEN MergeAction ='INSERT' THEN 1 ELSE 0 END),0)  -- inserts & updates fall under this "INSERT" action
            -- ,@UpdateCnt  = IsNULL(SUM(CASE WHEN MergeAction ='UPDATE' THEN 1 ELSE 0 END),0)  -- these aren't true updates
            ,DeleteCnt  = IsNULL(SUM(CASE WHEN MergeAction ='DELETE' THEN 1 ELSE 0 END),0)  -- deletes & updates fall under this "DELETE" action
        FROM @ActionResult

select * from LAPro.CUST
--truncate table LAPro.CUST