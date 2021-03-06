----------
-- Address
----------
SELECT id as [AddressID] -- using account id for it!
      ,billingstate as [State]
      --,[sk]
      --,[HashValue] -- Calculated from ETL... so do it...
      --,[CreatedAt]
      --,[UpdatedAt]
      ,billingstreet as [Address1]
      ,billingcity as [City]
      ,billingpostalcode as [PostalCode]
	  ,null as [PhoneNumber] -- If below 3 can be found.
      ,null as [AltPhoneNumber] -- If below 3 can be found.
      ,null as [EmailAddress] -- If below 3 can be found.
from [CLS].[cls_migration].[public].[sf_account]
----------
--customer
----------
select  
account_external_id__c as [CustomerID], 
--null as [PortfolioID], -- this need to be addressed from loan.
SHCP.fico__c as [U_FICOScore],
net_disposable_income_ndi__c as [U_NDI],
--null as [U_MosSinceDerogatoryPubRec],
SA.id as [PrimaryAddressID], 
--[sk],
SA.[name] as [Prefix],
SA.[name] as [FirstName],
SA.[name] as [Suffix],
SA.[name] as [BusinessName],
debt_to_income_dti__c as [U_DTI],
income__c as [U_AnnualIncome],
--null as [U_HousingInformation],
--null as [U_EmploymentStatus],
--[HashValue]
SA.createddate as [CreatedAt],
SA.lastmodifieddate as [UpdatedAt],
bureau_date_of_birth__c as [DateOfBirth],
bureau_social_security__c as [Identification]
from [CLS].[cls_migration].[public].[sf_account] SA
	left outer join [CLS].[cls_migration].[public].[sf_hard_credit_pull__c] SHCP
	on SA.id = SHCP.account__c
------
--LoAN
------
select 
	--sga.delete_flag,SGA.isdeleted,
	coalesce(application_external_id__c,SC.id) as [LoanID]
	,account_external_id__c as [CustomerID]
	,Sa.id as [AddressID]
	,loan__application_date__c as [DateOfLoan]
    ,portfolioid_migration__c as [PortfolioID]
	,newid() as [LoanOfficer]
--,[DateOfAdjustment]
--,[RefinanceDate]
      ,loan__rate_change_dt_next__c as [MaturityDate]
      ,SC.loan__interest_rate__c as [Rate]
      ,loan__loan_amount__c as [OriginalAmount]
      ,'Monthly' as [BillCycleCode2] -- hardcoded.
--,[Classification]
      ,'USD' as [CurrencyCode] -- hardcoded.
      ,loan__principal_remaining__c as [PrincipalBalance]
--,[PaidOffDate]
--,[DueDay]
      ,loan__Pmt_Amt_Cur__c as [BillingAmt]
	  ,case when SC.loan__payment_mode__c = 'a2dg0000003g1aDAAQ' then 'Off' else 'Ach On' end as 
		[DebitBillAutomatic]
	  ,loan__bank_account_number__c as [ACHAccountNumber]
      ,loan__routing_number__c as [RoutingNumber]
      ,SLBA.loan__account_type__c as [AccountType]
--,[LastPaymentEffectiveDate]
      ,loan__interest_remaining__c as [InterestReceivable]
      ,loan__due_day__c as [CutoffDay]
      ,SC.loan__interest_rate__c as [PAInterestRate]
      --,[sk]
      --,[HashValue]
      --,[CreatedAt]
      --,[UpdatedAt]
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


-------
--LoAN2
-------
select 
	coalesce(application_external_id__c,SC.id) as [LoanID]
	,genesis__APR__c as [U_APR]
	,loan__application_date__c as [U_OriginationDate]
	--,[sk]
--,[InterestEarnedMTD]
	--,[HashValue]
	--,[CreatedAt]
	--,[UpdatedAt]
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
    ,null as [CMP_LoanMod]
    ,null as [U_LoanPremiumAmount]
    ,null as [U_OriginationIntAmt]
    ,null as [U_LoanPremiumPrice]
	from [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC 
	left join [CLS].[cls_migration].[public].[sf_genesis__applications__c] SGA on SGA.id = SC.application__c and SGA.delete_flag = 'N' and SGA.isdeleted = 'false'
	left join [CLS].[cls_migration].[public].[sf_account] SA on SA.id  = SGA.genesis__account__c and SA.delete_flag = 'N' and SA.isdeleted = 'false'
	left join [CLS].[cls_migration].[public].[sf_loan__bank_account__c] SLBA on SLBA.loan__account__c = SA.id and SLBA.delete_flag = 'N' and SLBA.isdeleted = 'false'
	where SC.delete_flag = 'N' and SC.isdeleted = 'false' 
	and sa.id is not null
	order by SGA.application_external_id__c 


------------------
-- ViewHistLoanpay
------------------

select 
 	loan__loan_account_external_id__c as [LoanID],
	loan__receipt_date__c as [EffectiveDate], -- coulde be transaction date or time field... wierd.
	loan__principal__c as [PrincipalPay],
	loan__interest__c as [InterestPay],
	case loan__reversed__c when 'true' then -1 else 0 end as [ReversedHistID], -- this need investigation... why we decided not to?
	payment_transaction_external_id__c as [History_id],
	null as	[NSFFee],
--	[PostDate],  could be an issue... why is there no posted date?
--	[sk] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
--	[HashValue] [binary](16) NULL,
--	[CreatedAt] [smalldatetime] NOT NULL,
--	[UpdatedAt] [smalldatetime] NOT NULL,
null as	[AutoPayment],
null as	[Method]

from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] SLLPT
	join [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC on SLLPT.loan__loan_account__c = SC.id
	
---------------------
--HistBillMain
---------------------

SELECT 
	null as [BillID]
    ,loan__loan_account_external_id__c as [LoanID]
    ,loan__due_date__c as [DueDate]
    ,loan__Principal_Billed__c as [PrinAmount]
    ,loan__Interest_Billed__c as [IntAmount]
    ,null [PriorPeriodAdjustments]
      --,[FeeID]
      --,[FeeType]
      --,[sk]
      --,[HashValue]
      --,[CreatedAt]
      --,[UpdatedAt]
from [CLS].[cls_migration].[public].[sf_loan__loan_account_due_details__c] bill
join	[CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC on bill.loan__loan_account__c = SC.id

---------------------
-- PaidPastDue
---------------------

SELECT 
	null as [ID]
    ,loan__loan_account_external_id__c as [LoanID]
    ,loan__transaction_date__c as [PastDueDate]
    ,loan__principal__c as [PaidPrincipal]
    ,loan__interest__c as [PaidInterest]
    ,0.0 as [PaidLateFee]
    ,payment_transaction_external_id__c as [HistoryID]
    ,loan__receipt_date__c as [EffectiveDate]
    ,0 as [PaidPenaltyAmount]
--      ,[sk]
--      ,[HashValue]
--      ,[CreatedAt]
--      ,[UpdatedAt]
from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] PPD
join	[CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC on PPD.loan__loan_account__c = SC.id
	

---------------------
--ViewPastDueLoan
---------------------

SELECT 
	null as [ID]
	,loan__loan_account_external_id__c as [LoanID]
    ,loan__transaction_date__c as [PastDueDate]
    ,loan__principal__c as [Principal]
    ,loan__interest__c as [Interest]
    ,0.0 as [LateFee]
    ,account_external_id__c as [CustomerID]
    ,'001' as [FacilityID]
    ,0.00 as [PenaltyAmount]
--      ,[sk]
--      ,[PastDueDateExcludingWeekends]-- importantat
--      ,[HashValue]
--      ,[CreatedAt]
--      ,[UpdatedAt]
from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] PPD
join	[CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC on PPD.loan__loan_account__c = SC.id
join [CLS].[cls_migration].[public].[sf_account] SF on SF.id = SC.loan__account__c

------------------
-- zcAmortSchedule
------------------

select null as [ID]
	,loan__loan_account_external_id__c as [LoanID]
    ,loan__due_date__c as [PaymentDate]
    ,loan__due_principal__c as [PrincipalPayment]
    ,loan__due_interest__c as [InterestPayment]
    ,0 as [LateFeePayment]
    ,0 as [NSFPayment]
    ,isnull(loan__due_principal__c,0) + isnull(loan__due_interest__c,0) as [TotalPayment]
    ,null as [sk] -- need to increment value based on.... hmmm payment date, etc....
    --,[HashValue]
    ,A.createddate as [CreatedAt]
    ,A.lastmodifieddate as [UpdatedAt]
from [CLS].[cls_migration].[public].[sf_loan__repayment_schedule__c] A
left join [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC on A.loan__loan_account__c = SC.id
where A.delete_flag = 'N' and A.isdeleted = 'false'