select 
 	coalesce(application_external_id__c,SC.id) as [LoanID],
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
	left join [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC on SLLPT.loan__loan_account__c = SC.id and SC.delete_flag = 'N' and SC.isdeleted = 'false'
	left join [CLS].[cls_migration].[public].[sf_genesis__applications__c] SGA on SGA.id = SC.application__c and SGA.delete_flag = 'N' and SGA.isdeleted = 'false'
	where SLLPT.delete_flag = 'N' and SLLPT.isdeleted = 'false'


	--select top 10 * from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] SLLPT
	--select distinct loan__loan_account__c from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] where isdeleted = 'false' and delete_flag = 'N'
	select Z.id,Z.application__c, * from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] SLLPT
		left join
		(select SGA.application_external_id__c, SC.* from [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC
			left join [CLS].[cls_migration].[public].[sf_genesis__applications__c] SGA on SC.application__c = SGA.id  and SGA.delete_flag = 'N' and SGA.isdeleted = 'false'
			where SC.delete_flag = 'N' and SC.isdeleted = 'false'
		) Z on SLLPT.loan__loan_account__c = Z.id 
	where SLLPT.delete_flag = 'N' and SLLPT.isdeleted = 'false'


	select distinct(loan__loan_account__c)
		--, * 
		from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] SLLPT
		where loan__loan_account__c in (
			select id from 
				(select SGA.application_external_id__c, SC.* from [CLS].[cls_migration].[public].[sf_loan__loan_account__c] SC
					left join [CLS].[cls_migration].[public].[sf_genesis__applications__c] SGA on SC.application__c = SGA.id  and SGA.delete_flag = 'N' and SGA.isdeleted = 'false'
					where SC.delete_flag = 'N' and SC.isdeleted = 'false'
				) Z
			)
	and SLLPT.delete_flag = 'N' and SLLPT.isdeleted = 'false'


	select * from [CLS].[cls_migration].[public].[sf_loan__loan_payment_transaction__c] where loan__loan_account__c in
	(
		'a1hg0000001ijG6AAI',
		'a1hg0000001ijIEAAY',
		'a1hg0000001ijMCAAY',
		'a1hg0000001ijGoAAI',
		'a1hg0000001ijFtAAI'
	)