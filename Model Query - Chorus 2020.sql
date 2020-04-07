drop table if exists temp_date;
create temporary table temp_date as(
select
	'2020-1-31'::date as report_date
);

drop table if exists temp_loans;
create temporary table temp_loans as(
select
	-- (date_trunc('MONTH',a.applicant_transaction_datetime) + Interval '1 Month - 1 day')::date as app_month
	a.application_datetime::date as application_date
	,case when a.loan_funded = 1 and rescission = 0 then a.loan_funded_date end as funding_date
	,case when a.loan_funded = 1 then b.fpd_90 end as fpd
	,case when a.loan_funded = 1 and rescission = 0 then a.loan_paid_off_date else null end as paid_off_date
    ,case when a.loan_funded = 1 and rescission = 0 and paid_off = 1 then 1 else null end as paid_off
	,case when a.loan_funded = 1 then null else null end as charge_off_date
    ,case when a.loan_funded = 1 and charge_off = 1 then 1 else null end as charge_off
	,a.loan_id
	,a.applied
	,case	when a.previous_customer = '2' then 'Returning'
		when a.previous_customer = '1' then 'VIP'
		else 'New' end 
	as cust_type
	,'Core'::text as product
	,case when a.state not in ('OH','TX') then 'Non-CSO' else a.state end as state_adj
	,a.source as source
	,case when rescission = 0 then a.loan_funded else 0 end as loan_funded
	,case when a.loan_funded = 1 and rescission = 0 then a.loan_actual_amount else 0 end as loan_amount
	,case 	when a.loan_funded = 1 and rescission = 0 then a.loan_apr 
		else 0 end 
	as loan_apr
	,case when a.loan_funded = 1 and rescission = 0 then a.loan_term_in_days else 0 end as loan_term

from
	dw_reporting_chorus_views.fact_application a
	left join (select 
			a.loan_id
			,case 	when a.first_payment_date <= dw_reporting_meta.add_business_days(current_date, -5) 
					and a.first_payment_default = 0 
				then 0
				when d.amount_paid_to_period >= a.payment_amount 
				then 0
				else 1 end 
			as fpd_90
		from 
			dw_reporting_chorus_views.fact_application a
			join dw_reporting_chorus_lp.loan_status_archive b on a.loan_id = b.loan_id
			join dw_reporting_chorus_views.fact_installment c on a.loan_id = c.loan_id
			join (select loan_id ,amount_paid_to_period
				from dw_reporting_chorus_views.fact_installment
				where installment_number = 4
				order by loan_id) d on a.loan_id = d.loan_id          
		where 
			a.loan_id > 0 
			and (first_payment_date + interval '90 days' = date) 
			and installment_number = 1  
		order by 
			a.loan_id        
	) b on a.loan_id = b.loan_id
	left join dw_reporting_chorus_views.dim_duplicate_application_flg d using (application_id)
    left join (select
           loan_id
           ,case when principal_balance <= 0 and loan_status_id = 5 and (loan_sub_status_id = 26 or loan_sub_status_id = 27) then 1 else 0 end as paid_off
           ,case when days_past_due >= 150 and loan_status_id in (4,5) then 1 else 0 end as charge_off
           ,case when loan_status_id = 5 and loan_sub_status_id = 36 then 1 else 0 end as rescission
    	   from dw_reporting_chorus_lp.loan_status_archive
           cross join temp_date r
           where date = r.report_date
           order by loan_id        
    ) c on a.loan_id = c.loan_id
	cross join temp_date r

where
	-- enter date here
	a.application_datetime::date <= r.report_date
	and (d.duplicate_application_flg is false or d.application_id is null)
);

drop table if exists temp_originations;
create temporary table temp_originations as(
select
	(date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
	,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
	,l.loan_id
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,l.applied
	,l.loan_funded
	,l.loan_amount
-- 	CHANGE
	,l.loan_apr * l.loan_amount as loan_apr
	,l.fpd * l.loan_amount as loan_fpd
	,0::numeric as paid_off
	,0::numeric as charge_off
	,round(l.loan_term/30,0) as loan_term_months
	,0::numeric as total_letters
	,(extract (month from age(l.funding_date,l.funding_date)))
		+ (extract (year from age(l.funding_date,l.funding_date)))*12+1
	as trans_month

from
	temp_loans l
);

drop table if exists temp_mailings;
create temporary table temp_mailings as(
select 
	(date_trunc('MONTH',(case when campaign in ('EXP-41','TU-42') then '2016-12-31' else d.offer_date + interval '9 days' end)) + interval '1 month - 1 day')::date as app_month
	,null::date as funding_month
	,null::integer as loan_id
	,'New'::text as cust_type
	,'Core'::text as product
	,case when state not in ('OH','TX') then 'Non-CSO' else state end as state_adj
	,'DM'::text as source
	,0::numeric as applied
	,0::numeric as loan_funded
	,0::numeric as loan_amount
	,0::numeric as loan_apr
	,0::numeric as loan_fpd
	,0::numeric as paid_off
	,0::numeric as charge_off
	,0::numeric as loan_term_months
	,sum(mailed) as total_letters
	,null::double precision as trans_month
	

from 
	dw_reporting_chorus_views.fact_dm_conversion d
	inner join finance.next_business_dates n on d.offer_date = n.date
	cross join temp_date r

where
	offer_date <= r.report_date
	and campaign not like 'DarwillTest%'

group by
	app_month
	,product
	,state_adj
	,source

order by
	app_month
);

drop table if exists temp_paid_off;
create temporary table temp_paid_off as(
select
	(date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
	,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
	,l.loan_id
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,0::numeric as applied
	,0::numeric as loan_funded
	,0::numeric as loan_amount
	,0::numeric as loan_apr
	,0::numeric as loan_fpd
	,1::numeric as paid_off
	,0::numeric as charge_off
	,0::numeric as loan_term_months
	,0::numeric as total_letters
	,(extract (month from age(l.paid_off_date,l.funding_date)))
		+ (extract (year from age(l.paid_off_date,l.funding_date)))*12 +1
	as trans_month

from
	temp_loans l

where
	l.paid_off is not null and l.charge_off is null
);

drop table if exists temp_charge_off;
create temporary table temp_charge_off as(
select
	(date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
	,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
	,l.loan_id
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,0::numeric as applied
	,0::numeric as loan_funded
	,0::numeric as loan_amount
	,0::numeric as loan_apr
	,0::numeric as loan_fpd
	,0::numeric as paid_off
	,1::numeric as charge_off
	,0::numeric as loan_term_months
	,0::numeric as total_letters
	,(extract (month from age(l.charge_off_date::date,l.funding_date)))
		+ (extract (year from age(l.charge_off_date::date,l.funding_date)))*12 +1 -2
	as trans_month

from
	temp_loans l

where
	l.charge_off is not null
);

select
	a.app_month
	,a.funding_month
	,a.cust_type
	,a.product
	,a.state_adj
	,a.source
	,a.trans_month
	,sum(a.total_letters) as total_letters
	,sum(a.applied) as total_apps
	,sum(a.loan_funded) as total_loans
	,sum(a.loan_amount) as total_amount_funded
	,sum(a.loan_apr) as total_apr
	,sum(a.loan_term_months) as total_loan_term
	,sum(a.loan_fpd) as total_fpd_count
	,sum(a.paid_off) as total_paid_off
	,sum(a.charge_off) as total_charge_off

from	
	(select * from temp_originations
	union all
	select * from temp_mailings
	union all
	select * from temp_paid_off
	union all
	select * from temp_charge_off) a

group by
	a.app_month
	,a.funding_month
	,a.cust_type
	,a.product
	,a.state_adj
	,a.source
	,a.trans_month

order by
	1,2,3,4,5