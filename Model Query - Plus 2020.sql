drop table if exists temp_date;
create temporary table temp_date as(
select
	'2020-02-29'::date as report_date
);

drop table if exists temp_loans;
create temporary table temp_loans as(
select
	-- (date_trunc('MONTH',a.applicant_transaction_datetime) + Interval '1 Month - 1 day')::date as app_month
	a.applicant_transaction_datetime::date as application_date
	,case when a.loan_funded = 1 then a.loan_funded_date end as funding_date
	,case when a.loan_funded = 1 then a.first_payment_default end as fpd
	,case when a.loan_funded = 1 then a.loan_paid_off_date end as paid_off_date
	,case when a.loan_funded = 1 then a.loan_charge_off_date end as charge_off_date
	,a.lms
	,a.loan_num
	,a.applied
	,case	when a.previous_customer = '2' then 'Returning'
		when a.previous_customer = '1' then 'VIP'
		else 'New' end 
	as cust_type
	,case
	    when a.state_code  in ('CA') then 'Plus'
        when a.state_code in ('TN', 'KS') then 'LOC'
        when a.state_code not in ('CA', 'TN', 'KS') then 'Core' end as product
	,case when a.state_code not in ('OH','TX') then 'Non-CSO' else a.state_code end as state_adj
	,case when s.customer_type = 'Previous' then 'Other' else right(s.customer_type,char_length(s.customer_type)-6) end as source
	,a.loan_funded
	,case when a.loan_funded = 1 then a.loan_actual_amount else 0 end as loan_amount
	,case 	when a.loan_funded = 1 and a.lms = 'EPIC' then coalesce(a.effective_apr,(select annualpercentrate from dw_reporting_ep.loan where id = a.loan_num))
		when a.loan_funded = 1 and a.lms <> 'EPIC' then a.effective_apr 
		else 0 end 
	as loan_apr
	,case when a.loan_funded = 1 then a.loan_term_in_days else 0 end as loan_term
	,a.latest_campaign
	,case 	when a.loan_funded = 1 
		then coalesce(((	a.loan_number_of_payments*a.payment_amount)/(a.loan_term_in_days::numeric/30)),0)
		else 0 end
	as monthly_pmt
	,case when a.loan_funded = 1 then a.loan_expected_roi_adjusted else 0 end as loan_npv

from
	dw_reporting_views.fact_application a
	left join dw_reporting_views.dim_duplicate_application_flg d using (application_number)
	left join dw_reporting_views.dim_application_source s using (application_number)
	cross join temp_date r

where
	-- enter date here
	a.applicant_transaction_datetime::date <= r.report_date
	and (d.duplicate_application_flg is false or d.application_number is null)
	and a.state_code in ('CA')
);

create index temp_loans_idx1 on temp_loans(loan_num asc) with (fillfactor = 100);

drop table if exists temp_originations;
create temporary table temp_originations as(
select
	(date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
	,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
	,l.lms
	,l.loan_num
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,l.applied
	,l.loan_funded
	,l.loan_amount
	,l.monthly_pmt
	,l.loan_npv
	,l.loan_apr * l.loan_amount as loan_apr
	,l.fpd * l.loan_amount as loan_fpd
	,0::numeric as paid_off
	,0::numeric as charge_off
	,round(l.loan_term/30,0) as loan_term_months
	,0::numeric as total_letters
	,0::numeric as total_current
	,0::numeric as total_30DPD
	,(extract (month from age(l.funding_date,l.funding_date)))
		+ (extract (year from age(l.funding_date,l.funding_date)))*12+1
	as trans_month

from
	temp_loans l
);

drop table if exists temp_mailings;
create temporary table temp_mailings as(
select 
	(date_trunc('MONTH',(case when campaign in ('EXP-41','TU-42') then '2016-12-31'
				  when campaign in ('EXP-90','TU-90','FT-90') then '2019-01-31'
					--adjustment made to campaign 90
				  else d.offer_date + interval '9 days' 
				  end)) + interval '1 month - 1 day')::date as app_month
	,null::date as funding_month
	,null::text as lms
	,null::integer as loan_num
	,'New'::text as cust_type
	,case
        when state in ('CA') then 'Plus'
        when state in ('TN', 'KS') then 'LOC'
        when state not in ('CA', 'TN', 'KS') then 'Core' end as product
	,case when state not in ('OH','TX') then 'Non-CSO' else state end as state_adj
	,'DM'::text as source
	,0::numeric as applied
	,0::numeric as loan_funded
	,0::numeric as loan_amount
	,0::numeric as monthly_pmt
	,0::numeric as loan_npv
	,0::numeric as loan_apr
	,0::numeric as loan_fpd
	,0::numeric as paid_off
	,0::numeric as charge_off
	,0::numeric as loan_term_months
	,sum(mailed) as total_letters
	,0::numeric as total_current
	,0::numeric as total_30DPD
	,null::double precision as trans_month
	

from 
	dw_reporting_views.fact_dm_conversion d
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
	,l.lms
	,l.loan_num
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,0::numeric as applied
	,0::numeric as loan_funded
	,0::numeric as loan_amount
	,0::numeric as monthly_pmt
	,0::numeric as loan_npv
	,0::numeric as loan_apr
	,0::numeric as loan_fpd
	,1::numeric as paid_off
	,0::numeric as charge_off
	,0::numeric as loan_term_months
	,0::numeric as total_letters
	,0::numeric as total_current
	,0::numeric as total_30DPD
	,(extract (month from age(l.paid_off_date,l.funding_date)))
		+ (extract (year from age(l.paid_off_date,l.funding_date)))*12 +1
	as trans_month

from
	temp_loans l

where
	l.paid_off_date is not null and l.charge_off_date is null
);

drop table if exists temp_charge_off;
create temporary table temp_charge_off as(
select
	(date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
	,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
	,l.lms
	,l.loan_num
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,0::numeric as applied
	,0::numeric as loan_funded
	,0::numeric as loan_amount
	,0::numeric as monthly_pmt
	,0::numeric as loan_npv
	,0::numeric as loan_apr
	,0::numeric as loan_fpd
	,0::numeric as paid_off
	,1::numeric as charge_off
	,0::numeric as loan_term_months
	,0::numeric as total_letters
	,0::numeric as total_current
	,0::numeric as total_30DPD
	,(extract (month from age(l.charge_off_date,l.funding_date)))
		+ (extract (year from age(l.charge_off_date,l.funding_date)))*12 +1 -2
	as trans_month

from
	temp_loans l

where
	l.charge_off_date is not null
);

drop table if exists month_ends;
create temporary table month_ends as(
select date 
from finance.next_business_dates 
where date = (date_trunc('MONTH',date) + Interval '1 Month - 1 day') 
and date between '2014-12-31' and (select report_date from temp_date));

create index month_ends_idx1 on month_ends(date asc) with (fillfactor = 100);

drop table if exists temp_ar;
create temporary table temp_ar as(
with ar_table as(
select
	l.application_date
	,l.funding_date
	,l.lms
	,l.loan_num
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,lsa.loan_status_text as loan_status
	,lsa.loan_sub_status_id as loan_substatus
	,case 	when lsa.days_past_due = 0 then 0
		when lsa.days_past_due between 1 and 30 then 30
		when lsa.days_past_due between 31 and 60 then 60
		when lsa.days_past_due between 61 and 90 then 90
		when lsa.days_past_due between 91 and 120 then 120
		when lsa.days_past_due between 121 and 150 then 150
		when lsa.days_past_due between 151 and 180 then 180
		when lsa.days_past_due between 181 and 210 then 210
		when lsa.days_past_due between 211 and 240 then 240
		when lsa.days_past_due between 241 and 270 then 270
		when lsa.days_past_due between 271 and 300 then 300
		when lsa.days_past_due >= 301 then 330
		end 
	as aging_bucket
	,lsa.principal_balance as outstanding_principal
	,lsa.date

from
	dw_reporting_lp.loan_status_archive lsa
	inner join temp_loans l on l.loan_num = lsa.loan_id and l.lms = 'LP'
	inner join month_ends m on lsa.date = m.date

where
	lsa.loan_status_text in ('Active', 'Closed') -- counts only funded loans
	and lsa.loan_sub_status_id not in (36,39)
	and lsa.days_past_due <= 60
)

select
	a.date as report_date
	,a.application_date
	,a.funding_date
	,a.lms
	,a.loan_num
	,a.cust_type
	,a.product
	,a.state_adj
	,a.source
	,sum(case when aging_bucket = 0 then a.outstanding_principal else 0 end) as current_principal
	,sum(case when aging_bucket = 30 then a.outstanding_principal else 0 end) as t30DPD_principal

from 
	ar_table a

group by
	report_date
	,a.application_date
	,a.funding_date
	,a.lms
	,a.loan_num
	,a.cust_type
	,a.product
	,a.state_adj
	,a.source
);

drop table if exists temp_ar_final;
create temporary table temp_ar_final as(
select 
	(date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
	,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
	,l.lms
	,l.loan_num
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,0::numeric as applied
	,0::numeric as loan_funded
	,0::numeric as loan_amount
	,0::numeric as monthly_pmt
	,0::numeric as loan_npv
	,0::numeric as loan_apr
	,0::numeric as loan_fpd
	,0::numeric as paid_off
	,0::numeric as charge_off
	,0::numeric as loan_term_months
	,0::numeric as total_letters
	,sum(current_principal) as total_current
	,sum(t30DPD_principal) as total_30DPD
	,(extract (month from age(l.report_date,l.funding_date)))
		+ (extract (year from age(l.report_date,l.funding_date)))*12 +1
	as trans_month

from
	temp_ar l

group by
	app_month
	,funding_month
	,l.lms
	,l.loan_num
	,l.cust_type
	,l.product
	,l.state_adj
	,l.source
	,trans_month
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
	,sum(a.monthly_pmt) as total_pmt
	,sum(a.loan_npv) as total_npv
	,sum(a.loan_fpd) as total_fpd_count
	,sum(a.paid_off) as total_paid_off
	,sum(a.charge_off) as total_charge_off
	,sum(a.total_current) as total_current_principal
	,sum(a.total_30DPD) as total_30DPD_principal


from	
	(select * from temp_originations
	union all
	select * from temp_mailings
	union all
	select * from temp_paid_off
	union all
	select * from temp_charge_off
	union all
	select * from temp_ar_final) a

where
	a.product = 'Plus'

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