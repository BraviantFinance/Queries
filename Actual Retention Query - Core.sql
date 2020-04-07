with
	report_dates as (select '2020-2-29'::date as report_date
			,'2019-12-31'::date as end_date)

, A as (
select
	(date_trunc('MONTH',a.paid_off_date) + Interval '1 Month - 1 day')::date as paid_month
	,a.cust_type
	,(a.retention_days/30)+1 as retention_month
	,sum(a.paid_off_count) as total_paid_off
	,sum(a.retention_count) as total_retention
	,((extract (month from a.report_date)) - (extract (month from a.paid_off_date))) +
		(((extract (year from a.report_date)) - (extract (year from a.paid_off_date)))*12) +1
	as max_month

from
(
select
	-- a.loan_num,
	case when a.loan_funded = 1 then a.loan_paid_off_date end as paid_off_date
	,case when a.previous_customer in ('1','2') then 'Previous'
		else 'New' end
	as cust_type
	,1::numeric as paid_off_count
	,case when n.loan_num is not null then 1 else 0 end as retention_count
	,n.loan_funded_date as retention_funded_date
	,(a.loan_paid_off_date - a.loan_funded_date) as paid_off_days
	,(n.loan_funded_date - a.loan_paid_off_date) as retention_days
	,case	when n.previous_customer in ('1','2') then 'Previous'
		when n.previous_customer is null then null
		else 'New' end
	as next_cust_type
	,d.report_date

from
	dw_reporting_views.fact_application a
	inner join dw_reporting_views.fact_retention r on a.lms = r.lms and a.loan_num = r.loan_num
	left join dw_reporting_views.fact_application n on n.lms = r.retention_lms and n.loan_num = r.retention_loan_num and n.loan_funded = 1 and n.refinance_loan_flag = 0
	cross join report_dates d

where
	a.state_code not in ( 'CA', 'OH', 'KS', 'TN')
	and a.loan_paid_off_date is not null
	and a.loan_paid_off_date between '2018-12-1' and d.end_date
)as a

group by
	paid_month
	,a.cust_type
	,retention_month
	,max_month

order by
	paid_month
	,a.cust_type
	,retention_month
)

select * from A