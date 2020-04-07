drop table if exists temp_date;
create temporary table temp_date as(
select
    '2020-02-29'::date as report_date)
;
drop table if exists temp_loans;
create temporary table temp_loans as(
select
    -- (date_trunc('MONTH',a.applicant_transaction_datetime) + Interval '1 Month - 1 day')::date as app_month
    a.applicant_transaction_datetime::date as application_date
    ,case when a.loan_funded = 1 then a.loan_contract_date end as funding_date
    ,case when a.loan_funded = 1 then a.first_payment_default end as fpd
    ,case when a.loan_funded = 1 and a.refinance_payoff_flag = 0 then a.loan_paid_off_date end as paid_off_date
    ,case when a.loan_funded = 1 then a.loan_charge_off_date end as charge_off_date
    ,case when a.loan_funded = 1 and a.refinance_payoff_flag = 1 then a.loan_paid_off_date end as refi_date
    ,a.lms
    ,a.loan_num
    ,a.applied
    ,case   when a.previous_customer in ('1','2') then 'Previous'
        else 'New' end
    as cust_type
    ,case
        when a.state_code  in ('CA') then 'Plus'
        when a.state_code in ('TN', 'KS') then 'LOC'
        when a.state_code not in ('CA', 'TN', 'KS') then 'Core' end as product
    ,case when a.state_code not in ('OH','TX','VA') then 'Non-CSO' else a.state_code end as state_adj
    ,case when a.refinance_loan_flag = 1 then 'refi' else 'base loan' end as loan_type
    ,case when s.customer_type = 'Previous' then 'Other' else coalesce(right(s.customer_type,char_length(s.customer_type)-6),'Other') end as source
    ,a.loan_funded
    ,case when a.loan_funded = 1 then a.loan_actual_amount else 0 end as initial_draw_amount
    ,case when a.loan_funded = 1 then a.loan_approved_amount else 0 end as line_amount
    ,case   when a.loan_funded = 1 and a.lms = 'EPIC' then coalesce(a.effective_apr,(select annualpercentrate from dw_reporting_ep.loan where id = a.loan_num))
        when a.loan_funded = 1 and a.lms <> 'EPIC' then a.effective_apr
        else 0 end
    as loan_apr
    ,case when a.loan_funded = 1 then a.loan_term_in_days::numeric else 0 end as loan_term
    ,a.latest_campaign
    ,case when a.loan_funded = 1 then a.payment_amount else 0 end as monthly_pmt
    ,case when a.loan_funded = 1 then a.loan_expected_roi_adjusted else 0 end as loan_npv
from
    dw_reporting_views.fact_application a
    left join dw_reporting_views.dim_duplicate_application_flg d using (application_number)
    left join dw_reporting_views.dim_application_source s using (application_number)
    cross join temp_date r
where
    -- enter date here
    a.applicant_transaction_datetime::date <= r.report_date
    -- and a.loan_funded_date between '2018-2-1' and '2018-2-28'
    and (d.duplicate_application_flg is false or d.application_number is null)
    and (a.state_code in ('TN', 'KS')));
    -- and a.loan_num = 233464

-- select * from temp_loans where product = 'LOC'

create index temp_loans_idx1 on temp_loans(loan_num asc) with (fillfactor = 100);
drop table if exists temp_originations;
create temporary table temp_originations as(
select
    (date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
    ,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
    ,null::date as draw_month
    ,l.lms
    ,l.loan_num
    ,l.cust_type
    ,l.product
    ,l.state_adj
    ,l.loan_type
    ,l.source
    ,l.applied
    ,l.loan_funded
    ,l.initial_draw_amount
    ,0:: numeric subsequent_draws
    ,l.line_amount
    ,l.monthly_pmt
    ,l.loan_npv
    ,l.loan_apr * l.initial_draw_amount as loan_apr
    ,l.fpd * l.initial_draw_amount as loan_fpd
    ,0::numeric as paid_off
    ,0::numeric as charge_off
    ,0::numeric as refi
    ,round(l.loan_term/30,0) as loan_term_months
    ,0::numeric as total_letters
    ,0::numeric as total_current
    ,0::numeric as total_30DPD
    ,0::numeric as total_60DPD
    ,0::numeric as total_principal_ar
    ,(extract (month from age(l.funding_date,l.funding_date)))
        + (extract (year from age(l.funding_date,l.funding_date)))*12+1
    as trans_month
from
    temp_loans l
);

drop table if exists temp_draws;
create temporary table temp_draws as(
with draw_tbl as (select dw_reporting_bsf_identity.draw.*,  dw_reporting_lp.loan_tx.type from dw_reporting_bsf_identity.draw
join dw_reporting_lp.loan_tx on dw_reporting_bsf_identity.draw.loan_transaction_id = dw_reporting_lp.loan_tx.id
where type <> 'origination' and draw_status_id = 2)
    select
    (date_trunc('MONTH',tl.application_date) + Interval '1 Month - 1 day')::date as app_month
    ,(date_trunc('MONTH', tl.funding_date) + interval '1 month - 1 day')::date as funding_month
    ,(date_trunc('MONTH', d.draw_effective_date) + interval '1 month - 1 day')::date as draw_month
    ,null::text as lms
    ,d.loan_pro_loan_id as loan_num
    ,tl.cust_type
    ,'LOC' as product
    ,tl.state_adj
    ,'draw' as loan_type
    ,tl.source as source
    ,0::numeric as applied
    ,0::numeric as loan_funded
    ,0::numeric as initial_draw_amount
    ,sum(d.draw_amount) as subsequent_draws
    ,0::numeric as line_amount
    ,0::numeric as monthly_pmt
    ,0::numeric as loan_apr
    ,0::numeric as loan_npv
    ,0::numeric as loan_fpd
    ,0::numeric as paid_off
    ,0::numeric as charge_off
    ,0::numeric as refi
    ,0::numeric as loan_term_months
    ,0::numeric as total_letters
    ,0::numeric as total_current
    ,0::numeric as total_30DPD
    ,0::numeric as total_60DPD
    ,0::numeric as total_principal_ar
    ,(extract (month from age((date_trunc('MONTH', d.draw_effective_date) + interval '1 month - 1 day')::date, (date_trunc('MONTH', tl.funding_date) + interval '1 month - 1 day')::date)))
        + (extract (year from age((date_trunc('MONTH', d.draw_effective_date) + interval '1 month - 1 day')::date, (date_trunc('MONTH', tl.funding_date) + interval '1 month - 1 day')::date)))*12+1
    as trans_month
    from draw_tbl as d
    left join temp_loans as tl on d.loan_pro_loan_id = tl.loan_num
    where d.draw_effective_date is not null
        and now()<@d.asserted
        and now()<@d.effective
    group by
    app_month
    ,funding_month
    ,draw_month
	,d.product
    ,d.loan_pro_loan_id
    ,tl.loan_type
    ,tl.cust_type
	,tl.state_adj
	,tl.source
    ,trans_month
	order by 1,2);

-- select * from temp_draws where trans_month = 3

-- select * from temp_originations where product = 'LOC'

drop table if exists temp_mailings;
create temporary table temp_mailings as(
select
    (date_trunc('MONTH',(case when campaign in ('EXP-41','TU-42') then '2016-12-31'
                  when campaign in ('EXP-90','TU-90','FT-90') then '2019-01-31'
                    --adjustment made to campaign 90
                  else d.offer_date + interval '9 days'
                  end)) + interval '1 month - 1 day')::date as app_month
    ,null::date as funding_month
    ,null::date as draw_month
    ,null::text as lms
    ,null::integer as loan_num
    ,'New'::text as cust_type
    ,case
        when state  in ('CA') then 'Plus'
        when state in ('TN', 'KS') then 'LOC'
        when state not in ('CA', 'TN', 'KS') then 'Core' end as product
    ,case when state not in ('OH','TX','VA') then 'Non-CSO' else state end as state_adj
    ,'Base Loan'::text as loan_type
    ,'DM'::text as source
    ,0::numeric as applied
    ,0::numeric as loan_funded
    ,0::numeric as initial_draw_amount
    ,0::numeric as subsequent_draws
    ,0::numeric as line_amount
    ,0::numeric as monthly_pmt
    ,0::numeric as loan_npv
    ,0::numeric as loan_apr
    ,0::numeric as loan_fpd
    ,0::numeric as paid_off
    ,0::numeric as charge_off
    ,0::numeric as refi
    ,0::numeric as loan_term_months
    ,sum(mailed) as total_letters
    ,0::numeric as total_current
    ,0::numeric as total_30DPD
    ,0::numeric as total_60DPD
    ,0::numeric as total_principal_ar
    ,null::double precision as trans_month

from
    dw_reporting_views.fact_dm_conversion d
    inner join finance.next_business_dates n on d.offer_date = n.date
    cross join temp_date r
where
    d.offer_date + interval '9 days' <= r.report_date
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
    ,null::date as draw_month
    ,l.lms
    ,l.loan_num
    ,l.cust_type
    ,l.product
    ,l.state_adj
    ,l.loan_type
    ,l.source
    ,0::numeric as applied
    ,0::numeric as loan_funded
    ,0::numeric as initial_draw_amount
     ,0::numeric as subsequent_draws
     ,0::numeric as line_amount
    ,0::numeric as monthly_pmt
    ,0::numeric as loan_npv
    ,0::numeric as loan_apr
    ,0::numeric as loan_fpd
    ,1::numeric as paid_off
    ,0::numeric as charge_off
    ,0::numeric as refi
    ,0::numeric as loan_term_months
    ,0::numeric as total_letters
    ,0::numeric as total_current
    ,0::numeric as total_30DPD
    ,0::numeric as total_60DPD
    ,0::numeric as total_principal_ar
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
    ,null::date as draw_month
    ,l.lms
    ,l.loan_num
    ,l.cust_type
    ,l.product
    ,l.state_adj
    ,l.loan_type
    ,l.source
    ,0::numeric as applied
    ,0::numeric as loan_funded
    ,0::numeric as initial_draw_amount
     ,0::numeric as subsequent_draws
    ,0::numeric as line_amount
    ,0::numeric as monthly_pmt
    ,0::numeric as loan_apr
    ,0::numeric as loan_npv
    ,0::numeric as loan_fpd
    ,0::numeric as paid_off
    ,1::numeric as charge_off
    ,0::numeric as refi
    ,0::numeric as loan_term_months
    ,0::numeric as total_letters
    ,0::numeric as total_current
    ,0::numeric as total_30DPD
    ,0::numeric as total_60DPD
    ,0::numeric as total_principal_ar
    ,(extract (month from age(l.charge_off_date,l.funding_date)))
        + (extract (year from age(l.charge_off_date,l.funding_date)))*12 +1 -2
    as trans_month
from
    temp_loans l
where
    l.charge_off_date is not null
);

drop table if exists temp_refi;
create temporary table temp_refi as(
select
    (date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
    ,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
    ,null::date as draw_month
    ,l.lms
    ,l.loan_num
    ,l.cust_type
    ,l.product
    ,l.state_adj
    ,l.loan_type
    ,l.source
    ,0::numeric as applied
    ,0::numeric as loan_funded
    ,0::numeric as initial_draw_amount
     ,0::numeric as subsequent_draws
    ,0::numeric as line_amount
    ,0::numeric as monthly_pmt
    ,0::numeric as loan_apr
    ,0::numeric as loan_npv
    ,0::numeric as loan_fpd
    ,0::numeric as paid_off
    ,0::numeric as charge_off
    ,1::numeric as refi
    ,0::numeric as loan_term_months
    ,0::numeric as total_letters
    ,0::numeric as total_current
    ,0::numeric as total_30DPD
    ,0::numeric as total_60DPD
    ,0::numeric as total_principal_ar
    ,(extract (month from l.refi_date) - extract (month from l.funding_date)) +
      (extract (year from l.refi_date) - extract (year from l.funding_date))*12
--     ,(extract (month from age(l.refi_date,l.funding_date)))
--       + (extract (year from age(l.refi_date,l.funding_date)))*12
    as trans_month
from
    temp_loans l
where
    l.refi_date is not null
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
    ,null::date as draw_month
    ,l.lms
    ,l.loan_num
    ,l.cust_type
    ,l.product
    ,l.state_adj
    ,l.loan_type
    ,l.source
    ,lsa.loan_status_text as loan_status
    ,lsa.loan_sub_status_id as loan_substatus
    ,case   when lsa.days_past_due = 0 then 0
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
    ,null::date as draw_month
    ,a.lms
    ,a.loan_num
    ,a.cust_type
    ,a.product
    ,a.state_adj
    ,a.loan_type
    ,a.source
    ,sum(case when aging_bucket = 0 then a.outstanding_principal else 0 end) as current_principal
    ,sum(case when aging_bucket = 30 then a.outstanding_principal else 0 end) as t30DPD_principal
    ,sum(case when aging_bucket = 60 then a.outstanding_principal else 0 end) as t60DPD_principal
    ,sum(case when aging_bucket <= 60 then a.outstanding_principal else 0 end) as principal
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
    ,a.loan_type
    ,a.source
);


drop table if exists temp_ar_final;
create temporary table temp_ar_final as(
select
    (date_trunc('MONTH',l.application_date) + Interval '1 Month - 1 day')::date as app_month
    ,(date_trunc('MONTH',l.funding_date) + Interval '1 Month - 1 day')::date as funding_month
    ,null::date as draw_month
    ,l.lms
    ,l.loan_num
    ,l.cust_type
    ,l.product
    ,l.state_adj
    ,l.loan_type
    ,l.source
    ,0::numeric as applied
    ,0::numeric as loan_funded
    ,0::numeric as initial_draw_amount
     ,0::numeric as subsequent_draws
    ,0::numeric as line_amount
    ,0::numeric as monthly_pmt
    ,0::numeric as loan_npv
    ,0::numeric as loan_apr
    ,0::numeric as loan_fpd
    ,0::numeric as paid_off
    ,0::numeric as charge_off
    ,0::numeric as refi
    ,0::numeric as loan_term_months
    ,0::numeric as total_letters
    ,sum(current_principal) as total_current
    ,sum(t30DPD_principal) as total_30DPD
    ,sum(t60DPD_principal) as total_60DPD
    ,sum(principal) as total_principal_ar
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
    ,l.loan_type
    ,l.source
    ,trans_month
);

drop table if exists loc_table;
create temporary table loc_table as (
  select a.app_month
       , a.funding_month
       , a.draw_month
       , a.cust_type
       , a.product
       , a.loan_type
       , a.state_adj
       , a.source
       , a.trans_month
       , sum(a.total_letters)      as total_letters
       , sum(a.applied)            as total_apps
       , sum(a.loan_funded)        as total_loans
       , sum(a.initial_draw_amount)        as total_initial_draws_funded
       , sum(a.subsequent_draws)        as subsequent_draws
       , sum(a.line_amount)        as total_line_approved
       , sum(a.loan_apr)           as total_apr
       , sum(a.loan_term_months)   as total_loan_term
       , sum(a.monthly_pmt)        as total_pmt
       , sum(a.loan_npv)           as total_npv
       , sum(a.loan_fpd)           as total_fpd_count
       , sum(a.paid_off)           as total_paid_off
       , sum(a.charge_off)         as total_charge_off
       , sum(a.refi)               as total_refi
       , sum(a.total_current)      as total_current_principal
       , sum(a.total_30DPD)        as total_30DPD_principal
       , sum(a.total_60DPD)        as total_60DPD_principal
       , sum(a.total_principal_ar) as total_principal
  from (select *
        from temp_originations
        union all
        select *
        from temp_mailings
        union all
        select *
        from temp_paid_off
        union all
        select *
        from temp_charge_off
        union all
        select *
        from temp_refi
        union all
        select *
        from temp_draws
        union all
        select *
        from temp_ar_final) a
  where a.product = 'LOC'
  group by a.app_month
         , a.funding_month
         , a.draw_month
         , a.cust_type
         , a.product
         , a.loan_type
         , a.state_adj
         , a.source
         , a.trans_month
  order by 1, 2, 3, 4, 5
);

drop table if exists lead_master_temp;
create temp table lead_master_temp as
select
    l.lead_id
    , l.lead_uuid
    , l.lead_created_at
    , l.price_point
    , lp.state
    , l.reject_reason
    , l.purchase_decision
    , l.purchase_date
    , l.purchase_price
	, l.offer_amount
    , case when (l.reject_reason in ('Failed Lead Purchase Model', 'Leads WaterfallRules') or l.purchase_Decision = 'Purchased') then 1 else 0 end as credit_report_pulled
    , case when l.purchase_date is not null then 1 else 0 end as purchased
    , case when exists(select 1 from dw_reporting_bsf_leads.lead_url_tracker lt where l.lead_id = lt.lead_id and url_type = 'REDIRECT') then 1 else 0 end as redirected
    , case when l.lead_status = 'Accepted' then 1 else 0 end as accepted
    , case when fl.count_clicked_marketing_url > 0 or fl.count_clicked_redirect_url > 0 then 1 else 0 end as website_landed
    , fl.offer_accepted
    , case when l.lead_provider = 1 then 'lead_economy' else 'it_media' end as lead_provider
	, case when a.campaign is not null then 1 else 0 end as dm_offer
	, a.application_number
	, fa.loan_funded
	, fa.first_payment_default
  , fa.first_payment_due
	, fa.loan_expected_npv_initial
  , fa.loan_expected_npv_adjusted
	, (date_trunc('MONTH',l.lead_created_at) + Interval '1 Month - 1 day')::date as lead_created_month
       --DATE_TRUNC('month', l.lead_created_at)) AS lead_created_month
    --, TO_CHAR(DATE_TRUNC('week', l.lead_created_at), 'YYYY-MM-DD') AS lead_created_week
from dw_reporting_bsf_leads.lead l
inner join dw_reporting_bsf_leads.lead_parsed lp on l.lead_id = lp.lead_id
left join dw_reporting_bsf_leads.lead_rmodel_underwriting rm on rm.lead_id = l.lead_id
left join dw_reporting_bsf_origination.application a on a.lead_id = l.lead_id and now()<@a.effective and now()<@a.asserted
left join dw_reporting_views.fact_leads fl on l.lead_id = fl.lead_id
left join dw_reporting_views.fact_application fa on a.application_number = fa.application_number
where 1=1
	and (l.reject_reason not in ('Cool Off',
                                 'Existing Python Identity',
                                 'Existing Identity Check in BSF',
                                 'Bank Account Number Length',
                                 'Monthly Pay Amount',
                                 'State Check',
                                 'Blacklisted Routing Number',
                                 'Clarity/GDS error',
                                 'Price Point Reject',
                                 'Over Time Limit') or l.reject_reason is null)
    and l.lead_created_at is not null
    and l.lead_created_at::date >= '2018-09-30'::date
    --and l.lead_created_at::date < CURRENT_DATE - 1
    and now() <@l.effective
    and now() <@l.asserted
order by l.lead_id;
create index app_id22398762345 on lead_master_temp(application_number);

drop table if exists lead_master_temp_r;
create temp table lead_master_temp_r as
  select lead_created_month
       , sum(credit_report_pulled) as valid_looks
       , sum(purchased)            as purchased
       , sum(loan_funded)          as funded_count
       , sum(first_payment_due)    as loans_due
  from lead_master_temp
  where credit_report_pulled = 1
  group by 1;

select *
from loc_table
left join lead_master_temp_r
on app_month = lead_created_month
  and cust_type = 'New'
  and loan_type = 'base loan'
  and source = 'Lead'
  and trans_month = 1
  and state_adj = 'Non-CSO'
  and funding_month = app_month
