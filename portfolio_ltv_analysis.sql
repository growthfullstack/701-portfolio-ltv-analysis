with user_metadata as (
select user_id
    , country
    , min(case when lead_app <> application then date end) as cross_promo_date
    , ROW_NUMBER() OVER (partition by user_id) as row_number
from user_portfolio_ltv
where 
lead_app_install_date  >= CURRENT_DATE - 67
and lead_app_install_date < CURRENT_DATE - 60 -- we only want mature cohorts
and platform is not null -- data quality issues
and coalesce(ad_rev,iap_rev) > 0
group by 1,2
), 
user_portfolio_ltv_pivot as (
select DATE 
    , user_portfolio_ltv.user_id
    , lead_app_install_date
    , lead_app
    , platform
    , user_metadata.country
    , cross_promo_date
    , sum(case when days_since_lead_app_install <= 60 and lead_app = application then iap_rev + ad_rev end) as d60_ltv_lead_app
    , sum(case when days_since_lead_app_install <= 60 and lead_app <> application then iap_rev + ad_rev end) as d60_ltv_other_app
    , sum(case when days_since_lead_app_install <= 30 then iap_rev + ad_rev end) as d30_ltv
    , sum(case when days_since_lead_app_install <= 7 then iap_rev + ad_rev end) as d7_ltv
    , sum(case when days_since_lead_app_install <= 3 then iap_rev + ad_rev end) as d3_ltv
    , sum(case when days_since_lead_app_install <= 1 then iap_rev + ad_rev end) as d1_ltv

from user_portfolio_ltv
inner join user_metadata on user_metadata.user_id = user_portfolio_ltv.user_id
                                  and row_number = 1 --filter edge cases
where 
lead_app_install_date  >= CURRENT_DATE - 67
and lead_app_install_date < CURRENT_DATE - 60 -- we only want mature cohorts
and platform is not null -- filter data quality issues
group by 1,2,3,4,5,6,7
),
cross_promo_metrics as (
select user_id 
      , lead_app 
      , platform
      , country 
      , coalesce(sum(case when date < cross_promo_date or cross_promo_date is null then d60_ltv_lead_app end),0) as d60_ltv_lead_app_before
      , coalesce(sum(case when date >= cross_promo_date then d60_ltv_lead_app end),0) as d60_ltv_lead_app_after
      , coalesce(sum(case when date >= cross_promo_date then d60_ltv_other_app end),0) as d60_ltv_other_app
from user_portfolio_ltv_pivot
group by 1,2,3,4
),
start_end_date as (
    select min(lead_app_install_date) as start_date,
           max(lead_app_install_date) as end_date
    from user_portfolio_ltv_pivot
)
select  
         replace(lead_app, '''', ' ') as lead_app
        , platform 
        , country 
        , start_date as start_install_date
        , end_date as end_install_date
        , case when d60_ltv_other_app > 0 then 'several' else 'single' end as apps_played
        , case when d60_ltv_other_app > 0 and d60_ltv_lead_app_after > 0 then 'continue' 
              when d60_ltv_other_app > 0 then 'stop' end as lead_app_status
        , count(user_id) as lead_app_installs
        , sum(d60_ltv_lead_app_before) as d60_ltv_lead_app_before
        , sum(d60_ltv_lead_app_after) as d60_ltv_lead_app_after
        , sum(d60_ltv_other_app) as d60_ltv_other_app
        , sum(d60_ltv_lead_app_before+d60_ltv_lead_app_after) as d60_lead_app
        , sum(d60_ltv_lead_app_before+d60_ltv_lead_app_after+d60_ltv_other_app) as d60_portfolio_revenue
        , count(case when d60_ltv_other_app > 0 then user_id end) as lead_app_installs_several
from cross_promo_metrics
left join start_end_date on 1=1
group by 1,2,3,4,5,6,7
order by 1