with fees as (
    select feeable_id
    , feeable_type
    , sum(case
        when type in('AdminFee', 'FactoringRequestAdminFee') then
            amount
        end) as admin_fee
    , sum(case         
        when type in('ApplicationFee', 'FactoringRequestApplicationFee') then
            amount
        end) as application_fees    
    , listagg(description, ', ') as description
    from prod_analytics.finance_platform.fees
    where 
    (fees.description not in('balance correction',
        'Ledger Adjustment',
        'Preseason Fee',
        'Preseason Fees',
        'preseasonfees',
        'starting_balance') OR fees.description is null)
    AND fees.feeable_type in ('Shipment','FactoringRequest')
    group by feeable_id, feeable_type
    having ((sum(case when type in('AdminFee', 'FactoringRequestAdminFee') then amount end)) is not null 
    OR (sum(case when type in('ApplicationFee', 'FactoringRequestApplicationFee') then amount end)) is not null)
)
, additional_fees as(
    select shipment_id
        , sum(amount) as add_fee
    from prod_analytics.finance_platform.additional_shipment_fees additional_fees
    group by shipment_id
)
, accept_ship as(
    select sum(COALESCE(accepted_quantity, quantity, 0)) as accepted_shipment
    	, sum(total_sale_price) as total_gmv
        , shipment_id
    from prod_analytics.finance_platform.shipment_items
    group by shipment_id
)
, grower as(
    Select max(grower.id) as grower_asset_id
        , r.grower_id
    from prod_analytics.finance_platform.asset_pools grower
    join prod_analytics.finance_platform.relationships r on grower.id = r.grower_id
    group by r.grower_id
)
, distr as(
    Select max(distributor.id) as distributor_asset_id
        , r.distributor_id
    from prod_analytics.finance_platform.asset_pools distributor
    join prod_analytics.finance_platform.relationships r on distributor.id = r.distributor_id
    group by r.distributor_id
)
, notes as(
    select listagg(n.body, ', ') as self_note_body
        , case
            when n.noteable_type like 'Fact%' then
                n.noteable_id
            when n.noteable_type like 'Ship%' then
                si.shipment_id
            end as id
        , listagg(n.noteable_type, ', ') as noteable_type
    from prod_analytics.finance_platform.notes n
    left join prod_analytics.finance_platform.shipment_items si on n.noteable_id = si.id 
    group by (case when n.noteable_type like 'Fact%' then n.noteable_id when n.noteable_type like 'Ship%' then si.shipment_id end)
)
, payments as(
    select sum(amount) as pay
        , factoring_request_id
    from prod_analytics.finance_platform.factoring_request_payments
    group by factoring_request_id
)
, balance as(
    select to_id
        , sum(coalesce(amount, 0)) as bal
    from prod_analytics.finance_platform.asset_balance_transfers
    where to_type = 'FactoringRequest'
    group by to_id
)
, migrated as(
	select s.marketplace_order_id
		, s.id
	from prod_analytics.finance_platform.shipments s
	where s.marketplace_order_id is null --chage back to null after testing
)
, sop as
(
    select distinct slug__c as slug
        , product_type__c as product_type
        , oms_transaction_id__c as trade_order_id
    from prod_analytics.salesforce.opportunity
    where not isdeleted
    and stagename != 'Canceled'
)
select U_ID
, TRADE_ORDER_ID
, STATE
, NAME
, CURRENCY_TYPE
, BUYER_COMPANY_NAME
, COMPANY_NAME
, RECEIVER_COMPANY_NAME
, DATE_SHIPMENT_CLOSED
, DATE_SHIPMENT_ACCEPTED
, DATE_SHIPMENT_SHIPPED
, TOTAL_GMV
, ESTIMATED_GMV
, SELLER_ADDR_STATE
, DIST_ADDR_STATE
, SHIPMENT_ID
, SELF_NOTE_BODY
, BUYER_DEDUCTION
, WIRE_RECEIVER
, PAYABLE_DAYS1
, ADVANCED_PERCENTAGE
, TOTAL_ADVANCE_AMOUNT
, DATE_SHIPMENT_ADVANCED
, SELLER_ADMIN_FEE_NOTES
, BUYER_ADMIN_FEE_NOTES
, ADMIN_FEE_NOTES
, ADMIN_FEE
, ADMIN_FEE_T
, APPLICATION_FEE
, FINANCING_FEE
, OTHER_FEE
, MARKETPLACE_FEE
, SERVICE_FEE
, SCS_FEES
, QUICKPAY_REBATE_REDUCTION_AMOUNT
, TOTAL_ADMIN_FEES_WITH_ADDITIONAL
, SELLER_ALL_FEES
, BUYER_ALL_FEES
, ALL_FEES
, SELLER_BASE_FEE_PERCENTAGE
, BUYER_BASE_FEE_PERCENTAGE
, BASE_FEE_PERCENTAGE
, OTHER_FEE_PERCENTAGE
, TOTAL_BASE_FEES
, REBATE_REDUCTION_RATE
, BUYER_COMMISSION
, SELLER_MARKETPLACE_FEE
, SELLER_MARKETPLACE_FEE_PERCENTAGE
, BUYER_MARKETPLACE_FEE
, BUYER_MARKETPLACE_FEE_PERCENTAGE
, PRODUCE_PAY_FEE
, COLLECTION_DUE_ON
, BANK_COMPANY_NAME
, COMPANY_COUNTRY
, BUYER_COMPANY_COUNTRY
, FULFILLMENT_STATUS_CODE
, FUNDING_STATUS_CODE
, ADVANCE_STATUS_CODE
, MAX_DATE_INVOICE
, MIN_DATE_INVOICE
, INVOICE_COUNT
, CONTRACT_TERM_CODE
, FACILITY_CODE
, FACILITY_NAME
, FINANCIAL_STATUS_CODE
, TOTAL_LATE_FEES
, SLUG
, TOTAL_QUANTITY
, date_shipment_paid_to_bank
, DAYS_PAST_DUE
, PRODUCT_TYPE
, SOURCE
from
(
    SELECT s.slug as u_id
        , '' as trade_order_id
        , s.state as state
        , s.reference_no as name
        , null as currency_type
        , comdist.name as buyer_company_name
        , comsh.name as company_name
        , '' as receiver_company_name
        , date(s.closed_at) as date_shipment_closed
        , date(s.accepted_at) as date_shipment_accepted
        , date(s.shipping_date) as date_shipment_shipped
        , si.total_gmv as total_gmv 
        , coalesce(s.fixed_estimated_shipment_value, s.calc_estimated_shipment_value, 0) as estimated_gmv
        , comsh.ADDR_STATE as seller_addr_state
        , comdist.ADDR_STATE as dist_addr_state
        , to_varchar(s.id) as shipment_id
        , notes.self_note_body as self_note_body
        , s.distributor_deduction as buyer_deduction
        , CASE
            WHEN s.grower_receives_advance THEN 'grower'
            ELSE 'distributor'
            END AS wire_receiver
        , s.payable_days1 as payable_days1
        , s.advance2_limit_percentage as advanced_percentage
        , case 
            when coalesce(s.fixed_advance1, s.calc_advance1) is null and coalesce(s.fixed_advance2, s.calc_advance2) is null 
            then null
            else coalesce(s.fixed_advance1, s.calc_advance1, 0) + coalesce(s.fixed_advance2, s.calc_advance2, 0)
            end as total_advance_amount
        , date(s.advanced1_at) as date_shipment_advanced
        , '' as seller_admin_fee_notes
        , '' as buyer_admin_fee_notes
        , fees.description as admin_fee_notes
        , fees.admin_fee as admin_fee
        , coalesce(fees.admin_fee, 0) + coalesce(fees.application_fees, 0) as admin_fee_t
        , fees.application_fees as application_fee
        , 0.00 as financing_fee
        , 0.00 as other_fee
        , 0.00 as marketplace_fee
        , 0.00 as service_fee
        , 0.00 as scs_fees
        , 0.00 as quickpay_rebate_reduction_amount
        , coalesce(additional_fees.add_fee, 0) + (coalesce(fees.admin_fee, 0) + coalesce(fees.application_fees, 0)) as total_admin_fees_with_additional
        , 0.00 as seller_all_fees
        , 0.00 as buyer_all_fees
        , 0.00 as all_fees
        , 0.00 as seller_base_fee_percentage
        , 0.00 as buyer_base_fee_percentage
        , s.base_servicing_fee1 as base_fee_percentage
        , 0.00 as other_fee_percentage
        , (coalesce(s.calc_base_fee1,0) + coalesce(s.calc_base_fee2,0)) AS  total_base_fees
        , TRUNC((COALESCE(s.daily_penalty1, 0.0) / 100.0) * 365.0, 4) as rebate_reduction_rate
        , s.distributor_commission as buyer_commission
        , coalesce(s.fixed_grower_marketplace_fee, s.calc_grower_marketplace_fee, 0) as seller_marketplace_fee
        , coalesce(s.grower_marketplace_fee_percentage, r.grower_marketplace_fee_percentage, 0) as seller_marketplace_fee_percentage
        , coalesce(s.fixed_distributor_marketplace_fee, s.calc_distributor_marketplace_fee, 0) as buyer_marketplace_fee    
        , coalesce(s.distributor_marketplace_fee_percentage, r.distributor_marketplace_fee_percentage, 0) as buyer_marketplace_fee_percentage
        , s.calc_pp_fees as produce_pay_fee 
        , date(s.collection_due_on) as collection_due_on
        , 'ProducePay' as bank_company_name
        , comsh.country as company_country  	        
        , comdist.country as buyer_company_country
        , '' as fulfillment_status_code
        , '' as funding_status_code
        , '' as advance_status_code
        , to_date(null) as max_date_invoice
        , to_date(null) as min_date_invoice
        , 0 as invoice_count
        , '' as contract_term_code   
        , '' as facility_code
        , '' as facility_name
        , '' as FINANCIAL_STATUS_CODE
        , (coalesce(s.calc_late_fee1,0) + coalesce(s.calc_late_fee2, 0)) AS total_late_fees
        , s.slug as slug
        , si.accepted_shipment as total_quantity
        , date(s.settled_at) as date_shipment_paid_to_bank
        , null as days_past_due
        , sop.product_type as product_type
        , 'Legacy' as source    
    FROM prod_analytics.finance_platform.shipments as s
    join prod_analytics.finance_platform.relationships r on r.id = s.relationship_id
    join prod_analytics.finance_platform.companies comsh on comsh.id = r.grower_id
    join prod_analytics.finance_platform.companies comdist on comdist.id = r.distributor_id
    left join fees on fees.feeable_id = s.id and fees.feeable_type = 'Shipment'
    left join accept_ship si on si.shipment_id = s.id
    left join distr on distr.distributor_id = s.id
    left join grower on grower.grower_id = s.id
    left join additional_fees on additional_fees.shipment_id = s.id
    left join notes on notes.id = s.id and notes.noteable_type like 'Ship%'
    left join sop on sop.slug = s.slug
UNION
    SELECT fr.slug as u_id  
        , '' as trade_order_id
        , fr.state as state
        , fi.invoice_number as name
        , null as currency_type
        , com.name as buyer_company_name
        , null as company_name
        , '' as receiver_company_name
        , date(fr.closed_at) as date_shipment_closed
        , date(fr.approved_at) as date_shipment_accepted
        , date(fr.created_at) as date_shipment_shipped
        , payments.pay + balance.bal as total_gmv
        , fi.amount AS estimated_gmv
        , '' as seller_addr_state
        , com.addr_state as dist_addr_state
        , to_varchar(fr.shipment_id) as shipment_id
        , notes.self_note_body as self_note_body
        , 0 as buyer_deduction
        , 'distributor' as wire_receiver
        , fr.payable_days1 as payable_days1
        , fr.advance2_limit_percentage as advanced_percentage
        , coalesce(fr.fixed_advance1, fr.calc_advance1, null) as total_advance_amount
        , date (fr.advanced_at) as date_shipment_advanced
        , '' as seller_admin_fee_notes
        , '' as buyer_admin_fee_notes
        , fees.description as admin_fee_notes
        , fees.admin_fee as admin_fee
        , coalesce(fees.admin_fee, 0) + coalesce(fees.application_fees, 0) as admin_fee_t
        , fees.application_fees as application_fee
        , 0.00 as financing_fee
        , 0.00 as other_fee
        , 0.00 as marketplace_fee
        , 0.00 as service_fee
        , 0.00 as scs_fees
        , 0.00 as quickpay_rebate_reduction_amount
        , (coalesce(fees.admin_fee, 0) + coalesce(fees.application_fees, 0)) as total_admin_fees_with_additional
        , 0.00 as seller_all_fees
        , 0.00 as buyer_all_fees
        , 0.00 as all_fees
        , 0.00 as seller_base_fee_percentage
        , 0.00 as buyer_base_fee_percentage
        , 0.00 as base_fee_percentage
        , 0.00 as other_fee_percentage
        , (coalesce(fr.calc_base_fee1, 0) + coalesce(fr.calc_base_fee2, 0)) AS total_base_fees
        , null as rebate_reduction_rate
        , 0 as buyer_commission
        , null as seller_marketplace_fee
        , null as seller_marketplace_fee_percentage
        , null as buyer_marketplace_fee
        , null as buyer_marketplace_fee_percentage
        , (coalesce(fr.calc_base_fee1, 0) + coalesce(fr.calc_late_fee2, 0)) as produce_pay_fee
        , date(fr.collection_due_on) as collection_due_on
        , 'ProducePay' as bank_company_name
        , com.country AS company_country       
        , com.country AS buyer_company_country
        , '' as fulfillment_status_code
        , '' as funding_status_code
        , '' as advance_status_code
        , to_date(null) as max_date_invoice
        , to_date(null) as min_date_invoice
        , 0 as invoice_count
        , '' as contract_term_code
        , '' as facility_code
        , '' as facility_name
        , '' as FINANCIAL_STATUS_CODE
        , (coalesce(fr.calc_late_fee1, 0) + coalesce(fr.calc_late_fee2, 0)) AS total_late_fees
        , fr.slug as slug
        , 0 as total_quantity
        , date(fr.settled_at) as date_shipment_paid_to_bank
        , null as days_past_due
        , sop.product_type as product_type
        , 'Legacy' as source
FROM prod_analytics.finance_platform.factoring_requests as fr
JOIN prod_analytics.finance_platform.factoring_invoices as fi ON fr.id = fi.factoring_request_id
join prod_analytics.finance_platform.companies com on com.id = fr.distributor_id
left join fees on fees.feeable_id = fr.id and fees.feeable_type = 'FactoringRequest'
left join payments on payments.factoring_request_id = fr.id
left join balance on balance.to_id = fr.id
left join notes on notes.id = fr.id and notes.noteable_type like 'Fact%'
left join sop on sop.slug = fr.slug
)us
join migrated on migrated.id = us.shipment_id
UNION
(
    select trade_order_id as U_ID
        , TRADE_ORDER_ID
        , STATE
        , NAME
        , CURRENCY_TYPE
        , BUYER_COMPANY_NAME
        , COMPANY_NAME
        , RECEIVER_COMPANY_NAME
        , DATE_SHIPMENT_CLOSED
        , DATE_SHIPMENT_ACCEPTED
        , DATE_SHIPMENT_SHIPPED
        , TOTAL_GMV
        , ESTIMATED_GMV
        , SELLER_ADDR_STATE
        , DIST_ADDR_STATE
        , SHIPMENT_ID
        , SELF_NOTE_BODY
        , BUYER_DEDUCTION
        , WIRE_RECEIVER
        , PAYABLE_DAYS1
        , ADVANCED_PERCENTAGE
        , TOTAL_ADVANCE_AMOUNT
        , DATE_SHIPMENT_ADVANCED
        , SELLER_ADMIN_FEE_NOTES
        , BUYER_ADMIN_FEE_NOTES
        , ADMIN_FEE_NOTES
        , ADMIN_FEE
        , ADMIN_FEE_T
        , APPLICATION_FEE
        , FINANCING_FEE
        , OTHER_FEE
        , MARKETPLACE_FEE
        , SERVICE_FEE
        , SCS_FEES
        , QUICKPAY_REBATE_REDUCTION_AMOUNT
        , TOTAL_ADMIN_FEES_WITH_ADDITIONAL
        , SELLER_ALL_FEES
        , BUYER_ALL_FEES
        , ALL_FEES
        , SELLER_BASE_FEE_PERCENTAGE
        , BUYER_BASE_FEE_PERCENTAGE
        , BASE_FEE_PERCENTAGE
        , OTHER_FEE_PERCENTAGE
        , TOTAL_BASE_FEES
        , REBATE_REDUCTION_RATE
        , BUYER_COMMISSION
        , SELLER_MARKETPLACE_FEE
        , SELLER_MARKETPLACE_FEE_PERCENTAGE
        , BUYER_MARKETPLACE_FEE
        , BUYER_MARKETPLACE_FEE_PERCENTAGE
        , PRODUCE_PAY_FEE
        , COLLECTION_DUE_ON
        , BANK_COMPANY_NAME
        , COMPANY_COUNTRY
        , BUYER_COMPANY_COUNTRY
        , FULFILLMENT_STATUS_CODE
        , FUNDING_STATUS_CODE
        , ADVANCE_STATUS_CODE
        , MAX_DATE_INVOICE
        , MIN_DATE_INVOICE
        , INVOICE_COUNT
        , CONTRACT_TERM_CODE
        , FACILITY_CODE
        , FACILITY_NAME
        , FINANCIAL_STATUS_CODE
        , TOTAL_LATE_FEES
        , SLUG
        , TOTAL_QUANTITY
        , date(null) as date_shipment_paid_to_bank
        , DAYS_PAST_DUE
        , PRODUCT_TYPE
        , 'TMS' as SOURCE
    from prod_analytics.product_analytics.vw_finance_tms_report
)
LIMIT 100

