module.exports = {
	"pg_credentials": {
  					"user": "colldev",
  					"host": "localhost",
  					"database": "alma_dashboard",
  					"password": "colldev",
  					"port": 5432
					},
	"queries": {
        "refresh_ts_query": `select distinct timestamp, 'Funds' as tablename
                            from funds_table
                            union all
                            select distinct timestamp, 'Orders' as tablename
                            from pol_table
                            union all
                            select distinct timestamp, 'Wishlist' as tablename
                            from airtable_funds
                            `,
				"orders_query": `select (case
                                  when (pol_table.renewal_date is not null) and
                                       (pol_table.po_line_creation_date < pol_table.fiscal_period_start_date)
                                  then 'Renewal'
                                  else 'New Order'
                              end) as order_type,
                          pol_table.po_line_reference,
                          pol_table.po_line_title as title,
                          pol_table.renewal_date,
                          pol_table.vendor_code,
                          encumbrances.enc_fund_names,
                          encumbrances.enc_fund_codes,
                          encumbrances.encumbrance_amount,
                          expenditures.exp_fund_names,
                          expenditures.exp_fund_codes,
                          expenditures.expenditure_amount,
                          (case 
                              when array_length(invoice_lines.id, 1) = 1 
                              then (case 
                                      when invoice_lines.invoice_paid[1] = 'PAID'
                                      then 'Paid'
                                      else 'Received'
                                    end)
                              when array_length(invoice_lines.id, 1) is null
                              then 'No Invoice'
                              else 'Multiple Invoices'
                          end) as order_status,
                          expenditures.exp_fund_names <> encumbrances.enc_fund_names as funds_mismatch
                      from
                          pol_table
                      left join
                          (select po_line_reference,
                                  array_agg(distinct fund_ledger_name) as enc_fund_names,
                                  array_agg(distinct fund_ledger_code) as enc_fund_codes,
                                  sum(transaction_amount) as encumbrance_amount
                              from transactions_table
                              where transaction_item_sub_type in ('ENCUMBRANCE', 'DISENCUMBRANCE')
                              group by po_line_reference
                          ) as encumbrances on pol_table.po_line_reference = encumbrances.po_line_reference
                      left join
                          (select po_line_reference,
                                  array_agg(distinct fund_ledger_name) as exp_fund_names,
                                  array_agg(distinct fund_ledger_code) as exp_fund_codes,
                                  sum(transaction_amount) as expenditure_amount
                              from transactions_table
                              where transaction_item_sub_type in ('EXPENDITURE')
                              group by po_line_reference
                          ) as expenditures on pol_table.po_line_reference = expenditures.po_line_reference
                      left join
                          (select po_line_reference,
                              array_agg(invoice_approval_status) as invoice_received,
                              array_agg(invoice_payment_status) as invoice_paid,
                              array_agg(invoice_line_unique_identifier) as id
                          from invoice_line_table
                          where invoice_line_unique_identifier <> ':'
                          group by po_line_reference) as invoice_lines 
                          on pol_table.po_line_reference = invoice_lines.po_line_reference
                      union
                      select 'Wishlist' as order_type, 
                          concat(cast(order_id as text), '-wishlist') as po_line_reference,
                          resource_title as title,
                          null as renewal_date,
                          null as vendor_code,
                          array_agg(distinct fund_ledger_name) as enc_fund_names,
                          array_agg(distinct fund_ledger_code) as enc_fund_codes,
                          sum(allocation_amount_calculated) as encumbrance_amount,    
                          null as exp_fund_names,
                          null as exp_fund_codes,
                          null expenditure_amount,
                          json_build_object('negotiation_status', negotiation_status,
                                            'license_review_status', license_review_status) as order_status,
                          null as funds_mismatch
                       from wishlist_orders_table
                       group by resource_title, order_id, negotiation_status, license_review_status`,
	      "funds_query": `
            select 
              funds_table.balance_available as alma_balance_available,
              funds_table.fund_ledger_code,
              funds_table.fund_ledger_name,
              funds_table.ledger_name,
              funds_table.parent_fund_ledger_name,
              funds_table.transaction_encumbrance_amount,
              funds_table.transaction_expenditure_amount,
              funds_table.fiscal_period_description,
              funds_table.balance_available - coalesce(airtable_funds.total_allocated, 0) as wishlist_balance_available
            from funds_table
            left join
              airtable_funds
            on funds_table.fund_ledger_code = airtable_funds.fund_ledger_code`,
      "all_funds_bd_query": `
          select distinct dates.day,
              (select sum(transaction_allocation_amount) from funds_table) as total_alloc,
              sum(coalesce(exp.daily_exp, 0)) over (order by dates.day) as daily_exp,
              sum(coalesce(enc.daily_enc, 0)) over (order by dates.day) as daily_enc,
              (select sum(coalesce(total_allocated, 0)) from airtable_funds) as wishlist_proposed
          from dates
          left join 
              expenditures exp
          on dates.day = exp.expenditure_date and exp.fund_ledger_code is null and exp.ledger_name is null
          left join 
              encumbrances enc
          on dates.day = enc.encumbrance_date and enc.fund_ledger_code is null and enc.ledger_name is null
          order by dates.day`,
      "single_fund_bd_query": `
        select distinct dates.day,
            sum(coalesce(exp.daily_exp, 0)) over (order by dates.day) as daily_exp,
            sum(coalesce(enc.daily_enc, 0)) over (order by dates.day) as daily_enc,
            (select transaction_allocation_amount from funds_table where fund_ledger_code = $1) as total_alloc,
            (select coalesce(sum(total_allocated), 0) from airtable_funds where fund_ledger_code = $2) as wishlist_proposed
        from dates
        left join 
            expenditures exp
        on dates.day = exp.expenditure_date and exp.fund_ledger_code = $3
        left join 
            encumbrances enc
        on dates.day = enc.encumbrance_date and enc.fund_ledger_code = $4
        order by dates.day`,
    "single_ledger_bd_query": `
        select distinct dates.day,
            sum(coalesce(exp.daily_exp, 0)) over (order by dates.day) as daily_exp,
            sum(coalesce(enc.daily_enc, 0)) over (order by dates.day) as daily_enc,
            (select sum(transaction_allocation_amount) from funds_table where ledger_name = $1) as total_alloc,
            (select coalesce(sum(total_allocated), 0) from airtable_funds where ledger_name = $2) as wishlist_proposed
        from dates
        left join 
            expenditures exp
        on dates.day = exp.expenditure_date and exp.fund_ledger_code is null and exp.ledger_name = $3
        left join 
            encumbrances enc
        on dates.day = enc.encumbrance_date and enc.fund_ledger_code is null and enc.ledger_name = $4
        order by dates.day`
  }
}