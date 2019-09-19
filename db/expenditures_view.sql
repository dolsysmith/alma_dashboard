create materialized view expenditures as 
      select least(greatest(date %(start_date)s, transaction_date), date %(end_date)s)
                as expenditure_date,
                fund_ledger_code,
                ledger_name,
            sum(transaction_amount) as daily_exp
        from transactions_table
        where transaction_item_sub_type = 'EXPENDITURE'
        group by rollup(least(greatest(date %(start_date)s, transaction_date), date %(end_date)s),
                ledger_name,
                fund_ledger_code)