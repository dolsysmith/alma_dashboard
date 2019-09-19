create materialized view encumbrances as 
       select e1.transaction_date as encumbrance_date,
        e1.fund_ledger_code,
        e1.ledger_name,
        sum(e1.transaction_amount) as daily_enc
        from (select coalesce(renewal_date, transaction_date) as transaction_date,
            fund_ledger_code,
            ledger_name,
            transaction_amount
            from transactions_table
            inner join 
                pol_table
            on transactions_table.po_line_reference = pol_table.po_line_reference
            where transaction_item_sub_type in ('ENCUMBRANCE', 'DISENCUMBRANCE')
        ) e1
        group by rollup(e1.transaction_date, 
                    e1.ledger_name,
                    e1.fund_ledger_code
                    )