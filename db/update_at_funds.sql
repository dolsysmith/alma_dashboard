select 
    funds_table.balance_available as alma_balance_available,
    airtable_funds.id
    from funds_table
    inner join 
        airtable_funds
    on funds_table.fund_ledger_code = airtable_funds.fund_ledger_code