create materialized view dates as 
      select day::date from generate_series(
            timestamp %(start_date)s,
            timestamp %(last_valid_renewal)s,
            interval '1 day') as day