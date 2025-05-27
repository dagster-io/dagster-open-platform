SELECT *,
  CASE WHEN 
    sal_date IS NOT NULL
    and new_arr_bucket != 'NULL'
    and is_closed = true
    and opportunity_type = 'New Business'
    and date(created_at) > '2023-06-30'
  THEN 'training'
  WHEN
    sal_date IS NOT NULL
    and new_arr_bucket != 'NULL'
    and is_closed = false
    and opportunity_type = 'New Business'
    and date(created_at) > '2023-06-30'
  THEN 'prediction'
  ELSE NULL
  END AS split
FROM model_opportunity_win_probability