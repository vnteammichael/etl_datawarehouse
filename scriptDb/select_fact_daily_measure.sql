SELECT
	D.full_date,
	S.social_name,
	P.platform_name,
	L.level_range,
	F.total_daily_active_users,
	F.total_new_register_users,
	F.total_paying_users,
	F.total_purchases,
	F.total_revenue,
	F.total_first_paying_users
FROM
	fact_daily_measure F
	INNER JOIN dim_date D ON (F.date_id = D.id)
	LEFT JOIN dim_social S ON (F.social_id = S.id)
	LEFT JOIN dim_platform P ON (F.platform_id = P.id)
	LEFT JOIN dim_level_range L ON (F.level_range_id = L.id)
