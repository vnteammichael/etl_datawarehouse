SELECT
	D.full_date,
	D.quarter,
	D.day,
	D.week,
	D.month,
	D.year,
	G.game,
	U.user_name,
	E.department,
	H.valid_bet_amount,
	H.recharge_amount,
	H.times,
	H.scores
FROM
	user_history H
	INNER JOIN dim_date D ON (H.date_id = D.id)
	LEFT JOIN dim_game G ON (H.game_id = G.id)
	LEFT JOIN dim_user U ON (H.user_id = U.id)
	LEFT JOIN dim_department E ON (U.department_id = E.id)