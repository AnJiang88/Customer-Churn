with
	reporting_period as (
		select min(candidate_date) as begin_date
		from
			(
				-- calendar year-based start
				select
					dateadd('year', -3, date_trunc('year', current_date - 1))
					as candidate_date

				union

				-- school year-based start
				select dateadd('year', -3, begin_date) as candidate_date
				from weld_north_prod.il_math_dim_model.vw_curr_ay
			)
	),

	edgenuity_data as (
		select
			'Edgenuity' as platform,
			user_groups.ugr_group_desc as user_role,
			user_status.sta_description as user_status,
			date_trunc('month', session_log.ssl_log_on_date_time)::date as month_start,
			session_log.ssl_user_id as user_id,
			sum(
				datediff(
					second,
					session_log.ssl_log_on_date_time,
					coalesce(session_log.ssl_last_access, session_log.ssl_log_on_date_time)
				)
			) as usage_seconds
		from weld_north_prod.edge_lms.session_log
		inner join weld_north_prod.edge_lms.users
			on users.usr_user_id = session_log.ssl_user_id
			and users.realm_id = session_log.realm_id
		inner join weld_north_prod.edge_lms.user_groups
			on user_groups.ugr_group_id = users.usr_group_id
			and user_groups.realm_id = users.realm_id
		inner join weld_north_prod.edge_lms.user_status
			on user_status.sta_status_id = users.usr_status
			and user_status.realm_id = users.realm_id
		inner join weld_north_prod.edge_lms.schools
			on schools.sch_school_id = users.usr_school_id
			and schools.realm_id = users.realm_id
		inner join weld_north_prod.edge_lms.district
			on district.dis_district_id = schools.sch_district_id
			and district.realm_id = schools.realm_id
		inner join weld_north_prod.edge_lms.district_type
			on district_type.id = district.dis_account_type
			and district_type.realm_id = district.realm_id
		where
			(select begin_date from reporting_period)
			<= session_log.ssl_log_on_date_time::date
			and session_log.ssl_log_on_date_time::date
			< date_trunc('month', current_date)
			-- exclude zero-second sessions
			and datediff(
				second,
				session_log.ssl_log_on_date_time,
				coalesce(session_log.ssl_last_access, session_log.ssl_log_on_date_time)
			) > 0
			and district_type.reporting
			and lower(district.dis_district_name) not like '%internal use%'
			and district.dis_district_id != '-999999'
			and lower(schools.sch_school_name) not like '%sandbox%'
			and lower(schools.sch_school_name) not like '%course manag%'
		group by
			platform,
			user_id,
			user_role,
			user_status,
			month_start
	),

	ll_espanol_data as (
		select
			'Student' as user_role,
			'Not Applicable' as user_status,
			date_trunc('month', starttime)::date as month_start,
			studentid as user_id,
			case producttag
				when 'ILE' then 'Language & Literacy'
				when 'Spanish' then 'Español'
			end as platform,
			sum(sessiontime) as usage_seconds
		from
			weld_north_prod.imagine_learning.il_events_stg_tbl_student_session_historical
		where producttag in ('ILE', 'Spanish')
			and (select begin_date from reporting_period) <= starttime::date
			and starttime::date < date_trunc('month', current_date)
			-- exclude zero-second sessions
			and sessiontime > 0
		group by
			platform,
			user_id,
			user_role,
			user_status,
			month_start
	),

	math_data as (
		select
			'Math' as platform,
			'Student' as user_role,
			'Not Applicable' as user_status,
			dim_student.student_nid as user_id,
			date_trunc('month', agg_student_usage_daily.local_date_sid) as month_start,
			sum(agg_student_usage_daily.time_on_system) as usage_seconds
		from weld_north_prod.il_math_dim_model.agg_student_usage_daily
		inner join weld_north_prod.il_math_dim_model.dim_student
			on dim_student.student_sid = agg_student_usage_daily.student_sid
		where
			(select begin_date from reporting_period)
			<= agg_student_usage_daily.local_date_sid
			and agg_student_usage_daily.local_date_sid
			< date_trunc('month', current_date)
			-- exclude zero-second times on system
			and agg_student_usage_daily.time_on_system > 0
			and not dim_student.is_demo
		group by
			platform,
			user_id,
			user_role,
			user_status,
			month_start
	),

	math_facts_data as (
		select
			'Math Facts' as platform,
			'Student' as user_role,
			'Not Applicable' as user_status,
			dim_student.student_nid as user_id,
			date_trunc(
				'month', agg_imf_student_usage_daily.local_date_sid
			) as month_start,
			sum(agg_imf_student_usage_daily.total_time_played) as usage_seconds
		from weld_north_prod.il_math_dim_model.agg_imf_student_usage_daily
		inner join weld_north_prod.il_math_dim_model.dim_student
			on dim_student.student_sid = agg_imf_student_usage_daily.student_sid
		where
			(select begin_date from reporting_period)
			<= agg_imf_student_usage_daily.local_date_sid
			and agg_imf_student_usage_daily.local_date_sid
			< date_trunc('month', current_date)
			-- exclude zero-second times on system
			and agg_imf_student_usage_daily.total_time_played > 0
			and not dim_student.is_demo
		group by
			platform,
			user_id,
			user_role,
			user_status,
			month_start
	),

	-- Supplemental CTEs for use in mypath_k12_reading_lectura_data
	abridged_login_data as (
		select distinct
			user_key,
			session_id,
			user_uuid
		from weld_north_prod.wne_dim_model.login_event_fact
		where
			(select begin_date from reporting_period)
			<= event_time_iso::date
			and event_time_iso::date
			< date_trunc('month', current_date)
			and app_domain_key in ('11', '33', '60', '111')
	),

	mypath_k12_data as (
		select
			'Student' as user_role,
			'Not Applicable' as user_status,
			'MyPath K-12' as platform,
			coalesce(
				content_event_log.user_uuid,
				vw_user_dim_ala.user_uuid,
				abridged_login_data.user_uuid
			)
			as user_id_alias,
			date_trunc('month', content_event_log.event_time_iso::date) as month_start,
			sum(content_event_log.delta_time) as usage_seconds
		from weld_north_prod.wne_dim_model.content_event_log
		inner join weld_north_prod.wne_dim_model.vw_user_dim_ala
			on vw_user_dim_ala.user_key = content_event_log.user_key
		inner join abridged_login_data
			on abridged_login_data.user_key = content_event_log.user_key
			and abridged_login_data.session_id = content_event_log.session_id
		where
			(select begin_date from reporting_period)
			<= content_event_log.event_time_iso::date
			and content_event_log.event_time_iso::date
			< date_trunc('month', current_date)
			-- exclude zero-second times on system
			and content_event_log.delta_time > 0
			and content_event_log.app_domain_key in ('11', '60')
		group by
			platform,
			user_id_alias,
			user_role,
			user_status,
			month_start
	),

	reading_lectura_data as (
		select
			'Student' as user_role,
			'Not Applicable' as user_status,
			case content_event_log.app_domain_key
				when '33' then 'Lectura'
				when '111' then 'Reading'
			end as platform,
			case content_event_log.app_domain_key
				when
					'33'
					then coalesce(
							content_event_log.user_uuid,
							user_dim.user_uuid,
							abridged_login_data.user_uuid
						)
				when
					'111'
					then coalesce(
							content_event_log.user_uuid,
							user_dim.user_uuid,
							abridged_login_data.user_uuid
						)
			end as user_id_alias,
			date_trunc('month', content_event_log.event_time_iso::date) as month_start,
			sum(content_event_log.delta_time) as usage_seconds
		from weld_north_prod.wne_dim_model.content_event_log
		inner join weld_north_prod.wne_dim_model.user_dim
			on user_dim.user_key = content_event_log.user_key
		inner join abridged_login_data
			on abridged_login_data.user_key = content_event_log.user_key
			and abridged_login_data.session_id = content_event_log.session_id
		where
			(select begin_date from reporting_period)
			<= content_event_log.event_time_iso::date
			and content_event_log.event_time_iso::date
			< date_trunc('month', current_date)
			-- exclude zero-second times on system
			and content_event_log.delta_time > 0
			and content_event_log.app_domain_key in ('33', '111')
		group by
			platform,
			user_id_alias,
			user_role,
			user_status,
			month_start
	),

	odyssey_data as (
		select
			'Odyssey' as platform,
			role.description as user_role,
			status.description as user_status,
			tlo_user.user_id as user_id,
			date_trunc('month', attendance_repository.logon_time::date) as month_start,
			sum(
				timestampdiff(
					second,
					attendance_repository.logon_time,
					attendance_repository.logoff_time
				)
			) as usage_seconds
		from weld_north_prod.ody_tlo.attendance_repository
		inner join weld_north_prod.ody_tlo.tlo_user
			on tlo_user.user_id = attendance_repository.user_id
		inner join weld_north_prod.ody_tlo.role
			on role.role_id = tlo_user.role_id
		inner join weld_north_prod.ody_tlo.account
			on account.account_id = tlo_user.account_id
		inner join weld_north_prod.ody_tlo.status
			on status.status_cd = account.status
		where
			(select begin_date from reporting_period)
			<= attendance_repository.logon_time::date
			and attendance_repository.logon_time::date
			< date_trunc('month', current_date)
			and attendance_repository.logoff_time is not null
			-- exclude zero-second times on system
			and timestampdiff(
				second,
				attendance_repository.logon_time,
				attendance_repository.logoff_time
			) > 0
		group by
			platform,
			tlo_user.user_id,
			user_role,
			user_status,
			month_start
	),

	ow_ignitia_data as (
		select
			sales_channel.sales_channel_title as platform,
			app_user.app_user_uuid as user_id,
			initcap(replace(app_user.app_user_type, '_', ' ')) as user_role,
			initcap(replace(app_user.app_user_status, '_', ' ')) as user_status,
			date_trunc('month', app_user_session.session_opened_at::date) as month_start,
			sum(
				timestampdiff(
					second,
					app_user_session.session_opened_at,
					app_user_session.session_closed_at
				)
			) as usage_seconds
		from weld_north_prod.ow_lms.app_user_session
		inner join weld_north_prod.ow_lms.app_user
			on app_user.app_user_id = app_user_session.app_user_id
			and app_user.realm_id = app_user_session.realm_id
		inner join weld_north_prod.ow_lms.school
			on school.school_id = app_user.school_id
			and school.realm_id = app_user.realm_id
		inner join weld_north_prod.ow_lms.customer
			on customer.customer_id = school.customer_id
			and customer.realm_id = school.realm_id
		inner join weld_north_prod.ow_lms.sales_channel
			on sales_channel.sales_channel_name = customer.sales_channel_type
			and sales_channel.realm_id = customer.realm_id
		where
			(select begin_date from reporting_period)
			<= app_user_session.session_opened_at::date
			and app_user_session.session_opened_at::date
			< date_trunc('month', current_date)
			-- exclude zero-second times on system
			and timestampdiff(
				second,
				app_user_session.session_opened_at,
				app_user_session.session_closed_at
			) > 0
			and not customer.hidden
		group by
			platform,
			user_id,
			user_role,
			user_status,
			month_start
	),

	all_data as (
		select
			platform,
			user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from edgenuity_data

		union all

		select
			platform,
			user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from ll_espanol_data

		union all

		select
			platform,
			user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from math_data

		union all

		select
			platform,
			user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from math_facts_data

		union all

		select
			platform,
			user_id_alias as user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from mypath_k12_data

		union all

		select
			platform,
			user_id_alias as user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from reading_lectura_data

		union all

		select
			platform,
			user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from odyssey_data

		union all

		select
			platform,
			user_id,
			user_role,
			user_status,
			month_start,
			usage_seconds
		from ow_ignitia_data
	)

select
	platform,
	all_data.user_id,
    user_org_dim.org_id as org_id,
    user_org_dim.org_uuid as org_uuid,
    netsuite_middleware_cloud.netsuite_id as netsuite_id,
	user_role,
	user_status,
	month_start,
	usage_seconds
from all_data
inner join weld_north_prod.wne_dim_model.user_org_dim
    on user_org_dim.user_id = all_data.user_id
inner join sandbox.scratch.netsuite_middleware_cloud
    on netsuite_middleware_cloud.organization_id = user_org_dim.org_uuid