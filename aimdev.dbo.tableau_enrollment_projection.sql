--Run the following lines in SSMS to update the table for tableau dashboard
--Then refresh tableau data extract to update the dashboard
--Then republish the dashboard to the server 
--------------------------------------------------------------------------------------
--use aimdev;
--go
--exec dbo.enrollment_projection_tableau2;


---------------------------------------------------------------------------------------------------------
--SQL of the stored procedure

USE aimdev;  
GO
create procedure dbo.enrollment_projection_tableau2
--alter procedure dbo.enrollment_projection_tableau2   
AS   

--------------------------------------------------------------------------------------
--                  Update Raw Data Table (GC and Nursing Combined)
--------------------------------------------------------------------------------------
--Load any new data into the static overall table
insert into aimdev.dbo.enrl_proj_ug_raw_data
select seq, laps_term, cohort, avg(headcount) as headcount, res_status, as_admit
from
(
	select 
		case when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=1 then 25 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=2 then 26 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=0 then 27
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=1 then 19 
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=2 then 20
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=0 then 21
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=1 then 19 
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=2 then 20
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=0 then 21
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=1 then 19 
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=2 then 20
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=0 then 21
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=1 then 10 
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=2 then 11
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=0 then 12
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=1 then 10 
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=2 then 11
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=0 then 12
			 else seq end as seq, 
		laps_term, 
		case when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=1 then cohort+seq-25 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=2 then cohort+seq-26 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=0 then cohort+seq-27
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=1 then cohort+seq-10 
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=2 then cohort+seq-11
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=0 then cohort+seq-12
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=1 then cohort+seq-10 
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=2 then cohort+seq-11
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=0 then cohort+seq-12
			 else cohort end as cohort,
		headcount,
		res_status,
		as_admit
	from
	(
		select 
			seq, laps_term, cohort, 
			case when res_status = 'R' and as_admit = 'F' and seq >= 25 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'N' and as_admit = 'F' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'I' and as_admit = 'F' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'R' and as_admit = 'A' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'N' and as_admit = 'A' and seq >= 10 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'I' and as_admit = 'A' and seq >= 10 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 else headcount end as headcount, 
			res_status, as_admit
		from
		(
			select 
				laps_term, cohort, res_status, as_admit,
				laps_term-cohort+1 as seq, 
				count(distinct uid_no) as headcount, 
				case when res_status = 'R' and as_admit = 'F' and laps_term-cohort+1 >= 25 then 1 
					 when res_status = 'N' and as_admit = 'F' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'I' and as_admit = 'F' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'R' and as_admit = 'A' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'N' and as_admit = 'A' and laps_term-cohort+1 >= 10 then 1
					 when res_status = 'I' and as_admit = 'A' and laps_term-cohort+1 >= 10 then 1
					 else 0 end as cap_flag
			from
			(
				select
					t1.laps_term, t1.uid_no, t1.as_admit, 
					min(t1.first_reg_abs) over(partition by t2.acad_yr_trail_sum) as cohort, 
					--case when t1.css_residence_status = 'N' and t1.fee_payer_status = 'Non-Resident Fee Payer' then 'N' else 'R' end as res_status
					case when t1.reducedethnic = 'Foreign' AND t1.css_residence_status = 'N' AND t1.fee_payer_status = 'Non-Resident Fee Payer' then 'I'  
						 when t1.reducedethnic <> 'Foreign' AND t1.css_residence_status = 'N' AND t1.fee_payer_status = 'Non-Resident Fee Payer' then 'N'
						 else 'R' end as res_status
				from 
					aimdr.dbo.headcount_p_tb t1,
					aimdr.dbo.laps_term_p_tb t2
				where 
					t1.first_reg_abs = t2.laps_term 
					and t2.summer_flag <> 1
					and t1.laps_term >= t1.first_reg_abs
					and t1.css_student_level in ('1','2','3','4') 
					and t1.laps_term > (select max(laps_term) from aimdev.dbo.enrl_proj_ug_raw_data)
			)q1
			group by laps_term, cohort, res_status, as_admit
		)q2
	)q3
)q4
group by seq, laps_term, cohort, res_status, as_admit;

declare @max_cohort float = (select max(cohort) from aimdev.dbo.enrl_proj_ug_raw_data);


--Refresh GC raw data table2 with two residence groups
IF object_id('aimdev.dbo.enrl_proj_ug_raw_data2') IS NOT NULL
  BEGIN
    TRUNCATE TABLE aimdev.dbo.enrl_proj_ug_raw_data2 
    DROP TABLE aimdev.dbo.enrl_proj_ug_raw_data2 
  END;

select seq, laps_term, cohort, as_admit, res_status, sum(cast(headcount as int)) as headcount
into aimdev.dbo.enrl_proj_ug_raw_data2
from (select seq, laps_term, cohort, as_admit, case when res_status in ('N','I') then 'N' else res_status end as res_status, headcount from aimdev.dbo.enrl_proj_ug_raw_data)q
group by seq, laps_term, cohort, as_admit, res_status;


--------------------------------------------------------------------------------------
--                            Load Initial Nursing Data Table
--------------------------------------------------------------------------------------
/*
--Ran the following code block to create nursing data static table on May 8, 2018
--------------------------------------------------------------------------------------
select seq, laps_term, cohort, avg(headcount) as headcount, res_status, as_admit
into aimdev.dbo.enrl_proj_ug_nursing_raw_data
from
(
	select 
		case when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=1 then 25 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=2 then 26 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=0 then 27
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=1 then 19 
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=2 then 20
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=0 then 21
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=1 then 19 
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=2 then 20
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=0 then 21
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=1 then 19 
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=2 then 20
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=0 then 21
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=1 then 10 
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=2 then 11
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=0 then 12
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=1 then 10 
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=2 then 11
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=0 then 12
			 else seq end as seq, 
		laps_term, 
		case when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=1 then cohort+seq-25 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=2 then cohort+seq-26 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=0 then cohort+seq-27
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=1 then cohort+seq-10 
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=2 then cohort+seq-11
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=0 then cohort+seq-12
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=1 then cohort+seq-10 
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=2 then cohort+seq-11
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=0 then cohort+seq-12
			 else cohort end as cohort,
		headcount,
		res_status,
		as_admit
	from
	(
		select 
			seq, laps_term, cohort, 
			case when res_status = 'R' and as_admit = 'F' and seq >= 25 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'N' and as_admit = 'F' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'I' and as_admit = 'F' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'R' and as_admit = 'A' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'N' and as_admit = 'A' and seq >= 10 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'I' and as_admit = 'A' and seq >= 10 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 else headcount end as headcount, 
			res_status, as_admit
		from
		(
			select 
				laps_term, cohort, res_status, as_admit,
				laps_term-cohort+1 as seq, 
				count(distinct uid_no) as headcount, 
				case when res_status = 'R' and as_admit = 'F' and laps_term-cohort+1 >= 25 then 1 
					 when res_status = 'N' and as_admit = 'F' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'I' and as_admit = 'F' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'R' and as_admit = 'A' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'N' and as_admit = 'A' and laps_term-cohort+1 >= 10 then 1
					 when res_status = 'I' and as_admit = 'A' and laps_term-cohort+1 >= 10 then 1
					 else 0 end as cap_flag
			from
			(
				select
					t1.laps_term, t1.uid_no, t1.as_admit, 
					min(t1.first_reg_abs) over(partition by t2.acad_yr_trail_sum) as cohort, 
					--case when t1.css_residence_status = 'N' and t1.fee_payer_status = 'Non-Resident Fee Payer' then 'N' else 'R' end as res_status
					case when t1.reducedethnic = 'Foreign' AND t1.css_residence_status = 'N' AND t1.fee_payer_status = 'Non-Resident Fee Payer' then 'I'  
						 when t1.reducedethnic <> 'Foreign' AND t1.css_residence_status = 'N' AND t1.fee_payer_status = 'Non-Resident Fee Payer' then 'N'
						 else 'R' end as res_status
				from 
					aimdr.dbo.headcount_p_tb t1,
					aimdr.dbo.laps_term_p_tb t2
				where 
					t1.first_reg_abs = t2.laps_term 
					and t2.summer_flag <> 1
					and t1.laps_term >= t1.first_reg_abs
					and t1.css_student_level in ('1','2','3','4') 
					and t1.org_code = '2300'  --nursing
			)q1
			group by laps_term, cohort, res_status, as_admit
		)q2
	)q3
)q4
group by seq, laps_term, cohort, res_status, as_admit;
*/


--------------------------------------------------------------------------------------
--                              Update Nursing Raw Data Table
--------------------------------------------------------------------------------------
--Load any new data into the static nursing table 
insert into aimdev.dbo.enrl_proj_ug_nursing_raw_data
select seq, laps_term, cohort, avg(headcount) as headcount, res_status, as_admit
from
(
	select 
		case when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=1 then 25 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=2 then 26 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=0 then 27
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=1 then 19 
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=2 then 20
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=0 then 21
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=1 then 19 
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=2 then 20
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=0 then 21
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=1 then 19 
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=2 then 20
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=0 then 21
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=1 then 10 
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=2 then 11
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=0 then 12
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=1 then 10 
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=2 then 11
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=0 then 12
			 else seq end as seq, 
		laps_term, 
		case when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=1 then cohort+seq-25 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=2 then cohort+seq-26 
			 when res_status = 'R' and as_admit = 'F' and seq>27 and seq%3=0 then cohort+seq-27
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'N' and as_admit = 'F' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'I' and as_admit = 'F' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=1 then cohort+seq-19 
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=2 then cohort+seq-20
			 when res_status = 'R' and as_admit = 'A' and seq>21 and seq%3=0 then cohort+seq-21
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=1 then cohort+seq-10 
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=2 then cohort+seq-11
			 when res_status = 'N' and as_admit = 'A' and seq>12 and seq%3=0 then cohort+seq-12
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=1 then cohort+seq-10 
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=2 then cohort+seq-11
			 when res_status = 'I' and as_admit = 'A' and seq>12 and seq%3=0 then cohort+seq-12
			 else cohort end as cohort,
		headcount,
		res_status,
		as_admit
	from
	(
		select 
			seq, laps_term, cohort, 
			case when res_status = 'R' and as_admit = 'F' and seq >= 25 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'N' and as_admit = 'F' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'I' and as_admit = 'F' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'R' and as_admit = 'A' and seq >= 19 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'N' and as_admit = 'A' and seq >= 10 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 when res_status = 'I' and as_admit = 'A' and seq >= 10 then sum(headcount*cap_flag) over(partition by laps_term, res_status, as_admit) 
				 else headcount end as headcount, 
			res_status, as_admit
		from
		(
			select 
				laps_term, cohort, res_status, as_admit,
				laps_term-cohort+1 as seq, 
				count(distinct uid_no) as headcount, 
				case when res_status = 'R' and as_admit = 'F' and laps_term-cohort+1 >= 25 then 1 
					 when res_status = 'N' and as_admit = 'F' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'I' and as_admit = 'F' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'R' and as_admit = 'A' and laps_term-cohort+1 >= 19 then 1
					 when res_status = 'N' and as_admit = 'A' and laps_term-cohort+1 >= 10 then 1
					 when res_status = 'I' and as_admit = 'A' and laps_term-cohort+1 >= 10 then 1
					 else 0 end as cap_flag
			from
			(
				select
					t1.laps_term, t1.uid_no, t1.as_admit, 
					min(t1.first_reg_abs) over(partition by t2.acad_yr_trail_sum) as cohort, 
					--case when t1.css_residence_status = 'N' and t1.fee_payer_status = 'Non-Resident Fee Payer' then 'N' else 'R' end as res_status
					case when t1.reducedethnic = 'Foreign' AND t1.css_residence_status = 'N' AND t1.fee_payer_status = 'Non-Resident Fee Payer' then 'I'  
						 when t1.reducedethnic <> 'Foreign' AND t1.css_residence_status = 'N' AND t1.fee_payer_status = 'Non-Resident Fee Payer' then 'N'
						 else 'R' end as res_status
				from 
					aimdr.dbo.headcount_p_tb t1,
					aimdr.dbo.laps_term_p_tb t2
				where 
					t1.first_reg_abs = t2.laps_term 
					and t2.summer_flag <> 1
					and t1.laps_term >= t1.first_reg_abs
					and t1.css_student_level in ('1','2','3','4') 
					and t1.laps_term > (select max(laps_term) from aimdev.dbo.enrl_proj_ug_nursing_raw_data)
					and t1.org_code = '2300'  --nursing
			)q1
			group by laps_term, cohort, res_status, as_admit
		)q2
	)q3
)q4
group by seq, laps_term, cohort, res_status, as_admit;


--Refresh NS raw data table2 with two residence groups
IF object_id('aimdev.dbo.enrl_proj_ug_nursing_raw_data2') IS NOT NULL
  BEGIN
    TRUNCATE TABLE aimdev.dbo.enrl_proj_ug_nursing_raw_data2 
    DROP TABLE aimdev.dbo.enrl_proj_ug_nursing_raw_data2 
  END;

select seq, laps_term, cohort, as_admit, res_status, sum(headcount) as headcount
into aimdev.dbo.enrl_proj_ug_nursing_raw_data2
from (select seq, laps_term, cohort, as_admit, case when res_status in ('N','I') then 'N' else res_status end as res_status, headcount from aimdev.dbo.enrl_proj_ug_nursing_raw_data)q
group by seq, laps_term, cohort, as_admit, res_status;


--------------------------------------------------------------------------------------
--                            Update General Campus Raw Data Table
--------------------------------------------------------------------------------------
--substract nursing headcounts from overall headcounts to get general campus headcounts
select t1.seq, t1.laps_term, t1.cohort, isnull(cast(t1.headcount as float),0) - isnull(cast(t2.headcount as float),0) as headcount, t1.res_status, t1.as_admit
into #enrl_proj_ug_gen_camp_raw_data
from aimdev.dbo.enrl_proj_ug_raw_data2 t1 
left join aimdev.dbo.enrl_proj_ug_nursing_raw_data2 t2
on t1.seq = t2.seq and t1.laps_term = t2.laps_term and t1.cohort = t2.cohort and t1.res_status = t2.res_status and t1.as_admit = t2.as_admit


--------------------------------------------------------------------------------------
--                             Calculate FR Retention Rates
--------------------------------------------------------------------------------------
select *,
	case when seq = 1 and count(hct7) over() = 1 then NULL 
		 when seq <=4 and count(hct7) over() = 1 then hct10+hct9+hct8
		 when seq <=7 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=13 and count(hct7) over() = 1 then hct13+hct12+hct11
		 when seq <=16 and count(hct7) over() = 1 then hct14+hct13+hct12
		 when seq <=19 and count(hct7) over() = 1 then hct15+hct14+hct13
		 when seq <=22 and count(hct7) over() = 1 then hct16+hct15+hct14
		 when seq <=25 and count(hct7) over() = 1 then hct17+hct16+hct15
		 when seq <=27 and count(hct7) over() = 1 then hct18+hct17+hct16

		 when seq = 1 and count(hct7) over() = 2 then NULL 
		 when seq = 2 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 2 then hct10+hct9+hct8
		 when seq <=8 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq <=14 and count(hct7) over() = 2 then hct13+hct12+hct11
		 when seq <=17 and count(hct7) over() = 2 then hct14+hct13+hct12
		 when seq <=20 and count(hct7) over() = 2 then hct15+hct14+hct13
		 when seq <=23 and count(hct7) over() = 2 then hct16+hct15+hct14
		 when seq <=26 and count(hct7) over() = 2 then hct17+hct16+hct15
		 when seq = 27 and count(hct7) over() = 2 then hct18+hct17+hct16

		 when seq = 1 and count(hct7) over() = 3 then NULL 
		 when seq <=3 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=6 and count(hct7) over() = 3 then hct10+hct9+hct8
		 when seq <=9 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=12 and count(hct7) over() = 3 then hct12+hct11+hct10
		 when seq <=15 and count(hct7) over() = 3 then hct13+hct12+hct11
		 when seq <=18 and count(hct7) over() = 3 then hct14+hct13+hct12
		 when seq <=21 and count(hct7) over() = 3 then hct15+hct14+hct13
		 when seq <=24 and count(hct7) over() = 3 then hct16+hct15+hct14
		 when seq <=27 and count(hct7) over() = 3 then hct17+hct16+hct15
		 end as numerator,

	case when seq <=3 and count(hct7) over() = 1 then hct10+hct9+hct8 
		 when seq <=6 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=9 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=12 and count(hct7) over() = 1 then hct13+hct12+hct11
		 when seq <=15 and count(hct7) over() = 1 then hct14+hct13+hct12
		 when seq <=18 and count(hct7) over() = 1 then hct15+hct14+hct13
		 when seq <=21 and count(hct7) over() = 1 then hct16+hct15+hct14
		 when seq <=24 and count(hct7) over() = 1 then hct17+hct16+hct15
		 when seq <=26 and count(hct7) over() = 1 then hct18+hct17+hct16
		 when seq = 27 and count(hct7) over() = 1 then NULL

		 when seq = 1 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=4 and count(hct7) over() = 2 then hct10+hct9+hct8 
		 when seq <=7 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq <=13 and count(hct7) over() = 2 then hct13+hct12+hct11
		 when seq <=16 and count(hct7) over() = 2 then hct14+hct13+hct12
		 when seq <=19 and count(hct7) over() = 2 then hct15+hct14+hct13
		 when seq <=22 and count(hct7) over() = 2 then hct16+hct15+hct14
		 when seq <=25 and count(hct7) over() = 2 then hct17+hct16+hct15
		 when seq = 26 and count(hct7) over() = 2 then hct18+hct17+hct16
		 when seq = 27 and count(hct7) over() = 2 then NULL

		 when seq <=2 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 3 then hct10+hct9+hct8 
		 when seq <=8 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 3 then hct12+hct11+hct10
		 when seq <=14 and count(hct7) over() = 3 then hct13+hct12+hct11
		 when seq <=17 and count(hct7) over() = 3 then hct14+hct13+hct12
		 when seq <=20 and count(hct7) over() = 3 then hct15+hct14+hct13
		 when seq <=23 and count(hct7) over() = 3 then hct16+hct15+hct14
		 when seq <=26 and count(hct7) over() = 3 then hct17+hct16+hct15
		 when seq = 27 and count(hct7) over() = 3 then NULL
		 end as denominator
into #FR_retn_rate0
from
(
	select
		seq, prev_seq, 
		--add a few extra cohorts to ensure benchmark cohort 10-11 is included
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(seq as int) - 1 as prev_seq,
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_raw_data2	
		where 
			as_admit = 'F' and res_status = 'R'

	)q1
	group by seq, prev_seq
)q2;

select t1.seq, case when t2.denominator = 0 then 0 else 1.0*t1.numerator / t2.denominator end as retn_rate
into #FR_retn_rate
from #FR_retn_rate0 t1
left join #FR_retn_rate0 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Calculate FN Retention Rates
--------------------------------------------------------------------------------------
select *,
	case when seq = 1 and count(hct7) over() = 1 then NULL 
		 when seq <=4 and count(hct7) over() = 1 then hct10+hct9+hct8
		 when seq <=7 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=13 and count(hct7) over() = 1 then hct13+hct12+hct11
		 when seq <=16 and count(hct7) over() = 1 then hct14+hct13+hct12
		 when seq <=19 and count(hct7) over() = 1 then hct15+hct14+hct13
		 when seq <=21 and count(hct7) over() = 1 then hct16+hct15+hct14

		 when seq = 1 and count(hct7) over() = 2 then NULL 
		 when seq = 2 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 2 then hct10+hct9+hct8
		 when seq <=8 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq <=14 and count(hct7) over() = 2 then hct13+hct12+hct11
		 when seq <=17 and count(hct7) over() = 2 then hct14+hct13+hct12
		 when seq <=20 and count(hct7) over() = 2 then hct15+hct14+hct13
		 when seq = 21 and count(hct7) over() = 2 then hct16+hct15+hct14

		 when seq = 1 and count(hct7) over() = 3 then NULL 
		 when seq <=3 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=6 and count(hct7) over() = 3 then hct10+hct9+hct8
		 when seq <=9 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=12 and count(hct7) over() = 3 then hct12+hct11+hct10
		 when seq <=15 and count(hct7) over() = 3 then hct13+hct12+hct11
		 when seq <=18 and count(hct7) over() = 3 then hct14+hct13+hct12
		 when seq <=21 and count(hct7) over() = 3 then hct15+hct14+hct13
		 end as numerator,

	case when seq <=3 and count(hct7) over() = 1 then hct10+hct9+hct8 
		 when seq <=6 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=9 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=12 and count(hct7) over() = 1 then hct13+hct12+hct11
		 when seq <=15 and count(hct7) over() = 1 then hct14+hct13+hct12
		 when seq <=18 and count(hct7) over() = 1 then hct15+hct14+hct13
		 when seq <=20 and count(hct7) over() = 1 then hct16+hct15+hct14
		 when seq = 21 and count(hct7) over() = 1 then NULL

		 when seq = 1 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=4 and count(hct7) over() = 2 then hct10+hct9+hct8 
		 when seq <=7 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq <=13 and count(hct7) over() = 2 then hct13+hct12+hct11
		 when seq <=16 and count(hct7) over() = 2 then hct14+hct13+hct12
		 when seq <=19 and count(hct7) over() = 2 then hct15+hct14+hct13
		 when seq = 20 and count(hct7) over() = 2 then hct16+hct15+hct14
		 when seq = 21 and count(hct7) over() = 2 then NULL

		 when seq <=2 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 3 then hct10+hct9+hct8 
		 when seq <=8 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 3 then hct12+hct11+hct10
		 when seq <=14 and count(hct7) over() = 3 then hct13+hct12+hct11
		 when seq <=17 and count(hct7) over() = 3 then hct14+hct13+hct12
		 when seq <=20 and count(hct7) over() = 3 then hct15+hct14+hct13
		 when seq = 21 and count(hct7) over() = 3 then NULL
		 end as denominator
into #FN_retn_rate0
from
(
	select
		seq, prev_seq, 
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(seq as int) - 1 as prev_seq,
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_raw_data2	
		where 
			as_admit = 'F' and res_status = 'N'

	)q1
	group by seq, prev_seq
)q2;

select t1.seq, case when t2.denominator = 0 then 0 else 1.0*t1.numerator / t2.denominator end as retn_rate
into #FN_retn_rate
from #FN_retn_rate0 t1
left join #FN_retn_rate0 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Calculate AR Retention Rates
--------------------------------------------------------------------------------------
select *,
	case when seq = 1 and count(hct7) over() = 1 then NULL 
		 when seq <=4 and count(hct7) over() = 1 then hct10+hct9+hct8
		 when seq <=7 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=13 and count(hct7) over() = 1 then hct13+hct12+hct11
		 when seq <=16 and count(hct7) over() = 1 then hct14+hct13+hct12
		 when seq <=19 and count(hct7) over() = 1 then hct15+hct14+hct13
		 when seq <=21 and count(hct7) over() = 1 then hct16+hct15+hct14

		 when seq = 1 and count(hct7) over() = 2 then NULL 
		 when seq = 2 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 2 then hct10+hct9+hct8
		 when seq <=8 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq <=14 and count(hct7) over() = 2 then hct13+hct12+hct11
		 when seq <=17 and count(hct7) over() = 2 then hct14+hct13+hct12
		 when seq <=20 and count(hct7) over() = 2 then hct15+hct14+hct13
		 when seq = 21 and count(hct7) over() = 2 then hct16+hct15+hct14

		 when seq = 1 and count(hct7) over() = 3 then NULL 
		 when seq <=3 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=6 and count(hct7) over() = 3 then hct10+hct9+hct8
		 when seq <=9 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=12 and count(hct7) over() = 3 then hct12+hct11+hct10
		 when seq <=15 and count(hct7) over() = 3 then hct13+hct12+hct11
		 when seq <=18 and count(hct7) over() = 3 then hct14+hct13+hct12
		 when seq <=21 and count(hct7) over() = 3 then hct15+hct14+hct13
		 end as numerator,

	case when seq <=3 and count(hct7) over() = 1 then hct10+hct9+hct8 
		 when seq <=6 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=9 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=12 and count(hct7) over() = 1 then hct13+hct12+hct11
		 when seq <=15 and count(hct7) over() = 1 then hct14+hct13+hct12
		 when seq <=18 and count(hct7) over() = 1 then hct15+hct14+hct13
		 when seq <=20 and count(hct7) over() = 1 then hct16+hct15+hct14
		 when seq = 21 and count(hct7) over() = 1 then NULL

		 when seq = 1 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=4 and count(hct7) over() = 2 then hct10+hct9+hct8 
		 when seq <=7 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq <=13 and count(hct7) over() = 2 then hct13+hct12+hct11
		 when seq <=16 and count(hct7) over() = 2 then hct14+hct13+hct12
		 when seq <=19 and count(hct7) over() = 2 then hct15+hct14+hct13
		 when seq = 20 and count(hct7) over() = 2 then hct16+hct15+hct14
		 when seq = 21 and count(hct7) over() = 2 then NULL

		 when seq <=2 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 3 then hct10+hct9+hct8 
		 when seq <=8 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 3 then hct12+hct11+hct10
		 when seq <=14 and count(hct7) over() = 3 then hct13+hct12+hct11
		 when seq <=17 and count(hct7) over() = 3 then hct14+hct13+hct12
		 when seq <=20 and count(hct7) over() = 3 then hct15+hct14+hct13
		 when seq = 21 and count(hct7) over() = 3 then NULL
		 end as denominator
into #AR_retn_rate0
from
(
	select
		seq, prev_seq, 
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(seq as int) - 1 as prev_seq,
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_raw_data2	
		where 
			as_admit = 'A' and res_status = 'R'

	)q1
	group by seq, prev_seq
)q2;

select t1.seq, case when t2.denominator = 0 then 0 else 1.0*t1.numerator / t2.denominator end as retn_rate
into #AR_retn_rate
from #AR_retn_rate0 t1
left join #AR_retn_rate0 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Calculate AN Retention Rates
--------------------------------------------------------------------------------------
select *,
	case when seq = 1 and count(hct7) over() = 1 then NULL 
		 when seq <=4 and count(hct7) over() = 1 then hct10+hct9+hct8
		 when seq <=7 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=12 and count(hct7) over() = 1 then hct13+hct12+hct11

		 when seq = 1 and count(hct7) over() = 2 then NULL 
		 when seq = 2 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 2 then hct10+hct9+hct8
		 when seq <=8 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq = 12 and count(hct7) over() = 2 then hct13+hct12+hct11

		 when seq = 1 and count(hct7) over() = 3 then NULL 
		 when seq <=3 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=6 and count(hct7) over() = 3 then hct10+hct9+hct8
		 when seq <=9 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=12 and count(hct7) over() = 3 then hct12+hct11+hct10
		 end as numerator,

	case when seq <=3 and count(hct7) over() = 1 then hct10+hct9+hct8 
		 when seq <=6 and count(hct7) over() = 1 then hct11+hct10+hct9
		 when seq <=9 and count(hct7) over() = 1 then hct12+hct11+hct10
		 when seq <=11 and count(hct7) over() = 1 then hct13+hct12+hct11
		 when seq = 12 and count(hct7) over() = 1 then NULL

		 when seq = 1 and count(hct7) over() = 2 then hct9+hct8+hct7
		 when seq <=4 and count(hct7) over() = 2 then hct10+hct9+hct8 
		 when seq <=7 and count(hct7) over() = 2 then hct11+hct10+hct9
		 when seq <=10 and count(hct7) over() = 2 then hct12+hct11+hct10
		 when seq = 11 and count(hct7) over() = 2 then hct13+hct12+hct11
		 when seq = 12 and count(hct7) over() = 2 then NULL

		 when seq <=2 and count(hct7) over() = 3 then hct9+hct8+hct7
		 when seq <=5 and count(hct7) over() = 3 then hct10+hct9+hct8 
		 when seq <=8 and count(hct7) over() = 3 then hct11+hct10+hct9
		 when seq <=11 and count(hct7) over() = 3 then hct12+hct11+hct10
		 when seq = 12 and count(hct7) over() = 3 then NULL
		 end as denominator
into #AN_retn_rate0
from
(
	select
		seq, prev_seq, 
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(seq as int) - 1 as prev_seq,
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_raw_data2	
		where 
			as_admit = 'A' and res_status = 'N'

	)q1
	group by seq, prev_seq
)q2;

select t1.seq, case when t2.denominator = 0 then 0 else 1.0*t1.numerator / t2.denominator end as retn_rate
into #AN_retn_rate
from #AN_retn_rate0 t1
left join #AN_retn_rate0 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                  Make empty sequence tables for later left joins
--------------------------------------------------------------------------------------
select seq, prev_seq, 
	case when hct1 is not null then 0 else hct1 end as hct1,
	case when hct2 is not null then 0 else hct2 end as hct2,
	case when hct3 is not null then 0 else hct3 end as hct3,
	case when hct4 is not null then 0 else hct4 end as hct4,
	case when hct5 is not null then 0 else hct5 end as hct5,
	case when hct6 is not null then 0 else hct6 end as hct6,
	case when hct7 is not null then 0 else hct7 end as hct7,
	case when hct8 is not null then 0 else hct8 end as hct8,
	case when hct9 is not null then 0 else hct9 end as hct9,
	case when hct10 is not null then 0 else hct10 end as hct10,
	case when hct11 is not null then 0 else hct11 end as hct11,
	case when hct12 is not null then 0 else hct12 end as hct12,
	case when hct13 is not null then 0 else hct13 end as hct13,
	case when hct14 is not null then 0 else hct14 end as hct14,
	case when hct15 is not null then 0 else hct15 end as hct15,
	case when hct16 is not null then 0 else hct16 end as hct16,
	case when hct17 is not null then 0 else hct17 end as hct17,
	case when hct18 is not null then 0 else hct18 end as hct18,
	case when hct19 is not null then 0 else hct19 end as hct19,
	case when hct20 is not null then 0 else hct20 end as hct20,
	case when hct21 is not null then 0 else hct21 end as hct21,
	case when hct22 is not null then 0 else hct22 end as hct22,
	case when hct23 is not null then 0 else hct23 end as hct23,
	case when hct24 is not null then 0 else hct24 end as hct24,
	case when hct25 is not null then 0 else hct25 end as hct25,
	case when hct26 is not null then 0 else hct26 end as hct26,
	case when hct27 is not null then 0 else hct27 end as hct27,
	case when hct28 is not null then 0 else hct28 end as hct28,
	case when hct29 is not null then 0 else hct29 end as hct29,
	case when hct30 is not null then 0 else hct30 end as hct30,
	case when hct31 is not null then 0 else hct31 end as hct31,
	case when hct32 is not null then 0 else hct32 end as hct32
into #seq27_table
from #FR_retn_rate0;

select * into #seq21_table from #seq27_table where seq <= 21;

select * into #seq12_table from #seq27_table where seq <= 12;


--------------------------------------------------------------------------------------
--                             Freshman Residents
--------------------------------------------------------------------------------------
--apply retention rates on general campus data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #FR_temp_table1
from #seq27_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			#enrl_proj_ug_gen_camp_raw_data	
		where 
			as_admit = 'F' and res_status = 'R'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #FR_temp_table2
from #FR_temp_table1 t1
left join #FR_temp_table1 t2
on t1.prev_seq = t2.seq
left join #FR_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table3
from #FR_temp_table2 t1
left join #FR_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table4
from #FR_temp_table3 t1
left join #FR_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table5
from #FR_temp_table4 t1
left join #FR_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table6
from #FR_temp_table5 t1
left join #FR_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table7
from #FR_temp_table6 t1
left join #FR_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table8
from #FR_temp_table7 t1
left join #FR_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table9
from #FR_temp_table8 t1
left join #FR_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table10
from #FR_temp_table9 t1
left join #FR_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table11
from #FR_temp_table10 t1
left join #FR_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table12
from #FR_temp_table11 t1
left join #FR_temp_table11 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table13
from #FR_temp_table12 t1
left join #FR_temp_table12 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table14
from #FR_temp_table13 t1
left join #FR_temp_table13 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table15
from #FR_temp_table14 t1
left join #FR_temp_table14 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table16
from #FR_temp_table15 t1
left join #FR_temp_table15 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table17
from #FR_temp_table16 t1
left join #FR_temp_table16 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table18
from #FR_temp_table17 t1
left join #FR_temp_table17 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table19
from #FR_temp_table18 t1
left join #FR_temp_table18 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table20
from #FR_temp_table19 t1
left join #FR_temp_table19 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table21
from #FR_temp_table20 t1
left join #FR_temp_table20 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table22
from #FR_temp_table21 t1
left join #FR_temp_table21 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table23
from #FR_temp_table22 t1
left join #FR_temp_table22 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table24
from #FR_temp_table23 t1
left join #FR_temp_table23 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table25
from #FR_temp_table24 t1
left join #FR_temp_table24 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table26
from #FR_temp_table25 t1
left join #FR_temp_table25 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FR_temp_table27
from #FR_temp_table26 t1
left join #FR_temp_table26 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Freshman Non-Residents
--------------------------------------------------------------------------------------
--apply retention rates on general campus data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #FN_temp_table1
from #seq21_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			#enrl_proj_ug_gen_camp_raw_data	
		where 
			as_admit = 'F' and res_status = 'N'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #FN_temp_table2
from #FN_temp_table1 t1
left join #FN_temp_table1 t2
on t1.prev_seq = t2.seq
left join #FN_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table3
from #FN_temp_table2 t1
left join #FN_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table4
from #FN_temp_table3 t1
left join #FN_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table5
from #FN_temp_table4 t1
left join #FN_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table6
from #FN_temp_table5 t1
left join #FN_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table7
from #FN_temp_table6 t1
left join #FN_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table8
from #FN_temp_table7 t1
left join #FN_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table9
from #FN_temp_table8 t1
left join #FN_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table10
from #FN_temp_table9 t1
left join #FN_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table11
from #FN_temp_table10 t1
left join #FN_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table12
from #FN_temp_table11 t1
left join #FN_temp_table11 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table13
from #FN_temp_table12 t1
left join #FN_temp_table12 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table14
from #FN_temp_table13 t1
left join #FN_temp_table13 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table15
from #FN_temp_table14 t1
left join #FN_temp_table14 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table16
from #FN_temp_table15 t1
left join #FN_temp_table15 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table17
from #FN_temp_table16 t1
left join #FN_temp_table16 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table18
from #FN_temp_table17 t1
left join #FN_temp_table17 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table19
from #FN_temp_table18 t1
left join #FN_temp_table18 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table20
from #FN_temp_table19 t1
left join #FN_temp_table19 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #FN_temp_table21
from #FN_temp_table20 t1
left join #FN_temp_table20 t2
on t1.prev_seq = t2.seq;



--------------------------------------------------------------------------------------
--                             Transfer Residents
--------------------------------------------------------------------------------------
--apply retention rates on general campus data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #AR_temp_table1
from #seq21_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			#enrl_proj_ug_gen_camp_raw_data	
		where 
			as_admit = 'A' and res_status = 'R'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #AR_temp_table2
from #AR_temp_table1 t1
left join #AR_temp_table1 t2
on t1.prev_seq = t2.seq
left join #AR_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table3
from #AR_temp_table2 t1
left join #AR_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table4
from #AR_temp_table3 t1
left join #AR_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table5
from #AR_temp_table4 t1
left join #AR_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table6
from #AR_temp_table5 t1
left join #AR_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table7
from #AR_temp_table6 t1
left join #AR_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table8
from #AR_temp_table7 t1
left join #AR_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table9
from #AR_temp_table8 t1
left join #AR_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table10
from #AR_temp_table9 t1
left join #AR_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table11
from #AR_temp_table10 t1
left join #AR_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table12
from #AR_temp_table11 t1
left join #AR_temp_table11 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table13
from #AR_temp_table12 t1
left join #AR_temp_table12 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table14
from #AR_temp_table13 t1
left join #AR_temp_table13 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table15
from #AR_temp_table14 t1
left join #AR_temp_table14 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table16
from #AR_temp_table15 t1
left join #AR_temp_table15 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table17
from #AR_temp_table16 t1
left join #AR_temp_table16 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table18
from #AR_temp_table17 t1
left join #AR_temp_table17 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table19
from #AR_temp_table18 t1
left join #AR_temp_table18 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table20
from #AR_temp_table19 t1
left join #AR_temp_table19 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AR_temp_table21
from #AR_temp_table20 t1
left join #AR_temp_table20 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Transfer Non-Residents
--------------------------------------------------------------------------------------
--apply retention rates on general campus data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #AN_temp_table1
from #seq12_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			#enrl_proj_ug_gen_camp_raw_data	
		where 
			as_admit = 'A' and res_status = 'N'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #AN_temp_table2
from #AN_temp_table1 t1
left join #AN_temp_table1 t2
on t1.prev_seq = t2.seq
left join #AN_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table3
from #AN_temp_table2 t1
left join #AN_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table4
from #AN_temp_table3 t1
left join #AN_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table5
from #AN_temp_table4 t1
left join #AN_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table6
from #AN_temp_table5 t1
left join #AN_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table7
from #AN_temp_table6 t1
left join #AN_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table8
from #AN_temp_table7 t1
left join #AN_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table9
from #AN_temp_table8 t1
left join #AN_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table10
from #AN_temp_table9 t1
left join #AN_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table11
from #AN_temp_table10 t1
left join #AN_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #AN_temp_table12
from #AN_temp_table11 t1
left join #AN_temp_table11 t2
on t1.prev_seq = t2.seq;



--------------------------------------------------------------------------------------
--                             Nursing Freshman Residents
--------------------------------------------------------------------------------------
--apply retention rates on nursing data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #nFR_temp_table1
from #seq27_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_nursing_raw_data2	
		where 
			as_admit = 'F' and res_status = 'R'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table2
from #nFR_temp_table1 t1
left join #nFR_temp_table1 t2
on t1.prev_seq = t2.seq
left join #FR_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table3
from #nFR_temp_table2 t1
left join #nFR_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table4
from #nFR_temp_table3 t1
left join #nFR_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table5
from #nFR_temp_table4 t1
left join #nFR_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table6
from #nFR_temp_table5 t1
left join #nFR_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table7
from #nFR_temp_table6 t1
left join #nFR_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table8
from #nFR_temp_table7 t1
left join #nFR_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table9
from #nFR_temp_table8 t1
left join #nFR_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table10
from #nFR_temp_table9 t1
left join #nFR_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table11
from #nFR_temp_table10 t1
left join #nFR_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table12
from #nFR_temp_table11 t1
left join #nFR_temp_table11 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table13
from #nFR_temp_table12 t1
left join #nFR_temp_table12 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table14
from #nFR_temp_table13 t1
left join #nFR_temp_table13 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table15
from #nFR_temp_table14 t1
left join #nFR_temp_table14 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table16
from #nFR_temp_table15 t1
left join #nFR_temp_table15 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table17
from #nFR_temp_table16 t1
left join #nFR_temp_table16 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table18
from #nFR_temp_table17 t1
left join #nFR_temp_table17 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table19
from #nFR_temp_table18 t1
left join #nFR_temp_table18 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table20
from #nFR_temp_table19 t1
left join #nFR_temp_table19 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table21
from #nFR_temp_table20 t1
left join #nFR_temp_table20 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table22
from #nFR_temp_table21 t1
left join #nFR_temp_table21 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table23
from #nFR_temp_table22 t1
left join #nFR_temp_table22 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table24
from #nFR_temp_table23 t1
left join #nFR_temp_table23 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table25
from #nFR_temp_table24 t1
left join #nFR_temp_table24 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table26
from #nFR_temp_table25 t1
left join #nFR_temp_table25 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFR_temp_table27
from #nFR_temp_table26 t1
left join #nFR_temp_table26 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Nursing Freshman Non-Residents
--------------------------------------------------------------------------------------
--apply retention rates on nursing data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #nFN_temp_table1
from #seq21_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_nursing_raw_data2	
		where 
			as_admit = 'F' and res_status = 'N'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table2
from #nFN_temp_table1 t1
left join #nFN_temp_table1 t2
on t1.prev_seq = t2.seq
left join #FN_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table3
from #nFN_temp_table2 t1
left join #nFN_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table4
from #nFN_temp_table3 t1
left join #nFN_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table5
from #nFN_temp_table4 t1
left join #nFN_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table6
from #nFN_temp_table5 t1
left join #nFN_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table7
from #nFN_temp_table6 t1
left join #nFN_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table8
from #nFN_temp_table7 t1
left join #nFN_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table9
from #nFN_temp_table8 t1
left join #nFN_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table10
from #nFN_temp_table9 t1
left join #nFN_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table11
from #nFN_temp_table10 t1
left join #nFN_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table12
from #nFN_temp_table11 t1
left join #nFN_temp_table11 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table13
from #nFN_temp_table12 t1
left join #nFN_temp_table12 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table14
from #nFN_temp_table13 t1
left join #nFN_temp_table13 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table15
from #nFN_temp_table14 t1
left join #nFN_temp_table14 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table16
from #nFN_temp_table15 t1
left join #nFN_temp_table15 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table17
from #nFN_temp_table16 t1
left join #nFN_temp_table16 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table18
from #nFN_temp_table17 t1
left join #nFN_temp_table17 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table19
from #nFN_temp_table18 t1
left join #nFN_temp_table18 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table20
from #nFN_temp_table19 t1
left join #nFN_temp_table19 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nFN_temp_table21
from #nFN_temp_table20 t1
left join #nFN_temp_table20 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Nursing Transfer Residents
--------------------------------------------------------------------------------------
--apply retention rates on nursing data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #nAR_temp_table1
from #seq21_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_nursing_raw_data2	
		where 
			as_admit = 'A' and res_status = 'R'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table2
from #nAR_temp_table1 t1
left join #nAR_temp_table1 t2
on t1.prev_seq = t2.seq
left join #AR_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table3
from #nAR_temp_table2 t1
left join #nAR_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table4
from #nAR_temp_table3 t1
left join #nAR_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table5
from #nAR_temp_table4 t1
left join #nAR_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table6
from #nAR_temp_table5 t1
left join #nAR_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table7
from #nAR_temp_table6 t1
left join #nAR_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table8
from #nAR_temp_table7 t1
left join #nAR_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table9
from #nAR_temp_table8 t1
left join #nAR_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table10
from #nAR_temp_table9 t1
left join #nAR_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table11
from #nAR_temp_table10 t1
left join #nAR_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table12
from #nAR_temp_table11 t1
left join #nAR_temp_table11 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table13
from #nAR_temp_table12 t1
left join #nAR_temp_table12 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table14
from #nAR_temp_table13 t1
left join #nAR_temp_table13 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table15
from #nAR_temp_table14 t1
left join #nAR_temp_table14 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table16
from #nAR_temp_table15 t1
left join #nAR_temp_table15 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table17
from #nAR_temp_table16 t1
left join #nAR_temp_table16 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table18
from #nAR_temp_table17 t1
left join #nAR_temp_table17 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table19
from #nAR_temp_table18 t1
left join #nAR_temp_table18 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table20
from #nAR_temp_table19 t1
left join #nAR_temp_table19 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAR_temp_table21
from #nAR_temp_table20 t1
left join #nAR_temp_table20 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                             Nursing Transfer Non-Residents
--------------------------------------------------------------------------------------
--apply retention rates on nursing data
select 
	t1.seq, t1.prev_seq, 
	case when t2.hct32 is null then t1.hct32 else t2.hct32 end as hct32,
	case when t2.hct31 is null then t1.hct31 else t2.hct31 end as hct31,
	case when t2.hct30 is null then t1.hct30 else t2.hct30 end as hct30,
	case when t2.hct29 is null then t1.hct29 else t2.hct29 end as hct29,
	case when t2.hct28 is null then t1.hct28 else t2.hct28 end as hct28,
	case when t2.hct27 is null then t1.hct27 else t2.hct27 end as hct27,
	case when t2.hct26 is null then t1.hct26 else t2.hct26 end as hct26,
	case when t2.hct25 is null then t1.hct25 else t2.hct25 end as hct25,
	case when t2.hct24 is null then t1.hct24 else t2.hct24 end as hct24,
	case when t2.hct23 is null then t1.hct23 else t2.hct23 end as hct23,
	case when t2.hct22 is null then t1.hct22 else t2.hct22 end as hct22,
	case when t2.hct21 is null then t1.hct21 else t2.hct21 end as hct21,
	case when t2.hct20 is null then t1.hct20 else t2.hct20 end as hct20,
	case when t2.hct19 is null then t1.hct19 else t2.hct19 end as hct19,
	case when t2.hct18 is null then t1.hct18 else t2.hct18 end as hct18,
	case when t2.hct17 is null then t1.hct17 else t2.hct17 end as hct17,
	case when t2.hct16 is null then t1.hct16 else t2.hct16 end as hct16,
	case when t2.hct15 is null then t1.hct15 else t2.hct15 end as hct15,
	case when t2.hct14 is null then t1.hct14 else t2.hct14 end as hct14,
	case when t2.hct13 is null then t1.hct13 else t2.hct13 end as hct13,
	case when t2.hct12 is null then t1.hct12 else t2.hct12 end as hct12,
	case when t2.hct11 is null then t1.hct11 else t2.hct11 end as hct11,
	case when t2.hct10 is null then t1.hct10 else t2.hct10 end as hct10,
	case when t2.hct9 is null then t1.hct9 else t2.hct9 end as hct9,
	case when t2.hct8 is null then t1.hct8 else t2.hct8 end as hct8,
	case when t2.hct7 is null then t1.hct7 else t2.hct7 end as hct7,
	case when t2.hct6 is null then t1.hct6 else t2.hct6 end as hct6,
	case when t2.hct5 is null then t1.hct5 else t2.hct5 end as hct5,
	case when t2.hct4 is null then t1.hct4 else t2.hct4 end as hct4,
	case when t2.hct3 is null then t1.hct3 else t2.hct3 end as hct3,
	case when t2.hct2 is null then t1.hct2 else t2.hct2 end as hct2,
	case when t2.hct1 is null then t1.hct1 else t2.hct1 end as hct1
into #nAN_temp_table1
from #seq12_table t1
left join
(
	select
		seq,  
		max(case when cohort = 25 then headcount end) as hct32,
		max(case when cohort = 24 then headcount end) as hct31,
		max(case when cohort = 23 then headcount end) as hct30,
		max(case when cohort = 22 then headcount end) as hct29,	
		max(case when cohort = 21 then headcount end) as hct28,
		max(case when cohort = 20 then headcount end) as hct27,
		max(case when cohort = 19 then headcount end) as hct26,
		max(case when cohort = 18 then headcount end) as hct25,
		max(case when cohort = 17 then headcount end) as hct24,
		max(case when cohort = 16 then headcount end) as hct23,
		max(case when cohort = 15 then headcount end) as hct22,
		max(case when cohort = 14 then headcount end) as hct21,
		max(case when cohort = 13 then headcount end) as hct20,
		max(case when cohort = 12 then headcount end) as hct19,
		max(case when cohort = 11 then headcount end) as hct18,
		max(case when cohort = 10 then headcount end) as hct17,
		max(case when cohort = 9 then headcount end) as hct16,
		max(case when cohort = 8 then headcount end) as hct15,
		max(case when cohort = 7 then headcount end) as hct14,
		max(case when cohort = 6 then headcount end) as hct13,
		max(case when cohort = 5 then headcount end) as hct12,
		max(case when cohort = 4 then headcount end) as hct11,
		max(case when cohort = 3 then headcount end) as hct10,
		max(case when cohort = 2 then headcount end) as hct9,
		max(case when cohort = 1 then headcount end) as hct8,
		max(case when cohort = 0 then headcount end) as hct7,
		case when seq = 1 then 1 end as hct6,
		case when seq = 1 then 1 end as hct5,
		case when seq = 1 then 1 end as hct4,
		case when seq = 1 then 1 end as hct3,
		case when seq = 1 then 1 end as hct2,
		case when seq = 1 then 1 end as hct1
	from
	(
		select 
			cast(laps_term as int) as laps_term, 
			cast(seq as int) as seq, 
			cast(headcount as float) as headcount,
			cast((@max_cohort - cast(cohort as float))/3 as int) as cohort
		from 
			aimdev.dbo.enrl_proj_ug_nursing_raw_data2	
		where 
			as_admit = 'A' and res_status = 'N'
	)foo
	group by seq
) t2
on t1.seq = t2.seq;

select t1.seq, t1.prev_seq, t3.retn_rate,
	case when t1.hct32 is null then t2.hct32*t3.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t3.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t3.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t3.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t3.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t3.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t3.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t3.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t3.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t3.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t3.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t3.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t3.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t3.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t3.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t3.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t3.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t3.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t3.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t3.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t3.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t3.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t3.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t3.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t3.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t3.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t3.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t3.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t3.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t3.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t3.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t3.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table2
from #nAN_temp_table1 t1
left join #nAN_temp_table1 t2
on t1.prev_seq = t2.seq
left join #AN_retn_rate t3
on t1.seq = t3.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table3
from #nAN_temp_table2 t1
left join #nAN_temp_table2 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table4
from #nAN_temp_table3 t1
left join #nAN_temp_table3 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table5
from #nAN_temp_table4 t1
left join #nAN_temp_table4 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table6
from #nAN_temp_table5 t1
left join #nAN_temp_table5 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table7
from #nAN_temp_table6 t1
left join #nAN_temp_table6 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table8
from #nAN_temp_table7 t1
left join #nAN_temp_table7 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table9
from #nAN_temp_table8 t1
left join #nAN_temp_table8 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table10
from #nAN_temp_table9 t1
left join #nAN_temp_table9 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table11
from #nAN_temp_table10 t1
left join #nAN_temp_table10 t2
on t1.prev_seq = t2.seq;

select t1.seq, t1.prev_seq, t1.retn_rate,
	case when t1.hct32 is null then t2.hct32*t1.retn_rate else t1.hct32 end as hct32, 
	case when t1.hct31 is null then t2.hct31*t1.retn_rate else t1.hct31 end as hct31, 
	case when t1.hct30 is null then t2.hct30*t1.retn_rate else t1.hct30 end as hct30, 
	case when t1.hct29 is null then t2.hct29*t1.retn_rate else t1.hct29 end as hct29, 
	case when t1.hct28 is null then t2.hct28*t1.retn_rate else t1.hct28 end as hct28, 
	case when t1.hct27 is null then t2.hct27*t1.retn_rate else t1.hct27 end as hct27, 
	case when t1.hct26 is null then t2.hct26*t1.retn_rate else t1.hct26 end as hct26, 
	case when t1.hct25 is null then t2.hct25*t1.retn_rate else t1.hct25 end as hct25, 
	case when t1.hct24 is null then t2.hct24*t1.retn_rate else t1.hct24 end as hct24, 
	case when t1.hct23 is null then t2.hct23*t1.retn_rate else t1.hct23 end as hct23, 
	case when t1.hct22 is null then t2.hct22*t1.retn_rate else t1.hct22 end as hct22, 
	case when t1.hct21 is null then t2.hct21*t1.retn_rate else t1.hct21 end as hct21, 
	case when t1.hct20 is null then t2.hct20*t1.retn_rate else t1.hct20 end as hct20, 
	case when t1.hct19 is null then t2.hct19*t1.retn_rate else t1.hct19 end as hct19, 
	case when t1.hct18 is null then t2.hct18*t1.retn_rate else t1.hct18 end as hct18, 
	case when t1.hct17 is null then t2.hct17*t1.retn_rate else t1.hct17 end as hct17, 
	case when t1.hct16 is null then t2.hct16*t1.retn_rate else t1.hct16 end as hct16, 
	case when t1.hct15 is null then t2.hct15*t1.retn_rate else t1.hct15 end as hct15, 
	case when t1.hct14 is null then t2.hct14*t1.retn_rate else t1.hct14 end as hct14, 
	case when t1.hct13 is null then t2.hct13*t1.retn_rate else t1.hct13 end as hct13, 
	case when t1.hct12 is null then t2.hct12*t1.retn_rate else t1.hct12 end as hct12, 
	case when t1.hct11 is null then t2.hct11*t1.retn_rate else t1.hct11 end as hct11, 
	case when t1.hct10 is null then t2.hct10*t1.retn_rate else t1.hct10 end as hct10, 
	case when t1.hct9 is null then t2.hct9*t1.retn_rate else t1.hct9 end as hct9, 
	case when t1.hct8 is null then t2.hct8*t1.retn_rate else t1.hct8 end as hct8, 
	case when t1.hct7 is null then t2.hct7*t1.retn_rate else t1.hct7 end as hct7, 
	case when t1.hct6 is null then t2.hct6*t1.retn_rate else t1.hct6 end as hct6, 
	case when t1.hct5 is null then t2.hct5*t1.retn_rate else t1.hct5 end as hct5, 
	case when t1.hct4 is null then t2.hct4*t1.retn_rate else t1.hct4 end as hct4, 
	case when t1.hct3 is null then t2.hct3*t1.retn_rate else t1.hct3 end as hct3, 
	case when t1.hct2 is null then t2.hct2*t1.retn_rate else t1.hct2 end as hct2, 
	case when t1.hct1 is null then t2.hct1*t1.retn_rate else t1.hct1 end as hct1
into #nAN_temp_table12
from #nAN_temp_table11 t1
left join #nAN_temp_table11 t2
on t1.prev_seq = t2.seq;


--------------------------------------------------------------------------------------
--                     Generate Final Table for Tableau Dashboard
--------------------------------------------------------------------------------------
-- declare @year32 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-75 = laps_term and summer_flag <> 1);
-- declare @year31 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-72 = laps_term and summer_flag <> 1);
-- declare @year30 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-69 = laps_term and summer_flag <> 1);
-- declare @year29 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-66 = laps_term and summer_flag <> 1);
-- declare @year28 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-63 = laps_term and summer_flag <> 1);
-- declare @year27 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-60 = laps_term and summer_flag <> 1);
-- declare @year26 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-57 = laps_term and summer_flag <> 1);
-- declare @year25 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-54 = laps_term and summer_flag <> 1);
-- declare @year24 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-51 = laps_term and summer_flag <> 1);
-- declare @year23 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-48 = laps_term and summer_flag <> 1);
-- declare @year22 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-45 = laps_term and summer_flag <> 1);
-- declare @year21 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-42 = laps_term and summer_flag <> 1);
-- declare @year20 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-39 = laps_term and summer_flag <> 1);
-- declare @year19 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-36 = laps_term and summer_flag <> 1);
declare @year18 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-33 = laps_term and summer_flag <> 1);
declare @year17 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-30 = laps_term and summer_flag <> 1);
declare @year16 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-27 = laps_term and summer_flag <> 1);
declare @year15 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-24 = laps_term and summer_flag <> 1);
declare @year14 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-21 = laps_term and summer_flag <> 1);
declare @year13 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-18 = laps_term and summer_flag <> 1);
declare @year12 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-15 = laps_term and summer_flag <> 1);
declare @year11 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-12 = laps_term and summer_flag <> 1);
declare @year10 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-9 = laps_term and summer_flag <> 1);
declare @year9 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-6 = laps_term and summer_flag <> 1);
declare @year8 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort-3 = laps_term and summer_flag <> 1);
declare @year7 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort = laps_term and summer_flag <> 1);
declare @year6 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort+3 = laps_term and summer_flag <> 1);
declare @year5 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort+6 = laps_term and summer_flag <> 1);
declare @year4 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort+9 = laps_term and summer_flag <> 1);
declare @year3 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort+12 = laps_term and summer_flag <> 1);
declare @year2 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort+15 = laps_term and summer_flag <> 1);
declare @year1 varchar(15) = (select acad_yr_trail_sum from aimdr.dbo.laps_term_p_tb where @max_cohort+18 = laps_term and summer_flag <> 1);


IF OBJECT_ID('aimdev.dbo.tableau_enrollment_projection2') IS NOT NULL 
  	BEGIN
		TRUNCATE TABLE aimdev.dbo.tableau_enrollment_projection2
		drop TABLE aimdev.dbo.tableau_enrollment_projection2
	END;

SELECT * INTO aimdev.dbo.tableau_enrollment_projection2
from
(
	--------------------------------------------------------------------------------------
	--                    Freshman Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FR_temp_table27
	union all
	--------------------------------------------------------------------------------------
	--                    Freshman Non-Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#FN_temp_table21
	union all
	
	--------------------------------------------------------------------------------------
	--                             Transfer Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AR_temp_table21
	union all
	
	--------------------------------------------------------------------------------------
	--                             Transfer Non-Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'GC' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#AN_temp_table12
	union all
	
	--------------------------------------------------------------------------------------
	--                    Nursing Freshman Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFR_temp_table27
	union all
	
	--------------------------------------------------------------------------------------
	--                    Nursing Freshman Non-Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'F' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nFN_temp_table21
	union all
	
	--------------------------------------------------------------------------------------
	--                             Transfer Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'R' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAR_temp_table21
	union all
	
	--------------------------------------------------------------------------------------
	--                             Transfer Non-Residents
	--------------------------------------------------------------------------------------
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct17 when 13 then hct18 when 16 then hct19 when 19 then hct20 when 22 then hct21 when 25 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct17 when 14 then hct18 when 17 then hct19 when 20 then hct20 when 23 then hct21 when 26 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year14 as academic_year,
		substring(@year14,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct17 when 15 then hct18 when 18 then hct19 when 21 then hct20 when 24 then hct21 when 27 then hct22 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct16 when 13 then hct17 when 16 then hct18 when 19 then hct19 when 22 then hct20 when 25 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct16 when 14 then hct17 when 17 then hct18 when 20 then hct19 when 23 then hct20 when 26 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year13 as academic_year,
		substring(@year13,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct16 when 15 then hct17 when 18 then hct18 when 21 then hct19 when 24 then hct20 when 27 then hct21 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct15 when 13 then hct16 when 16 then hct17 when 19 then hct18 when 22 then hct19 when 25 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct15 when 14 then hct16 when 17 then hct17 when 20 then hct18 when 23 then hct19 when 26 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year12 as academic_year,
		substring(@year12,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct15 when 15 then hct16 when 18 then hct17 when 21 then hct18 when 24 then hct19 when 27 then hct20 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct14 when 13 then hct15 when 16 then hct16 when 19 then hct17 when 22 then hct18 when 25 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct14 when 14 then hct15 when 17 then hct16 when 20 then hct17 when 23 then hct18 when 26 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year11 as academic_year,
		substring(@year11,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct14 when 15 then hct15 when 18 then hct16 when 21 then hct17 when 24 then hct18 when 27 then hct19 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct13 when 13 then hct14 when 16 then hct15 when 19 then hct16 when 22 then hct17 when 25 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct13 when 14 then hct14 when 17 then hct15 when 20 then hct16 when 23 then hct17 when 26 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year10 as academic_year,
		substring(@year10,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct13 when 15 then hct14 when 18 then hct15 when 21 then hct16 when 24 then hct17 when 27 then hct18 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct11 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct12 when 13 then hct13 when 16 then hct14 when 19 then hct15 when 22 then hct16 when 25 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct12 when 14 then hct13 when 17 then hct14 when 20 then hct15 when 23 then hct16 when 26 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year9 as academic_year,
		substring(@year9,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct12 when 15 then hct13 when 18 then hct14 when 21 then hct15 when 24 then hct16 when 27 then hct17 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct10 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct11 when 13 then hct12 when 16 then hct13 when 19 then hct14 when 22 then hct15 when 25 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct11 when 14 then hct12 when 17 then hct13 when 20 then hct14 when 23 then hct15 when 26 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year8 as academic_year,
		substring(@year8,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct11 when 15 then hct12 when 18 then hct13 when 21 then hct14 when 24 then hct15 when 27 then hct16 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 1 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 2 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		sum(case seq when 3 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all	
    select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
    union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct9 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct10 when 13 then hct11 when 16 then hct12 when 19 then hct13 when 22 then hct14 when 25 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct10 when 14 then hct11 when 17 then hct12 when 20 then hct13 when 23 then hct14 when 26 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year7 as academic_year,
		substring(@year7,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct10 when 15 then hct11 when 18 then hct12 when 21 then hct13 when 24 then hct14 when 27 then hct15 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 1 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 2 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		sum(case seq when 3 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 4 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 5 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		sum(case seq when 6 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all	
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct8 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct9 when 13 then hct10 when 16 then hct11 when 19 then hct12 when 22 then hct13 when 25 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct9 when 14 then hct10 when 17 then hct11 when 20 then hct12 when 23 then hct13 when 26 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year6 as academic_year,
		substring(@year6,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct9 when 15 then hct10 when 18 then hct11 when 21 then hct12 when 24 then hct13 when 27 then hct14 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 1 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 2 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 3 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 4 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 5 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		sum(case seq when 6 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 7 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 8 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		sum(case seq when 9 then hct7 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct8 when 13 then hct9 when 16 then hct10 when 19 then hct11 when 22 then hct12 when 25 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct8 when 14 then hct9 when 17 then hct10 when 20 then hct11 when 23 then hct12 when 26 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	select 
		@year5 as academic_year,	
		substring(@year5,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct8 when 15 then hct9 when 18 then hct10 when 21 then hct11 when 24 then hct12 when 27 then hct13 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12 
	union all
	--------------------------------------------------------------------------------------
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 1 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 2 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 3 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 4 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 5 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 6 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 7 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 8 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		sum(case seq when 9 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 10 then hct7 when 13 then hct8 when 16 then hct9 when 19 then hct10 when 22 then hct11 when 25 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 11 then hct7 when 14 then hct8 when 17 then hct9 when 20 then hct10 when 23 then hct11 when 26 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year4 as academic_year,
		substring(@year4,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 12 then hct7 when 15 then hct8 when 18 then hct9 when 21 then hct10 when 24 then hct11 when 27 then hct12 end) as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	--------------------------------------------------------------------------------------
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 1 then hct3 end) as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 2 then hct3 end) as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'1' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		sum(case seq when 3 then hct3 end) as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 4 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 5 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'2' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		0 as enrl_wt2,
		sum(case seq when 6 then hct4 end) as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 7 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 8 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'3' as student_level_year,
		0 as enrl_act,
		0 as enrl_wt1,
		sum(case seq when 9 then hct5 end) as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,3,2)+'F' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 13 then hct7 when 16 then hct8 when 19 then hct9 when 22 then hct10 when 25 then hct11 end) as enrl_act,
		sum(case seq when 10 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'W' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 14 then hct7 when 17 then hct8 when 20 then hct9 when 23 then hct10 when 26 then hct11 end) as enrl_act,
		sum(case seq when 11 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	union all
	select 
		@year3 as academic_year,
		substring(@year3,6,2)+'S' as term,
		'A' as as_admit,
		'N' as res_status,
		'NS' as campus,
		'4+' as student_level_year,
		sum(case seq when 15 then hct7 when 18 then hct8 when 21 then hct9 when 24 then hct10 when 27 then hct11 end) as enrl_act,
		sum(case seq when 12 then hct6 end) as enrl_wt1,
		0 as enrl_wt2,
		0 as enrl_wt3,
		0 as enrl_wt4
	from 
		#nAN_temp_table12
	
)foo;


GO