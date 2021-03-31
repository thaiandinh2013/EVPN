--QUestion 1 
--Number of delivery jobs per day, 
--Past-7-day trend
	select 
		start_time ::date ,
		count(job_id)
	
	from 
		driver 
	where 
		start_time ::date  >= date_trunc('day', NOW() - interval '7 day')
		and start_time ::date  <= date_trunc('day', NOW() )
	group  by 
		start_time ::date 
--Past-30 -day trend
	select 
		start_time ::date ,
		count(job_id)
	
	from 
		driver 
	where 
		start_time ::date  >= date_trunc('day', NOW() - interval '1 month')
		and start_time ::date  <= date_trunc('day', NOW() )
	group  by 
		start_time ::date 
		
--peak delivery hours throughout a day
	--Past-7-day trend
	
			select 
		extract(hour from start_time::timestamp) ,
		count(job_id)
	
	from 
		driver 
	where 
		start_time ::date  >= date_trunc('day', NOW() - interval '1 month')
		and start_time ::date  <= date_trunc('day', NOW() )
	group  by 
		extract(hour from start_time::timestamp)
		
--peak delivery hours throughout a day
	--Past-30 -day trend
		
			select 
		extract(hour from start_time::timestamp) ,
		count(job_id)
	
	from 
		driver 
	where 
		start_time ::date  >= date_trunc('day', NOW() - interval '7 day')
		and start_time ::date  <= date_trunc('day', NOW() )
	group  by 
		extract(hour from start_time::timestamp)
--------------------------------------------------------------------------------------------------------

		
--QUESTION 2 
-- The below table will show  the average speed 
select 
	job_id,
	sum(distance) :: decimal / sum(time_between_legs) ::decimal speed_per_hour
from
	(select 
		job_id ,
	 	ST_Distance(
	 	ST_MakePoint(lat_start, lon_start),
		ST_MakePoint(lat_end, lon_end) ) distance ,
		 
		extract(second from  age(time_end::timestamp,arrive_time::timestamp) )::decimal /3600  time_between_legs 
		from 
	
	(
		SELECT
				job_id ,
				driver_id ,
				arrive_time ,
				leg_lat lat_start ,
				leg_lon  lon_start,
				Lead(leg_lat ,1) OVER ( partition by job_id ,driver_id 
					ORDER BY arrive_time asc
				) lat_end,
				Lead(leg_lon ,1) OVER (partition by job_id ,driver_id 
					ORDER BY arrive_time asc
				) lon_end,
				Lead(arrive_time ,1) OVER (partition by job_id ,driver_id 
					ORDER BY arrive_time asc
				) time_end
		from
				(select 
					job_id job_id,
					driver_id driver_id,
					start_time arrive_time,
					start_coordinate_lat leg_lat,
					start_coordinate_lon leg_lon
					
				from 
					
					driver  
				union all
				select 
					job_id job_id,
					driver_id driver_id,
					arrive_time arrive_time,
					leg_lat ,
					leg_lon   
				from
					leg
					) t 
				
				
			) t1
	where
		lat_end is not null ) t2
group by job_id
--------------------------------------------------------------------------------------------------------

------Question 3]'
-- The below table will show  the median work speed 
with  median_work_speed as (
select 
	driver_id, 
	avg(time_between_legs) median_work_speed
from 	
(	select 
		driver_id, 
		
		time_between_legs,
		count(*)  OVER ( partition by driver_id
					) total_r_number, 
		row_number()  OVER ( partition by driver_id
						ORDER BY time_between_legs asc
					) r_number
		
		
	from 
	
		(select 
			driver_id,
			extract(second from  age(time_end::timestamp,arrive_time::timestamp) )::decimal   time_between_legs 
		
		from 
			
			(SELECT
						job_id ,
						driver_id ,
						arrive_time ,
						leg_lat lat_start ,
						leg_lon  lon_start,
				
						Lead(arrive_time ,1) OVER (partition by job_id ,driver_id
							ORDER BY arrive_time asc
						) time_end
		--				
	
		--				
			
				from 
					(select 
							job_id job_id,
							driver_id driver_id,
							start_time arrive_time,
							start_coordinate_lat leg_lat,
							start_coordinate_lon leg_lon
							
						from 
							
							driver  
						union all
						select 
							job_id job_id,
							driver_id driver_id,
							arrive_time arrive_time,
							leg_lat ,
							leg_lon   
						from 
							leg) t
							) t1
		where
				time_end is not null ) t2 
				)t3
	where 
		mod(total_r_number,2)=0 and r_number = total_r_number/2 
		or mod(total_r_number,2)=0 and r_number = ceil(total_r_number/2 ) +1 
		or mod(total_r_number,2)!=0 and r_number = ceil(total_r_number/2 ) +1
group  by 
driver_id )

select 
	*
from 

	median_work_speed
order by 
	median_work_speed asc -- to find the best performers order by asc
							--  for worst performers order by desc 
		
limit 10 

--------------------------------------------------------------------------------------------------------
--Question 4 
-- The below table will show  the median work speed 
with average_driving_speed  as (
select 
	driver_id ,
	avg(speed_km_per_hour)speed_km_per_hour
from 
	(select 
			driver_id, 
			
			speed_km_per_hour ,
			count(*)  OVER ( partition by driver_id
						) total_r_number, 
			row_number()  OVER ( partition by driver_id
							ORDER BY speed_km_per_hour  asc
						) r_number
	from 
		(select 
					driver_id,
					ST_Distance(
			 	ST_MakePoint(lat_start, lon_start),
				ST_MakePoint(lat_end, lon_end) ) /
				((case 
					when extract(second from  age(time_end::timestamp,arrive_time::timestamp) )::decimal =0 then  1
					else  extract(second from  age(time_end::timestamp,arrive_time::timestamp) )::decimal  end ) /3600 )speed_km_per_hour 
				
				from 
					
					(SELECT
								job_id ,
								driver_id ,
								arrive_time ,
								leg_lat lat_start ,
								leg_lon  lon_start,
								Lead(leg_lat ,1) OVER ( partition by job_id ,driver_id 
								ORDER BY arrive_time asc
								) lat_end,
						Lead(leg_lon ,1) OVER (partition by job_id ,driver_id 
							ORDER BY arrive_time asc
						) lon_end,
								Lead(arrive_time ,1) OVER (partition by job_id ,driver_id
									ORDER BY arrive_time asc
								) time_end
				--				
			
				--				
					
						from 
							(select 
									job_id job_id,
									driver_id driver_id,
									start_time arrive_time,
									start_coordinate_lat leg_lat,
									start_coordinate_lon leg_lon
									
								from 
									
									driver  
								union all
								select 
									job_id job_id,
									driver_id driver_id,
									arrive_time arrive_time,
									leg_lat ,
									leg_lon   
								from 
									leg) t
									) t2
				where
						time_end is not null  ) t3
						) t4
	where 
		 mod(total_r_number,2)=0 and r_number = total_r_number/2 
		or mod(total_r_number,2)=0 and r_number = ceil(total_r_number/2 ) +1 
		or mod(total_r_number,2)!=0 and r_number = ceil(total_r_number/2 ) +1
group by 
driver_id	)


select 
	*
from 
	average_driving_speed
order by 
speed_km_per_hour asc  -- for best performer , order by asc
						-- for worst performer , order by desc
						-- for speeding , fillter by average_driving_speed
limit 10 
	

				