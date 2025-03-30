 {{config(materialized= 'table',schema= 'transforming_dev')}}

  with recursive managers 
    (indent, employee_id, employee_name, employee_title, manager_id, manager_name, manager_title, office) 
    as(
        select '*' as indent,
            empid as employee_id,
            firstname as employee_name,
            title as employee_title, 
            empid as manager_id,
            firstname as manager_name, 
            title as manager_title, office
        from {{ref('stg_employees')}} where title = 'President'
        
        union all

        select indent || '*',
            e.empid as employee_id, 
            e.firstname as employee_name, 
            e.title as employee_title,
            m.employee_id,
            m.employee_name,
            m.employee_title, e.office
        from {{ref('stg_employees')}} as e join managers as m on e.reportsto = m.employee_id
      ),
      
    offices
        (office_id, office_city, office_state, office_country)
    as (
    
   select
        office, 
        officecity,  
        iff(OFFICESTATEPROVINCE is null, 'NA', OFFICESTATEPROVINCE),
        officecountry
   from {{ref('stg_offices')}}
   ) 
 
  -- This is the "main select".

  select 
   m.indent, m.employee_id, m.employee_name, m.employee_title, m.manager_id, m.manager_name, m.manager_title, o.office_id, o.office_city, o.office_state, o.office_country  
  from managers m 
  inner join offices o on m.office=o.office_id