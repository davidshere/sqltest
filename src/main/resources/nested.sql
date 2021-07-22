SELECT *
FROM claims cl
JOIN (
    SELECT *
    FROM person
) as p on p.id = cl.id
JOIN table2 t2 ON t2.id = cl.id