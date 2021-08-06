SELECT
  person_id,
  CAST(SUM(allowed_amt) AS INTEGER) AS high_cost_claims
FROM claims
GROUP BY person_id
HAVING SUM(allowed_amt) > 1000
ORDER BY person_id