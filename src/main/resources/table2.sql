CREATE TABLE person (
  id int,
  firstname String,
  lastname String,
  zip String
) engine = TinyLog;

CREATE TABLE claims (
  id int,
  person_id int,
  allowed_amt int
) engine = TinyLog;
