CREATE TABLE person (
  id int primary key,
  firstname varchar(255),
  lastname varchar(255),
  zip varchar(10)
);

CREATE TABLE claims (
  id int primary key,
  person_id int,
  allowed_amt int,
  constraint fk_person foreign key(person_id) references person(id)
);
