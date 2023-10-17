preload 5
create table hcy (id int, id2 int, id3 float);
insert into hcy values(1,2,3);
insert into hcy values(1,2,4);
insert into hcy values(3,5,4);
insert into hcy values(4,8,1);

txn1 4
t1a begin;
t1b select * from hcy where id = 1;
t1c update hcy set id = 5 where id = 1;
t1d commit;

txn2 5
t2a begin;
t2b select * from hcy where id = 1;
t2c update hcy set id = 5 where id = 1;
t2d select * from hcy where id = 1;
t2e commit;

permutation 8
t1a
t2a
t1b
t2b
t1c
t2c
t1d
t2d
t2e