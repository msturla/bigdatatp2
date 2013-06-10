create table TotalViewers( metric_key varchar(40) not null, minute int not null, quantity int not null);
create table ViewersPerCategory( metric_key varchar(40) not null, minute int not null, quantity int not null);
create table ViewersPerChannel( metric_key varchar(40) not null, minute int not null, quantity int not null);
create table ViewersPerFamilyGroup( metric_key varchar(40) not null, minute int not null, quantity int not null);
create table ViewersPerType( metric_key varchar(40) not null, minute int not null, quantity int not null);