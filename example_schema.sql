drop table if exists examples;

create table `examples` (
  `id` int not null auto_increment,
  `name` varchar(80) default null,
  primary key (`id`)
);

drop table if exists places;

create table `places` (
  `id` int not null auto_increment,
  `city` varchar(80) not null,
  `county` varchar(80) default null,
  `country` varchar(80) not null,
  primary key (`id`)
);

drop table if exists people;

create table `people` (
  `id` int not null auto_increment,
  `given_name` varchar(80) not null,
  `family_name` varchar(80) not null,
  `date_of_birth` varchar(80) default null,
  `place_of_birth_id` int default null,
  primary key (`id`),
  foreign key (`place_of_birth_id`) references places(`id`)
);
