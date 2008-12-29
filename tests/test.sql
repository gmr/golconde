-- Golconde Test Schema

CREATE TABLE friendship_statuses (
  status_id int2 not null primary key,
  description text not null
);

CREATE TABLE friendships (
  user_id int8 not null,
  friend_id int8 not null,
  created_at timestamp with time zone not null default now(),
  status_id int2 not null default 0,
  primary key ( user_id, friend_id ),
  foreign key ( status_id ) references friendship_statuses ( status_id ) ON DELETE RESTRICT ON UPDATE CASCADE
);

INSERT INTO friendship_statuses VALUES ( 0, 'Unconfirmed' );
INSERT INTO friendship_statuses VALUES ( 1, 'Confirmed' );
INSERT INTO friendship_statuses VALUES ( 2, 'Blocked' );
INSERT INTO friendship_statuses VALUES ( 3, 'Severed' );