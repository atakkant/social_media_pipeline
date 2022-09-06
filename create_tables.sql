create table users (
    id INT NOT NULL PRIMARY KEY,
    createdAt timestamp,
    updatedAt timestamp,
    city varchar(50),
    country varchar(50),
    email_domain varchar(50),
    gender varchar(50),
    isSmoking varchar(50),
    income varchar(50)
);

create table messages (
    id INT NOT NULL PRIMARY KEY,
    createdAt timestamp,
    receiverId int REFERENCES users (id),
    senderId int REFERENCES users (id)
);

create table subscriptions (
    id SERIAL NOT NULL PRIMARY KEY,
    createdAt timestamp,
    startDate timestamp,
    endDate timestamp,
    subs_status varchar(50),
    amount REAL,
    user_id INT REFERENCES users (id)
)