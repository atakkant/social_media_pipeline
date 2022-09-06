update users set issmoking=NULL WHERE issmoking='';
alter table users alter column issmoking type boolean using issmoking::boolean;

update users set income=NULL WHERE income='';
alter table users alter column income type real using income::real;

/* How many total messages are being sent every day? */
select createdat::date "day",count(*) from messages group by 1;

/* Are there any users that did not receive any message? */
select users.id,count(receiverid) 
from users 
left join messages 
on users.id=messages.receiverid 
group by users.id 
having count(messages.receiverid)=0;

/* How many active subscriptions do we have today? */
select * from subscriptions where createdat::date=date_trunc('day', now());

/* Are there users sending messages without an active subscription? (some extra
context for you: in our apps only premium users can send messages) */
select messages.senderid 
from messages 
where senderid not in (select subscriptions.user_id from subscriptions where subscriptions.subs_status = 'Active');

/* How much is the average subscription amount (sum amount subscriptions /
count subscriptions) breakdown by year/month (format YYYY-MM)? */
select avg(subscriptions.amount) as average_amount, date_trunc('month', createdat) as month from subscriptions group by month;