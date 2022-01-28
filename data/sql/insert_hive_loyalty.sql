INSERT OVERWRITE TABLE loyalty
PARTITION(data_dt='${data_dt}')
select a.cust_id, a.cust_nm, a.cust_member_card_no, sum(b.odr_prc) as spnd_amt 
from customers_cln as a
join transactions_cln as b
on a.cust_id = b.cust_id
group by a.cust_id, a.cust_nm, a.cust_member_card_no
