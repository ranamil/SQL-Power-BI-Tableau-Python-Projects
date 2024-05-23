with adv as
(
select l.loan, m.orgid, p.scenarioid, l.pool_client, count(l.loan) count, sum(p.totalprincipal) upb, sum(p.price) advprice,
sum(p.price)/sum(p.totalprincipal) waprice

from loan_summary_fields_wln l, loan_details_main m, pricing_output p
where l.loan = p.assetid and
l.summarydatadate = p.datadate and
p.assetid = m.id and
p.datadate = m.datadate and
p.scenarioid = 33589

group by p.scenarioid, l.pool_client, l.loan, m.orgid
),

base as
(
select l.loan, p.scenarioid, l.pool_client, count(l.loan) count, sum(p.totalprincipal) upb, sum(p.price) baseprice,
sum(p.price)/sum(p.totalprincipal) waprice

from loan_summary_fields_wln l, loan_details_main m, pricing_output p
where l.loan = p.assetid and
l.summarydatadate = p.datadate and
p.assetid = m.id and
p.datadate = m.datadate and
p.scenarioid = 33591

group by p.scenarioid, l.pool_client, l.loan
),

sev as
(
select l.loan, p.scenarioid, l.pool_client, count(l.loan) count, sum(p.totalprincipal) upb, sum(p.price) sevprice,
sum(p.price)/sum(p.totalprincipal) waprice

from loan_summary_fields_wln l, loan_details_main m, pricing_output p
where l.loan = p.assetid and
l.summarydatadate = p.datadate and
p.assetid = m.id and
p.datadate = m.datadate and
p.scenarioid = 33590

group by p.scenarioid, l.pool_client, l.loan
),

A as
(
select p.scenarioid, l.pool_client, count(l.loan) count, sum(p.totalprincipal) upb, sum(p.price) advprice,
sum(p.price)/sum(p.totalprincipal) waprice

from loan_summary_fields_wln l, loan_details_main m, pricing_output p
where l.loan = p.assetid and
l.summarydatadate = p.datadate and
p.assetid = m.id and
p.datadate = m.datadate and
p.scenarioid = 33589

group by p.scenarioid, l.pool_client
),

B as
(
select p.scenarioid, l.pool_client, count(l.loan) count, sum(p.totalprincipal) upb, sum(p.price) baseprice,
sum(p.price)/sum(p.totalprincipal) waprice

from loan_summary_fields_wln l, loan_details_main m, pricing_output p
where l.loan = p.assetid and
l.summarydatadate = p.datadate and
p.assetid = m.id and
p.datadate = m.datadate and
p.scenarioid = 33591

group by p.scenarioid, l.pool_client
),

S as
(
select p.scenarioid, l.pool_client, count(l.loan) count, sum(p.totalprincipal) upb, sum(p.price) sevprice,
sum(p.price)/sum(p.totalprincipal) waprice
from loan_summary_fields_wln l, loan_details_main m, pricing_output p
where l.loan = p.assetid and
l.summarydatadate = p.datadate and
p.assetid = m.id and
p.datadate = m.datadate and
p.scenarioid = 33590

group by p.scenarioid, l.pool_client
),

Final as
(
select 
a.pool_client,

cast((sum(a.advprice)/sum(b.baseprice)-1) * 100 as decimal(15,2)) cre_adverse,

(sum(a.advprice)/sum(b.baseprice)-1) cre_adverse_b,

cast((sum(s.sevprice)/sum(b.baseprice)-1) * 100 as decimal(15,2)) cre_severe,

(sum(s.sevprice)/sum(b.baseprice)-1) cre_severe_b

from A,B,S
where a.pool_client = b.pool_client
and b.pool_client = s.pool_client
group by a.scenarioid, a.pool_client, s.scenarioid, s.pool_client, b.count, b.scenarioid, b.pool_client
),

adjvalue as
(
select

adv.loan, adv.orgid, adv.pool_client advmgntp, sum(adv.upb) totalprincipal,
sum(base.baseprice) baseprice, sum(adv.advprice) advprice, sum(sev.sevprice) sevprice,
case when (base.baseprice * (1 + final.cre_adverse_b)) > adv.advprice
then adv.advprice
else base.baseprice * (1 + final.cre_adverse_b)
end
newadvprice,

case when (base.baseprice * (1 + final.cre_severe_b)) > sev.sevprice
then sev.sevprice
else base.baseprice * (1 + final.cre_severe_b)
end
newsevprice,
cre_adverse,
cre_severe

from adv left join base on base.loan = adv.loan
left join sev on adv.loan = sev.loan
left join final on final.pool_client = adv.pool_client
where adv.advprice > base.baseprice
or sev.sevprice > base.baseprice

group by adv.loan, adv.orgid, adv.pool_client, cre_adverse_b, final.cre_severe, final.cre_adverse, base.baseprice, final.cre_severe_b,
case when (base.baseprice * (1 + final.cre_adverse_b)) > adv.advprice
then adv.advprice
else base.baseprice * (1 + final.cre_adverse_b)
end,
case when (base.baseprice * (1 + final.cre_severe_b)) > sev.sevprice
then sev.sevprice
else base.baseprice * (1 + final.cre_severe_b)
end
),

adjprice as
(
select

p.assetid loan_num, p.price price_$, p.totalprincipal upb,
(p.price/p.totalprincipal) * 100 price_percent

case when p.scenarioid = 33591 then p.price
when p.assetid = adjvalue.loan and p.scenarioid = 33590 then adjvalue.newsevprice
--please enter adverse scenario id
when p.assetid = adjvalue.loan and p.scenarioid = 33589 then adjvalue.newadvprice
else p.price 
end adjstedprice,

p.scenarioid scenario

from pricing_output p left outer join adjvalue
on adjvalue.loan = p.assetid
where p.scenarioid = 33591
)

select

ap.scenario,
m.orgid,
l.pool_client,
count(*) count_of_loan_num,
sum(ap.upb) sum_of_upb,
sum(ap.price_$) sum_of_price_$,
sum(ap.adjustedprice) sum_of_adjusted_price

from loan_summary_fields_wln l, adjprice ap, loan_details_main m
where ap.loan_num = l.loan
and ap.loan_num = m.id
and l.summarydadate = '2023-02-28 00:00:00.000'
and m.datadate = '2023-02-28 00:00:00.000'

an l.pool_client = 'RES2'

group by ap.scenario,
m.orgid,
l.pool_client
  
order by cast(m.orgid as int)
