/*Assim que o novo data lake foi inaugurado, os usuários que fazem a gestão dos produtos da empresa nos 
pediram para ter uma visão da média do valor transacionado por cliente, por dia e por tipo de transação (pix, p2p e boleto). 
Crie uma consulta em SQL para extrair essa informação com a seguinte estrutura:

costumer_id | account_id | name | date | transaction_type | mean_value

As tabelas com essas informações estão documentadas no arquivo challenge_table.xlsx na pasta exercicio4.*/

select acc.account_id,
 acc.customer_id, 
 cust.name,
 acc.dt as date,
 transaction_amount.transaction_type, 
 coalesce(transaction_amount.mean_value, 0) as mean_value
from teste_itau.customer  as cust
left join teste_itau.account as acc
	on acc.customer_id = cust.customer_id 
left join (
	select account_id, dt, 'bankslip' as transaction_type, avg(amount) as mean_value
	from teste_itau.bankslip
	group by account_id, dt, transaction_type
	union
	select account_id, dt, transaction_type, avg(amount) as mean_value
	from (
			select account_id_destination as account_id, dt, 'p2p' as transaction_type, amount
			from teste_itau.p2p_tef
			union
			select account_id_source as account_id, dt, 'p2p' as transaction_type, amount 
			from teste_itau.p2p_tef) p2p
		   group by  account_id, dt, transaction_type
	union
	select account_id, dt, transaction_type, avg(amount) as mean_value
	from (
		select account_id as account_id, dt, 'pix' as transaction_type, amount
		from teste_itau.pix_send
		union
		select account_id as account_id, dt, 'pix' as transaction_type, amount 
		from teste_itau.pix_received) pix
	group by  account_id, dt, transaction_type
) transaction_amount
on acc.account_id  = transaction_amount.account_id
and acc.dt = transaction_amount.dt 