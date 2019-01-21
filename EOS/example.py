from eos_data_fetch_pk import eos_data_fetch

#%%
nodes='https://api.eosnewyork.io'
start_block = 33000500 
end_block = 33000600
eos = eos_data_fetch(nodes,start_block,end_block)

#%%
block = eos.eos_block()
transaction = eos.transaction_in_block()[0]
action = eos.transaction_in_block()[1]
