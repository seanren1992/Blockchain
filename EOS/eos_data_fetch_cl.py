# import library
from eosapi import Client
import pandas as pd

class eos_data_fetch(object):
    def __init__(self,nodes,start_block,end_block):
        self.nodes = nodes
        self.start_block = start_block
        self.end_block = end_block

    
    def eos_api_fetch(self):
        c = Client(nodes=[self.nodes])
        start = self.start_block
        end = self.end_block
        blocks_dict = []
        for block_num in range(start, end+1):
            blocks_dict.append(c.get_block(block_num))
        return(blocks_dict)

    
    def eos_block(self):
        # Block Information table 
        blocks_dict = self.eos_api_fetch()
        trx_len = []
        for i in blocks_dict:
            trx_len.append(len(i['transactions']))

        block_df = pd.DataFrame(blocks_dict)
        block_df['transactions'] = trx_len
        block_df = block_df.drop(['block_extensions','header_extensions'],axis=1)
        block_df.to_csv('eos_block.csv',index=False)
        return(block_df)

        
    def transaction_in_block(self):
        # Transactions in block table
        transactions_df = []
        blocks_dict = self.eos_api_fetch()
        block_num = []
        for j in blocks_dict:
            transactions_df.append(pd.DataFrame(j['transactions']))
            block_num.append(pd.DataFrame([j['block_num']]*len(j['transactions'])))
        transactions_df = pd.concat(transactions_df,axis=0)
        block_num = pd.concat(block_num,axis=0)
        transactions_df['block_num'] = block_num
        transactions_df.index = range(len(block_num))
        # Trx in transactions of block
        trx_dict = transactions_df.trx.tolist()
        trx_list = trx_dict.copy()
        trx_list1 = trx_dict.copy()
        for ind in range(len(trx_dict)):
            if type(trx_dict[ind]) is str:
                trx_list[ind] = trx_dict[ind]
            else:
                trx_list[ind] = trx_dict[ind]['id']
        for ind in range(len(trx_dict)):
            if type(trx_dict[ind]) is str:
                trx_list1[ind] = 1
        temp = transactions_df.copy()        
        temp['trx'] = trx_list1       
        temp = temp[temp.trx != 1]
        trx_dict_update = temp.trx.tolist()
        tx_df = pd.DataFrame.from_dict(trx_dict_update)
        tx_df['block_num'] = temp['block_num'].tolist()
        tx_df.to_csv('tx_in_transactions.csv')
        # Transaction in block updated to change dict type 
        transactions_df['trx'] = trx_list
        transactions_df.to_csv('transactions_in_block.csv',index=False)
        # Transaction in Trx
        tran_tx = pd.DataFrame.from_dict(tx_df.transaction.tolist())
        tran_tx['id'] = tx_df['id'].tolist()
        tran_tx['block_num'] = tx_df['block_num'].tolist()
        tran_tx = tran_tx.drop(['context_free_actions','transaction_extensions'],axis=1) 
        tran_tx.to_csv('tran_tx.csv',index=False)
        
        # Actions data in block
        action_dict = tran_tx.actions.tolist()
        action_dict_id = tran_tx.id.tolist()
        action_dict_block_num = tran_tx.block_num.tolist()
        action = []
        action_id = []  
        action_block_num = []
        for m in range(len(action_dict)):
            if len(action_dict[m]) == 1:
                action.append(action_dict[m][0])
                action_id.append(action_dict_id[m])
                action_block_num.append(action_dict_block_num[m])
            if len(action_dict[m]) > 1:
                for n in range(len(action_dict[m])):
                    action.append(action_dict[m][n])
                    action_id.append(action_dict_id[m])
                    action_block_num.append(action_dict_block_num[m])
        actions_df_raw = pd.DataFrame.from_dict(action)
        actions_df_raw['id'] = action_id
        actions_df_raw['block_num'] = action_block_num
        auth_dict = []
        for n in actions_df_raw.authorization.tolist():
            if len(n) == 1:
                auth_dict.append(n[0])
            else:
                for index in range(len(n)):
                    auth_dict.append(n[index])
        auth_df = pd.DataFrame.from_dict(auth_dict)    
        # Data dict
        data_dict = [ {} if isinstance(k, str) else k for k in actions_df_raw.data.tolist()]
        data_df = pd.DataFrame.from_dict(data_dict)
        #data_summary = data_df.describe()
        #data_df = data_df[data_summary.columns.values]
        actions_df = pd.concat([actions_df_raw,auth_df,data_df],axis=1)
        actions_df = actions_df.drop(['authorization','data'],axis=1)
        actions_df.to_csv('actions.csv',index=False)
        return(transactions_df,actions_df)

 
