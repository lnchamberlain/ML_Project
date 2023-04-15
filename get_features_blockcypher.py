'''
CSCEA415 Machine Learning
Feature extraction from BTC Address JSON files

Extracts the following features:

tx_count -> Number of transactions
total_in -> Sum incoming BTC
total_out -> Sum of outgoing BTC
final_balance -> total_in - total_out
aliases -> # of input addresses for input tx's where A is also an input address
fellow_reciever -> # of output addressses  for output tx's where A is also an output address
convergence -> aliases / fellow_reciever (if > 1, is convergent, else divergent)
incoming_range -> largest incoming value - smallest incoming value
outgoing_range -> largest outgoing value - smallest outgoing valuex


From the BTC abuse dataset not generated in this file:

num_reports -> # of reports filed w/ bitcoinabuse.com for this address
abuse_type -> 1-5 {1:ransomware, 2:darknet marketplace, 3:BTC tumbler, 4:Scam, 5:Benign}

'''
import json
import pandas as pd
from statistics import mean
from sys import exit
import os
import pickle



def enumerate_files():
    files = []
    path = "/home/lchamb/Desktop/Spring 2023/ML/Project/process_jsons"
    for r, d, f in os.walk(path):
        for file in f:
            if ("get_features" not in os.path.join(r,file) and 'pickle' not in os.path.join(r,file)) :
                files.append(os.path.join(r,file))
    return files
#Takes in an address JSON file and returns a list of length three:
# a[0] = aliases_per_inbound
# a[1] = reciever_per_outbound
# a[2] = convergence
def get_aliases_and_convergence(address):
    addr = address['address']
    print(addr)
    a = [0,0]
    for tx in address['txs']:
        #Parse inputs to tx
        input_addresses = []
        for inp in tx['inputs']:
            if(inp.get('addresses') is not None):
                    input_addresses.append(inp['addresses'][0])
            
        #Parse outputs to tx
        output_addresses = []
        for out in tx['outputs']:
            if(out.get('addresses') is not None):
                    output_addresses.append(out['addresses'][0])
        if(addr in input_addresses):
            #Don't count the address in question
            a[0] += len(input_addresses) - 1
        if(addr in output_addresses):
            a[1] += len(output_addresses) - 1
        
    return a

#Takes in an address in JSON form and returns a list of length 2
# r[0] = input_range
# r[1] = output_range
def get_ranges_and_average(address):
    r = [0,0,0]
    input_vals = []
    output_vals = []    
    for tx in address['txs']:
        #Parse inputs to tx
        for inp in tx['inputs']:
            if(inp.get('output_value') is not None):
                input_vals.append(inp['output_value'])
            
        #Parse outputs to tx
        for out in tx['outputs']:
            if(out.get('value') is not None):
                output_vals.append(out['value'])
    if(len(input_vals) == 0):
        input_vals.append(0)
    if(len(output_vals) == 0):
        output_vals.append(0)
    r[0] = (max(input_vals) - min(input_vals))
    r[1] = (max(output_vals) - min(output_vals))
    r[2] = round(mean(input_vals + output_vals), 4)
    return r
    
def get_features(address):
    features = {}
    features['Address'] = address['address']
    features['tx_count'] = address['n_tx']
    features['total_in'] = address['total_received']
    features['total_out'] = address['total_sent']
    features['final_balance'] = address['final_balance']
    #get_aliases_and_convergence returns the next three values as they are related
    a = get_aliases_and_convergence(address)
    features['aliases'] = a[0]
    features['fellow_receivers'] = a[1]
    if(a[1] == 0):
        features['convergence'] = 0
    else:
        features['convergence'] = round(a[0] / a[1], 4)
    #get_ranges returns a tuple with the inbound range at 0 and outbound at 1 
    r = get_ranges_and_average(address)
    features['incoming_range'] = r[0]
    features['outgoing_range'] = r[1]
    features['avg_tx'] = r[2]
    return features



def main():
    df = pd.DataFrame(columns=['address','tx_count','total_in','total_out','final_balance','aliases','fellow_receivers','convergence','incoming_range','outgoing_range','avg_tx'])
    files = enumerate_files()
    #with open('p.pickle','rb') as dict_file:
    #    reports_dict = pickle.load(dict_file)
    for path in files:
        f = open(path, "r")
        addr = json.load(f)
        f.close()
        features = get_features(addr)
        #report_count = int(reports_dict[features['Address']])
        #get rows
        #address	tx_count	total_in	total_out	final_balance	aliases	fellow_receivers	convergence	incoming_range	outgoing_range	avg_tx

        row = [features['Address'], features['tx_count'], features['total_in'], features['total_out'], features['final_balance'], features['aliases'], features['fellow_receivers'], features['convergence'], features['incoming_range'], features['outgoing_range'], features['avg_tx']]#, report_count]
        df.loc[len(df.index)] = row
    
    df.to_csv('features.csv')



if __name__ == "__main__":
    main()
