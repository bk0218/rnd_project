import configparser

config = configparser.ConfigParser()
config.read('./config')

oracle_config = config['ORACLE_INFO']
oracle_ip = oracle_config['oracle_IP']

oracle_c = config['ORACLE_INFO']['oracle_IP']

print(oracle_ip)
print(oracle_c)
