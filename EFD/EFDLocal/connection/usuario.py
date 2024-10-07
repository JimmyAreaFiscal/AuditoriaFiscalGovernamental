import oracledb 

# Defining Package Attributes for connection
ip = '192.169.1.2'
port = '1521'
sid = 'SIATEHOM'
dns_tns = oracledb.makedsn(ip, port, sid)
pw = 'SIATE_CONSULTA'
user = 'SIATE_CONSULTA'