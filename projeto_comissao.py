from __future__ import print_function
import os.path
from certifi import where
import gspread
import numpy as np
import pandas as pd
from pickle import TRUE
import psycopg2
from datetime import datetime
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import re
from functools import reduce
######################################################################################################################################################################################################################
con = psycopg2.connect(user = "postgres",
                        password = "postgres",
                        host = "localhost",
                        port = "5432",
                        database = "bdComissao")
con.autocommit = True
cur = con.cursor()
######################################################################################################################################################################################################################
sql_select1 = '''SELECT * FROM percentual'''
cur.execute(sql_select1)
rows_porcentagem = cur.fetchall()
dict_list = {}
for i in rows_porcentagem:
    dict_list[i[0]] = i[1]
######################################################################################################################################################################################################################
sql_select2 = '''SELECT * FROM ecrctitulo'''
cur.execute(sql_select2)
rows_titulo = cur.fetchall()
res = list(map(list, rows_titulo))
lista = []
lista_contrato = []
for i in res:
    deobservacao = re.split(' |:', i[6])
    if i[1].upper() == '1':
        i[1]='Vendedor 1'
    elif i[1].upper() == '2':
        i[1]='Vendedor 2'
    elif i[1].upper() == '3':
        i[1]='Vendedor 3'
    if deobservacao[-2].upper() == 'SAAS_1':
        lista = [i[0], i[2], i[3], i[4], i[5], i[1], deobservacao[-1], 0]
    elif deobservacao[-2].upper() == 'SAAS_2':
        lista = [i[0], i[2], i[3], i[4], i[5], i[1], 0, deobservacao[-1]]
    elif deobservacao[-2].upper() != 'SAAS_1' or deobservacao[-2].upper() != 'SAAS_2':
        lista = [i[0], i[2], i[3], i[4], i[5], i[1], 0, 0]
    lista_contrato.append(lista)
dfcontratos = pd.DataFrame(columns=['Numero Titulo', 'Valor SERVIÇOS', 'Cliente', 'CNPJ', 'Data Emissao', 'Vendedor','Contrato SaaS 1', 'Contrato Saas 2'], data=lista_contrato)
######################################################################################################################################################################################################################
dfsaas1 = pd.read_excel("Fechamento SaaS 1.xlsx", header = 0)
dfsaas1 = dfsaas1.replace(np.nan, 0)
dfsaas1.columns = ['Contrato SaaS 1', 'CNPJ', 'Cliente', 'Vendedor', 'Data Contrato SaaS 1', 'Valor SaaS 1']
######################################################################################################################################################################################################################
gc = gspread.service_account('formulariosaas2.json')
sh = gc.open("Planilha")
dfsaas2 = pd.DataFrame(sh.sheet1.get_all_records())
dfsaas2.columns = ['Contrato SaaS 2', 'Data Contrato SaaS 2', 'Cliente', 'CNPJ', 'Vendedor', 'Valor SaaS 2']
######################################################################################################################################################################################################################
dfsaas1['Contrato SaaS 1'] = dfsaas1['Contrato SaaS 1'].astype(str)
dfsaas1['Data SaaS 1'] = dfsaas1['Data SaaS 1'].astype(str)
dfsaas2['Contrato SaaS 2'] = dfsaas2['Contrato SaaS 2'].astype(str)
dfsaas2['Data SaaS 2'] = dfsaas2['Data SaaS 2'].astype(str)
######################################################################################################################################################################################################################
dfauxiliar1 = pd.merge(dfsaas1, dfcontratos, how = 'left', on=['Contrato SaaS 1', 'Cliente', 'Vendedor', 'CNPJ'])
dfauxiliar2 = pd.merge(dfsaas2, dfcontratos, how = 'left', on=['Contrato SaaS 2', 'Cliente', 'Vendedor', 'CNPJ'])
dfvenda = pd.concat([dfauxiliar1,dfauxiliar2, dfcontratos])
dfvenda = dfvenda.drop_duplicates(subset=['Numero Titulo', 'Contrato SaaS 1', 'Contrato SaaS 2'])
dfvenda = dfvenda.replace(np.nan, 0)
dfvenda=dfvenda.reindex(columns = ['Numero Titulo', 'Contrato SaaS 1', 'Contrato SaaS 2', 'Vendedor', 'Cliente', 'CNPJ', 'Valor SaaS 1', 'Valor Saas 2', 'Valor Serviço', 'Data Emissao', 'Data Contrato SaaS 1', 'Data Contrato SaaS 2'])
######################################################################################################################################################################################################################
tuples = [tuple(x) for x in dfvenda.to_numpy()]
for i in tuples:
    sql_insert1 = '''INSERT INTO venda(nutitulo, contrato_saas1, contrato_saas2, vendedor, cliente, cnpj, valor_saas1, valor_saas2, valor_servico, data_emissao, data_saas1, data_saas2) VALUES{} ON CONFLICT (nutitulo, contrato_saas1, contrato_saas2) DO NOTHING'''.format(i)
    cur.execute(sql_insert1)
######################################################################################################################################################################################################################
sql_select3 = '''SELECT 
	v.nutitulo,
    contrato_saas1,
    contrato_saas2,
	ecs.nutitulodestino,
    vendedor,
    cliente,
    cnpj,
    tipo,
    modalidade,
    valor_saas1,
    valor_saas2,
    valor_servico,
    ecb.dtvenctoant,
    data_saas1,
    data_saas2,
	ecb.flparcialtotal
FROM 
	venda v
INNER JOIN
	ecrcsubstituicao ecs
ON
	v.nutitulo = ecs.nutitulo
INNER JOIN
	ecrcbaixa ecb
ON
	ecb.nutitulo = ecs.nutitulodestino
WHERE
    ecb.nuparcela = 1'''
cur.execute(sql_select3)
rows_join_comissao = cur.fetchall()
dfcomissao = pd.DataFrame(columns=["TITULO", "CONTRATO SAAS 1", "CONTRATO SAAS 2", "TITULO SUBSTITUIÇÃO", "VENDEDOR", "CLIENTE", "CNPJ", "COMISSAO SAAS 1", "COMISSAO SAAS 2", "COMISSAO SERVIÇOS", "VENCIMENTO PARCELA", "DATA SAAS 1", "DATA SAAS 2", "STATUS"],data=rows_join_comissao)
######################################################################################################################################################################################################################
sql_select4 = '''SELECT
    nutitulo,
    contrato_saas1,
    contrato_saas2,
    vendedor,
    cliente,
    cnpj,
    valor_saas1,
    valor_saas2,
    valor_servico,
    data_saas1,
    data_saas2
FROM 
    venda
WHERE
    valor_servico = 0'''
cur.execute(sql_select4)
rows_sem_servico = cur.fetchall()
auxiliar = []
lista_sem_servico = []
for i in rows_sem_servico:
    auxiliar = [i[0], i[1], i[2], '0', i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], '0', i[13], i[14], '-']
    lista_sem_servico.append(auxiliar)
dfvenda_sem_servico = pd.DataFrame(columns=["TITULO", "CONTRATO SAAS 1", "CONTRATO SAAS 2", "TITULO SUBSTITUIÇÃO", "VENDEDOR", "CLIENTE", "CNPJ", "COMISSAO SAAS 1", "COMISSAO SAAS 2", "COMISSAO SERVIÇOS", "VENCIMENTO PARCELA", "DATA SAAS 1", "DATA SAAS 2", "STATUS"],data=lista_sem_servico)
######################################################################################################################################################################################################################
dfcomissao = pd.concat([dfcomissao, dfvenda_sem_servico], ignore_index=True)
dfcomissao['COMISSAO SAAS 1'] = dfcomissao['COMISSAO SAAS 1'] * dict_list['SAAS 1']
dfcomissao['COMISSAO SAAS 2'] = dfcomissao['COMISSAO SAAS 2'] * dict_list['SAAS 2']
dfcomissao['COMISSAO SERVIÇOS'] = dfcomissao['COMISSAO SERVIÇOS'] * dict_list['SERVICO']
for i in range(len(dfcomissao)):
    if dfcomissao.loc[i,'COMISSAO SERVIÇOS'] != '0':
        data = datetime.strptime(dfcomissao.loc[i,'VENCIMENTO PARCELA'], "%d/%m/%Y").date()
        data = data + relativedelta(months =+ 2)
        data_string = data.strftime("%b/%Y")
        dfcomissao.loc[i,'DATA PREVISÃO'] = data_string
    else:
        data = datetime.now().date()
        data = data + relativedelta(months =+ 1)
        data_string = data.strftime("%b/%Y")
        dfcomissao.loc[i,'DATA PREVISÃO'] = data_string
dfcomissao['VALOR TOTAL'] = dfcomissao['COMISSAO SAAS 1'] + dfcomissao['COMISSAO SERVIÇOS'] + dfcomissao['COMISSAO SAAS 2']
dfcomissao = dfcomissao.drop(columns = ['VENCIMENTO PARCELA', 'DATA SAAS 1', 'DATA SAAS 2'])
######################################################################################################################################################################################################################
tuples = [tuple(x) for x in dfcomissao.to_numpy()]
for i in tuples:
    sql_insert2 = '''INSERT INTO comissao(nutitulo, contrato_saas1, contrato_saas2, nutitulodestino, vendedor, cliente, cnpj, comissao_saas1, comissao_saas2, comissao_servico, status, data_pagamento, valor_total_comissao) VALUES{} ON CONFLICT (nutitulo, contrato_sienge, contrato_cv) DO NOTHING'''.format(i)
    cur.execute(sql_insert2)
######################################################################################################################################################################################################################
sql_update1 = 'UPDATE comissao SET status_comissao = %s WHERE comissao_servico = %s;'
cur.execute(sql_update1, ("LIBERADO", 0))
sql_update2 = 'UPDATE comissao SET status_comissao = %s WHERE status = %s and comissao_servico != %s;'
cur.execute(sql_update2, ("LIBERADO", "T", 0))
sql_update3 = 'UPDATE comissao SET status_comissao = %s WHERE status = %s and comissao_servico != %s;'
cur.execute(sql_update3, ("NÃO LIBERADO", "P", 0))
#T = PAGO
#P = EM ABERTO
######################################################################################################################################################################################################################