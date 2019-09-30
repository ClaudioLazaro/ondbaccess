#!/usr/local/bin/python3.7
import concurrent.futures
import argparse 
import subprocess
import time

from functools import partial
from datetime import datetime
from os import mkdir, path, getenv, chdir, listdir

parser = argparse.ArgumentParser(description='ondbacess, vai te ajudar na execucao de diversos arquivo em lista, serve para Informix ')
parser.add_argument('ffile',  type=str, help='Arquivo contendo Bases e Unidades' )
parser.add_argument('sfile',  type=str, help='Arquivo contendo Scripts')
parser.add_argument('prll',  type=int, help='Quantidade de paralelismos')
parser.add_argument('modes',  type=str, help='Mode de execuao single ou mult')

#Parametros de Inicializacao 
args = vars(parser.parse_args())

fargs = args['ffile']
sargs = args['sfile']
prll = args['prll']
modes = args['modes']

#Classe de abertura de Arquivos
class OpenFile (object):
    
    def __init__(self, filename):
        self.filename = filename


    def openFile(self):
        with open(self.filename, "r") as file:
            return file.read().splitlines()

def dbaccess(lscrips, mode, ldatadb ):

    #Variaveis Nomes 
    uniname = ldatadb.split(',')[1]
    ifxname = ldatadb.split(',')[0]

    #Criacao de diretorio 
    mkdir(uniname)
        
    #Copiando Scripts.sql
    copyf = subprocess.Popen(['cp *.sql '+uniname],shell=True,stdout=subprocess.PIPE)
    copyf.communicate()

    #Entrando no novo diretorio
    chdir(uniname)

    ifxsingle = 'EXECUTE FUNCTION ADMIN(\'onmode\',\'j\',\'y\');'
    ifxmult = 'EXECUTE FUNCTION ADMIN(\'onmode\',\'m\');'

    single = subprocess.Popen(['echo \"'+ifxsingle+'\" |dbaccess -e sysadmin@'+ifxname+' &>> /dev/null'], shell=True, stdout=subprocess.PIPE)
    multuser = subprocess.Popen(['echo \"'+ifxmult+'\" |dbaccess -e sysadmin@'+ifxname+' &>> /dev/null'], shell=True, stdout=subprocess.PIPE)

    if modes =='single':
        print('Execucao em :'+modes)
        #Deixando em single user
        single.communicate()
        #looping para execucao lista dos scrips
        #O Tratamento do looping das instancias sera feito na funcao main, em paralelismo
        for lscrip in lscrips:
            print('Executando :: dbacess -e wpdhosp@'+ifxname+' '+lscrip)

            
            #Chamada de execucao do dbaccess via subprocess
            dbaccess = subprocess.Popen(['dbaccess -e wpdhosp@'+ifxname+' '+lscrip+' &> '+uniname+'_'+lscrip+'.out'],shell=True,stdout=subprocess.PIPE)
            #Inicio
            start = datetime.now()
            dbaccess.communicate()
            end = datetime.now()
            #Inicio

            finaltime = (end - start).total_seconds()
            msglog = 'Instancia: '+ifxname+' Unidade :'+uniname+' Script : '+lscrip+' Hora Inicio :'+str(start)+' Hora Fim :'+str(end)+' TEMPO TOTAL: '+str(finaltime)
            logstimes = subprocess.Popen(['echo '+msglog+' >>'+uniname+'_times.log'],shell=True,stdout=subprocess.PIPE)
            logstimes.communicate()
  
        #Deixando em Multi User
        multuser.communicate()
        #Voltando para o diretorio 
        chdir('..')

    elif  modes == 'mult':

        print('Execucao em :'+modes)
        for lscrip in lscrips:
            print('Executando :: dbacess -e wpdhosp@'+ifxname+' '+lscrip)

            #Chamada de execucao do dbaccess via subprocess
            dbaccess = subprocess.Popen(['dbaccess -e wpdhosp@'+ifxname+' '+lscrip+' &> '+uniname+'_'+lscrip+'.out'],shell=True,stdout=subprocess.PIPE)
            #Inicio
            start = datetime.now()
            dbaccess.communicate()
            end = datetime.now()
            #Inicio

            finaltime = (end - start).total_seconds()
            msglog = 'Instancia: '+ifxname+' Unidade :'+uniname+' Script : '+lscrip+' Hora Inicio :'+str(start)+' Hora Fim :'+str(end)+' TEMPO TOTAL: '+str(finaltime)
            logstimes = subprocess.Popen(['echo '+msglog+' >>'+uniname+'_times.log'],shell=True,stdout=subprocess.PIPE)
            logstimes.communicate()
        #Voltando para o diretorio 
        chdir('..')
    else:
        print('Opcao inexistente')
        exit()

    #Compactando o diretorio
    zipmv = subprocess.Popen(['zip -r '+uniname+'.zip'+' '+uniname],shell=True,stdout=subprocess.PIPE)    
    zipmv.communicate()



#Chamada da funcao main
def main():

    #Validando se os aquvis existem   
    if path.isfile(fargs) and path.isfile(sargs):

        #Abertura do arquivo  de argumentos
        argbases = OpenFile(fargs)
        lbases = argbases.openFile()

        #Abertura do arquivo  de argumentos
        argscrips = OpenFile(sargs)
        lscrips = argscrips.openFile()
      
        with concurrent.futures.ProcessPoolExecutor(max_workers=prll) as executor:
            dbexec = partial(dbaccess,lscrips,modes)
            for unidade, status in zip(lbases, executor.map(dbexec, lbases)):
                print('Processo finalizado para : {}  Status: {}  \n '.format(unidade, status))     
    else:
        print('Erro, um ou mais arquivos nao existem')     
            
if __name__ == '__main__':
    main()