#!/usr/bin/python3
"""
Sevidor amil : /usr/local/bin/python3.7

Objetivo: Execucao de scrips sql no dbaccess, recebendo dois parametros.

Parametos: 
1 - Bases a serem executadas
    Exemplo do conteudo do arquivo: Instancias,Undiades
2 - Sequencia dos Scritps
    Exemplo do conteudo do arquivo: script_tools.sql

Output:
1 - Saida padrao dbaccess
    1.1 - Compactacao de saida nominal das unidades em formato zip


Autor: Claudio Lazaro Santos
"""

"""
Importes full de modulos
"""
import argparse 
import subprocess

"""
Importes parcial de modulos
"""
from os import mkdir, path, getenv, chdir
from zipfile import ZipFile
from datetime import datetime


parser = argparse.ArgumentParser(description='Coolpy, vai te ajudar na execucao de diversos arquivo em lista, serve para Informix ')
parser.add_argument('ffile', type=str, help='Arquivo contendo Bases e Unidades' )
parser.add_argument('sfile', type=str, help='Arquivo contendo Scripts')

#Parametros de Inicializacao 
args = parser.parse_args()

fargs = args.ffile
sargs = args.sfile

#Classe de abertura de Arquivos
class OpenFile (object):
    
    def __init__(self, filename):
        self.filename = filename


    def openFile(self):
        with open(self.filename, "r") as file:
            return file.read().splitlines()


#Classe implementada mais nao sera usada agora, servira apra descompactacao dos arquivos.
class ZipPress(object):
    
    def __init__(self, filename):
        self.filename = filename
    
    def unzip(self):
        with ZipFile(self.filename, 'r') as unzipObj:
            # Extract all the contents of zip file in current directory
            unzipObj.extractall()
        print('Unzip file '+self.filename+' finalizado')

    def zip(self):
        with ZipFile(self.filename+'.zip', 'w') as zipObj:
            # Extract all the contents of zip file in current directory
            zipObj.write(self.filename)
        print('Zip file '+self.filename+' finalizado')


class dbaccess(object):

    def __init__(self, lscrips):
        self.lscrips = lscrips
    
    def order(self):

        while len(self.lscrips) > 0:
            for i in range(len(self.lscrips)):
                print(i)

    pass



def main():


    if path.isfile(fargs) and path.isfile(sargs):

        arqbases = OpenFile(fargs)
        lbases = arqbases.openFile()

        arqscrips = OpenFile(sargs)
        lscrips = arqscrips.openFile()

        now = datetime.now()

        for lbase in lbases:
            print('#########################################################################')

            print('Iniciando Processo :'+lbase.split(",")[1]+' Data Hora '+str(now))
            
            print('Criando Diretorio :'+lbase.split(",")[1])
            
            mkdir(lbase.split(",")[1])
            
            cpfilesql = subprocess.Popen(['cp *.sql '+lbase.split(",")[1]],shell=True,stdout=subprocess.PIPE)
            
            cpfilesql.communicate()            
            
            chdir(lbase.split(",")[1])

 
            
            for lscrip in lscrips:     
                print('Executando :: dbacess -e wpdhosp@'+lbase.split(",")[0] +" "+ lscrip)
                
                dbaccess = subprocess.Popen(['dbaccess -e wpdhosp@'+lbase.split(",")[0]+' '+lscrip+' &> '+lbase.split(",")[1]+'_'+lscrip.split('.')[0]+'.out'],shell=True,stdout=subprocess.PIPE)
                
                dbaccess.communicate()
                       
            
            print('Compactando...'+lbase.split(",")[1]+'.zip')    
            
            chdir('..')

            zipmv = subprocess.Popen(['zip '+lbase.split(",")[1]+'.zip'+' '+lbase.split(",")[1]],shell=True,stdout=subprocess.PIPE)    
            
            zipmv.communicate()
            
            print('Finalizando Processo :'+lbase.split(",")[1]+' Data Hora '+str(now))            
            
            print('#########################################################################')
    else:
        print('Erro, um ou mais arquivos nao existem')



if __name__ == "__main__":
    main()
#'mv *.out '+lbase.split(",")[1]+' && '+