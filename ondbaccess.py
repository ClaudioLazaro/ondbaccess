#!/usr/bin/python3
import argparse
import sys

"""
Objetivo: Execucao de scrips sql no dbaccess, recebendo dois parametros.

Parametos: 
1 - Bases a serem executadas
2 - Sequencia dos Scritps

Output:
1 - Saida padrao dbaccess
    1.1 - Compactacao de saida nominal das unidades em formato zip


Autor: Claudio Lazaro Santos
"""


parser = argparse.ArgumentParser(description='Coolpy, vai te ajudar na execucao de diversos arquivo em lista, serve para Informix ')
parser.add_argument('ffile', type=str, help='Arquivo contendo Bases e Unidades' )
parser.add_argument('sfile', type=str, help='Arquivo contendo Scripts')

#Parametros de Inicializacao 
args = parser.parse_args()

fargs = args.ffile
sargs = args.sfile


class OpenFile (object):
    
    def __init__(self, filename):
        self.filename = filename


    def openFile(self):
        with open(self.filename, "r") as file:
            return file.read().splitlines()


def main():

    arqbases = OpenFile(fargs)
    lbases = arqbases.openFile()

    arqscrips = OpenFile(sargs)
    lscrips = arqscrips.openFile()

    for lbase, lscrip in zip(lbases,lscrips):
        print(lbase.split(",")[1]+"  "+lscrip.split(",")[0])



if __name__ == "__main__":
    main()