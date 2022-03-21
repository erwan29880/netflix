import pandas as pd
from connexion import Connexion
import os

class Nettoyage_csv(Connexion):

    def __init__(self, df):
        super().__init__()
        self.df = pd.read_csv(df)


    def traitement(self):

        #remplacer les NaN par un blanc
        df = self.df.fillna('')

        # mettre les données en liste de tuples pour insertion dans bdd
        liste = []
        for j in range(df.shape[0]):
            lis = []
            for i in range(df.shape[1]):
                a = df.iloc[j,i]           # assignation à une variable plus courte à écrire

                try:   
                    lis.append(a.item())   # parse les numpy.int64 non acceptés par mysql en int
                except:
                    lis.append(a)
            
            liste.append(tuple(lis))
        
        # self.req(liste, commit=True)
        return liste
    

        
    def insertion_donnees(self, sql):
        
        donnees = self.traitement()

        self.req_many(sql, donnees)



    
    

if __name__ == '__main__':

    dossier = os.getcwd()
    path = os.path.join(dossier,'Téléchargements','all-weeks-countries.csv')

    cl = Nettoyage_csv(path).traitement()
    cl.insertion_donnees()
    # print(cl)