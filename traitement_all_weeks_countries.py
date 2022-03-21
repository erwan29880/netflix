import pandas as pd


class Traitement:

    def __init__(self, df):
        self.df = df  # un dataframe

    
    def selection_des_noms_de_pays(self):
        self.df = self.df[(self.df['country_name']=='Belgium') | (self.df['country_name']=='Germany') | (self.df['country_name']=='Italy') | (self.df['country_name']=='Luxembourg') | (self.df['country_name']=='Spain') | (self.df['country_name']=='Switzerland')]
        return self.df


    def remplacement_nom_par_id(self, ls:str):
        
        
        """
            ls est le nom de la colonne d'un dataframe

            la fonction encode les variables qualitatives en variables numériques
        """

        chaines = self.df[ls].unique()
        numeriques = [x for x in range(1, len(chaines)+1)]

        # création d'un dictionnaire pour remplacer les valeurs avec .map
        remplacement_qual_par_numbr = dict()
        for i, j in zip(chaines, numeriques):
            remplacement_qual_par_numbr[i] = j


        # modifier le dataframe
        self.df[ls] = self.df[ls].map(remplacement_qual_par_numbr)

        return self.df

       


if __name__ == "__main__":

    #importation du fichier csv
    df = pd.read_csv('all-weeks-countries.csv')

    country_name = Traitement(df)

    
    country_name.enregistrer_csv

# #exportation du csv
# df.to_csv('all-weeks-traited.csv', index=False)
