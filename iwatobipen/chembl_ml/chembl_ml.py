from rdkit import Chem
from rdkit.Chem import AllChem
from rdkit.Chem import DataStructs

from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np
import pandas as pd
import luigi
from luigi.contrib.postgres import PostgresQuery
import psycopg2
import pickle
# convert smiles to finger print
def smi2fp( smiles ):
    mol = Chem.MolFromSmiles( smiles )
    fp = AllChem.GetMorganFingerprintAsBitVect( mol, 2 )
    arr = np.zeros( (1,) )
    DataStructs.ConvertToNumpyArray( fp, arr )
    return arr


# generate sql.
class Dataextractor( luigi.Task ):
    #TID 165 is herg
    tid = luigi.Parameter( default = 165 )
    def requires( self ):
        return []
    def output( self ):
        return luigi.LocalTarget( './data/data_tid_{}.csv'.format( self.tid ) )
    def run( self ):
        target = self.tid
        host = '127.0.0.1'
        database = 'chembl_22' # your database name
        user = 'username'
        password = 'your password'
        conn = psycopg2.connect(
                dbname = database,
                user = user,
                host = host,
                password = password)
        cur = conn.cursor()
        query = """SELECT CANONICAL_SMILES,
                                  MOLREGNO,
                                  ACTIVITY_ID,
                                  STANDARD_VALUE,
                                  STANDARD_UNITS FROM ACTIVITIES JOIN ASSAYS USING (ASSAY_ID) \
                                                                      JOIN COMPOUND_STRUCTURES USING (MOLREGNO) \
                                                                      WHERE TID={} AND STANDARD_TYPE='Ki' \
                                                                      AND STANDARD_VALUE IS NOT NULL \
                                                                      AND STANDARD_RELATION = '=' AND CANONICAL_SMILES NOT LIKE '%.%';""".format( self.tid )
        cur.execute( query )
        rows = cur.fetchall()
        df = pd.DataFrame( rows )
        df.columns = [ 'CANONICAL_SMILES', 'MOLREGNO', 'ACTIVITY_ID', 'STANDARD_VALUE', 'STANDARD_UNITS' ]
        df.to_csv( self.output().path, index=False, header =True )

class Vectorizer( luigi.Task ):
    threshold = luigi.FloatParameter( default = 1000 ) #unit nM
    tid = luigi.Parameter( default = 165 )
    def requires( self ):
        return Dataextractor()

    def output( self ):
        return luigi.LocalTarget( './data/vectorized_{}.pickle'.format(  self.tid ) )

    def run( self ):
        inputdf = pd.read_csv( self.input().path, header = 0 )
        X = list( inputdf.CANONICAL_SMILES.apply( smi2fp ) )
        y = np.where( inputdf.STANDARD_VALUE < self.threshold, 1, 0 )
        output = open( self.output().path, 'wb')
        pickle.dump( [X,y], output )
        output.close()

class Splitter( luigi.Task ):
    test_size = luigi.FloatParameter( default = 0.2 )
    tid = luigi.Parameter( default = 165 )
    def requires( self ):
        return Vectorizer()
    def output( self ):
        return [
                 luigi.LocalTarget( "./data/train_splitted_{}.pkl".format( str( self.tid )) ),
                 luigi.LocalTarget( "./data/test_splitted_{}.pkl".format( str( self.tid )) )
                ]
    def run( self ):
        fobj = open( self.input().path, 'rb' )
        X, y = pickle.load( fobj )
        train_x, test_x, train_y, test_y = train_test_split( X, y, test_size = self.test_size )
        output1 = open( self.output()[0].path, "wb" )
        output2 = open( self.output()[1].path, "wb" )
        pickle.dump( [ train_x, train_y ], output1 )
        pickle.dump( [ test_x, test_y ], output2 )
        output1.close()
        output2.close()

class ModelBuilder( luigi.Task ):
    tid = luigi.Parameter( default = 165 )
    def requires( self ):
        return Splitter()
    def output( self ):
        return luigi.LocalTarget( "./data/rf_classifier_{}.pkl".format( str( self.tid ) ))
    def run( self ):
        inputobj = open( self.input()[0].path, 'rb' )
        train_x, train_y = pickle.load( inputobj )
        rfc = RandomForestClassifier()
        rfc.fit( train_x, train_y )
        output = open( self.output().path, 'wb' )
        pickle.dump( rfc, output )
        output.close()

class ModelTester( luigi.Task ):
    tid = luigi.Parameter( default = 165 )
    def requires( self ):
        return ModelBuilder()
    def output( self ):
        return luigi.LocalTarget( "./data/classification_report.txt" )
    def run( self ):
        testobj = open( "./data/test_splitted_{}.pkl".format( str(self.tid) ), 'rb' )
        test_x, test_y = pickle.load( testobj )
        rfc = open( self.input().path, 'rb' )
        rfc = pickle.load( rfc )
        pred_y = rfc.predict( test_x )
        rep = classification_report( test_y, pred_y )
        with self.output().open( 'w' ) as output:
            output.write( rep )

class Predictor( luigi.Task ):
    tid = luigi.Parameter( default = 165 )
    #input file format is smiles\n without header
    inputfile = luigi.Parameter()
    def requires( self ):
        return ModelBuilder()
    def output( self ):
        return luigi.LocalTarget( "./data/predicted.csv" )
    def run( self ):
        rfc = open( self.input().path, 'rb' )
        rfc = pickle.load( rfc )
        smilesdf = pd.read_csv( self.inputfile, header=None )
        X = list( smilesdf[ 0 ].apply( smi2fp ))

        pred_y = rfc.predict( X )
        decode = { 1:"Active", 0:"None_Active"}
        with self.output().open( 'w' ) as output:
            for i in range( len(X) ):
                line = '{},{}\n'.format( smilesdf[0][i], decode[ pred_y[i] ] )
                output.write( line )

if __name__ == '__main__':
    luigi.run()
