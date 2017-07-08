import luigi
import os
from rdkit import Chem

class SaveSDF( luigi.Task ):
    def requires( self ):
        return None
    def run( self ):
        print( os.getcwd() )
        sdf = Chem.SDWriter( "out.sdf" )
        mol = Chem.MolFromSmiles( "c1ccccc1" )
        sdf.write( mol )
    def output( self ):
        return luigi.LocalTarget( "out.sdf" )

class ReadSDF( luigi.Task ):
    def requires( self ):
        return SaveSDF()
    def output( self ):
        return luigi.LocalTarget( "calcparam.txt" )
    def run( self ):
        with self.output().open("w") as outfile:
            sdf = Chem.SDMolSupplier( "out.sdf" )
            mol = sdf[0]
            smi = Chem.MolToSmiles( mol ) 
            outfile.write( smi )
            #outfile.write( "test" )


if __name__ == "__main__":
    luigi.run()

