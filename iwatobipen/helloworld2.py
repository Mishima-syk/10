import luigi

class HelloWorld( luigi.Task ):
    def requires( self ):
        return None
    def output( self ):
        return luigi.LocalTarget( "helloword.txt" )
    def run( self ):
        with self.output().open("w") as outfile:
            outfile.write( "hello\n" )

if __name__ == '__main__':
    luigi.run()
