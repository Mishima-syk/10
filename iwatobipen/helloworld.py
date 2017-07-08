import luigi

class HelloWorldTask( luigi.Task):
    task_namespace = 'examples'
    def run( self ):
        print( "{task} says: Hellow world!".format( task=self.__class__.__name__))

if __name__ == '__main__':
    luigi.run()

