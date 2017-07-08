import luigi

class HelloWorldTask( luigi.Task ):
    task_namespace = 'examples'
    def requires( self ):
        return WorldIsFineTask()
    def run( self ):
        print( "{task} says: Hello world!".format( task = self.__class__.__name__ ))

class WorldIsFineTask( luigi.Task ):
    task_namespace = 'examples'
    def run( self ):
        with self.output().open( 'w' ) as output:
            output.write( ' {task} says: The world is a fine place!\n'.format( task = self.__class__.__name__ ))
    def output( self ):
        return luigi.LocalTarget( "world.txt" )

if __name__ == '__main__':
    luigi.run()

