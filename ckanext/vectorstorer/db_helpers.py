import psycopg2
import urlparse  
class DB:    
  
    def setup_connection(self,db_conn_params):
	result = urlparse.urlparse(db_conn_params)
	user = result.username
	password = result.password
	database = result.path[1:]
	hostname = result.hostname
	self.conn = psycopg2.connect(database=database, user=user, password=password,host=hostname)
        self.cursor=self.conn.cursor()
    def check_if_table_exists(self,table_name):
	self.cursor.execute("SELECT * FROM information_schema.tables WHERE table_name='%s'"%table_name)
	table_exists=bool(self.cursor.rowcount)
	if table_exists:
	    return True
	else:
	    return False
    def create_table(self,table_name,fin,geometry,srs):
        self.cursor.execute("CREATE TABLE \"%s\" (_id serial PRIMARY KEY%s);"%(table_name,fin))
        self.cursor.execute("SELECT AddGeometryColumn ('%s','the_geom',%s,'%s',2);"%(table_name,srs,geometry))
        #self.cursor.execute("ALTER TABLE \"%s\" ADD CONSTRAINT enforce_geometry_type CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR geometrytype(the_geom) = 'POLYGON'::text OR the_geom IS NULL);"%(table_name))
        
    
    def insert_to_table(self,table,fields,geometry_text,srs):
	insert=("INSERT INTO \"%s\" VALUES (%s ST_GeomFromText('%s',%s));"%(table,fields,geometry_text,srs)) 
	
	self.cursor.execute(insert)
    
    def create_spatial_index_and_vacuum(self,table):
	#vacuum=("VACUUM \"%s\";"%(table)) 
	#self.cursor.execute(vacuum)
	indexing=("CREATE INDEX \"%s_the_geom_idx\" ON \"%s\" USING GIST(the_geom);"%(table,table)) 
	self.cursor.execute(indexing)
	
    def commit_and_close(self):
	self.conn.commit()
	self.cursor.close()
	self.conn.close()