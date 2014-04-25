from osgeo import ogr,osr
from db_helpers import DB



class Shapefile:
    default_epsg=4326
    Database=DB()
    def open_shapefile(self,shp_path,resource_id,db_conn_params):
	driver = ogr.GetDriverByName('ESRI Shapefile')
	dataSource = driver.Open(shp_path, 0)  
	
	# Check to see if shapefile is found.
	if dataSource is None:
	    print 'Could not open %s' % (shp_path)
	else:
	    layer = dataSource.GetLayer()
	    layer_name=layer.GetName()
	    srs=self.getSRS(layer)
	    self.db_table_name=resource_id.lower()
	    featureCount = layer.GetFeatureCount()
	    layerDefinition = layer.GetLayerDefn()
	    self.Database.setup_connection(db_conn_params)
	 
	    if self.Database.check_if_table_exists(self.db_table_name):
		print "exists"
	    else:
		fields=self.get_layer_fields(layerDefinition)
		geom_name=self.get_geometry_name(layer.GetGeomType())
		self.Database.create_table(self.db_table_name,fields,geom_name,srs)
		self.write_to_db(layer,srs)
		
    def getSRS(self,layer):
	if not layer.GetSpatialRef()==None:
	    prj=layer.GetSpatialRef().ExportToWkt()
	    srs_osr=osr.SpatialReference()
	    srs_osr.ImportFromESRI([prj])
	    
	    epsg=srs_osr.GetAuthorityCode(None)
	    	   
	    if epsg is None or epsg==0:
		epsg= self.default_epsg
	    return epsg
	else:
	    return self.default_epsg
	
    def get_layer_fields(self,layerDefinition):
	fields=''
	for i in range(layerDefinition.GetFieldCount()):
		fname= layerDefinition.GetFieldDefn(i).GetName()
		ftype=layerDefinition.GetFieldDefn(i).GetType()
		if ftype==0:
		    fields+=','+(fname+" "+"integer")
		elif ftype==1:
		    fields+=','+(fname+" "+"integer[]")
		elif ftype==2:
		    fields+=','+(fname+" "+"real")
		elif ftype==3:
		    fields+=','+(fname+" "+"real[]")
		elif ftype==4:
		    fields+=',\"'+(fname+"\" "+"varchar")
		elif ftype==5:
		    fields+=',\"'+(fname+"\" "+"varchar[]")
		elif ftype==6:
		    fields+=','+(fname+" "+"varchar")
		elif ftype==7:
		    fields+=','+(fname+" "+"varchar[]")
		elif ftype==8:
		    fields+=','+(fname+" "+"bytea")
		elif ftype==9:
		    fields+=','+(fname+" "+"date")
		elif ftype==10:
		    fields+=','+(fname+" "+"time without time zone")
		elif ftype==11:
		    fields+=','+(fname+" "+"timestamp without time zone")
	
	return fields
			   
    def get_geometry_name(self,GeomType):
	
	if GeomType==0:
	    return "UNKNOWN"
	elif GeomType==1:
	    return "POINT"
	elif GeomType==2:
	    return "LINESTRING"
	elif GeomType==3:
	    return "POLYGON"
	elif GeomType==4:
	    return "MULTIPOINT"
	elif GeomType==5:
	    return "MULTILINESTRING"
	elif GeomType==6:
	    return "MULTIPOLYGON"
	elif GeomType==7:
	    return "GEOMETRYCOLLECTION"
		
    def write_to_db(self,layer,srs):
	for i in range(layer.GetFeatureCount()):
		feature_fields='%s,'%i
		feat=layer.GetFeature(i)
		for y in range(feat.GetFieldCount()):
		    if not feat.GetField(y)==None:
			if layer.GetLayerDefn().GetFieldDefn(y).GetType()==4:
			    feature_fields+='\''+str(feat.GetField(y)).decode('utf_8')+'\','
			elif layer.GetLayerDefn().GetFieldDefn(y).GetType()==9:
			    feature_fields+='\''+str(feat.GetField(y)).decode('utf_8')+'\','
			else:
			    feature_fields+=str(feat.GetField(y))+','
		    else:
			feature_fields+='NULL,'
			
		self.Database.insert_to_table(self.db_table_name,feature_fields,feat.GetGeometryRef(),srs)
	self.Database.create_spatial_index_and_vacuum(self.db_table_name)
	self.Database.commit_and_close()
		
  
    
    def get_db_table_name(self):
	return self.db_table_name

	
	
