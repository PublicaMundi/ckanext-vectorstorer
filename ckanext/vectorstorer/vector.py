from osgeo import ogr,osr
from db_helpers import DB



class Shapefile:
    default_epsg=4326
    Database=DB()
    def open_shapefile(self,shp_path,resource_id,db_conn_params):
	driver = ogr.GetDriverByName('ESRI Shapefile')
	dataSource = driver.Open(shp_path, 0)  
	
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
	 
	    fields=self.get_layer_fields(layerDefinition)
	    geom_name=self.get_geometry_name(layer)
	    self.Database.create_table(self.db_table_name,fields,geom_name,srs)
	    self.write_to_db(layer,srs,geom_name)
    
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
			   
    def get_geometry_name(self,layer):
	geometry_names=[]
	for i in range(layer.GetFeatureCount()):
	    feat=layer.GetFeature(i)
	    feat_geom=feat.GetGeometryRef().GetGeometryName()
	    if not feat_geom in geometry_names:
		geometry_names.append(feat_geom)
	geometry_name=None
	for gname in geometry_names:
	    if 'MULTI'in gname.upper():
		geometry_name=gname
	if not geometry_name is None:
	    return geometry_name
	else:
	    return geometry_names[0]
		
    def write_to_db(self,layer,srs,layer_geom_name):
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
		convert_to_multi=self.needs_conversion_to_multi(feat,layer_geom_name)	
		self.Database.insert_to_table(self.db_table_name,feature_fields,feat.GetGeometryRef(),convert_to_multi,srs)
	self.Database.create_spatial_index(self.db_table_name)
	self.Database.commit_and_close()
		
    def needs_conversion_to_multi(self,feat,layer_geom_name):
	if not feat.GetGeometryRef().GetGeometryName()==layer_geom_name:
	   return True
	else:
	   return False
    
    def get_db_table_name(self):
	return self.db_table_name

	
	
