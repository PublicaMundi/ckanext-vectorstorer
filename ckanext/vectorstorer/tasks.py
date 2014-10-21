import zipfile
import os
import urllib2 
import urllib
from ckan.lib.celery_app import celery
import json
import vector
import shutil
from db_helpers import DB
from pyunpack import Archive
from geoserver.catalog import Catalog
from resources import *
from ckanext.vectorstorer import settings

#Define Actions
RESOURCE_CREATE_ACTION='resource_create'
RESOURCE_UPDATE_ACTION='resource_update'
RESOURCE_DELETE_ACTION='resource_delete'


@celery.task(name="vectorstorer.upload", max_retries=24 * 7,
             default_retry_delay=3600)
def vectorstorer_upload( geoserver_cont,cont,data):
    
    resource = json.loads(data)
    context=json.loads(cont)
    geoserver_context=json.loads(geoserver_cont)
    db_conn_params=context['db_params']
    _handle_resource(resource,db_conn_params,context,geoserver_context)
    #wms_server,wms_layer=_publish_layer(geoserver_context, resource)
    #_update_resource_metadata(context,resource)
    #_add_wms_resource(context, resource, wms_server, wms_layer)
	

def _handle_resource(resource,db_conn_params,context,geoserver_context):
    resource_tmp_folder=_download_resource(resource)
    resource_format=resource['format'].lower()
    
    _GDAL_DRIVER=None
    _file_path=None
    
    if resource_format in settings.ARCHIVE_FORMATS:
	tmp_archive=_get_tmp_file_path(resource_tmp_folder,resource)
	Archive(tmp_archive).extractall(resource_tmp_folder)
	is_shp,_file_path=_is_shapefile(resource_tmp_folder)
	if is_shp:
	    _GDAL_DRIVER=vector.SHAPEFILE	
    elif resource_format=='kml':
	_GDAL_DRIVER=vector.KML
    elif resource_format=='gml':
	_GDAL_DRIVER=vector.GML
    elif resource_format=='gpx':
	_GDAL_DRIVER=vector.GPX
    elif resource_format=='geojson' or resource_format=='json':
	_GDAL_DRIVER=vector.GEOJSON
    elif resource_format=='sqlite':
	_GDAL_DRIVER=vector.SQLITE
    elif resource_format=='geopackage' or resource_format=='gpkg': 
	_GDAL_DRIVER=vector.GEOPACKAGE
    elif resource_format=='csv':
	_GDAL_DRIVER=vector.CSV      
    elif resource_format=='xls' or resource_format=='xlsx':
	_GDAL_DRIVER=vector.XLS
    
    if not _GDAL_DRIVER==vector.SHAPEFILE:
	_file_path=_get_tmp_file_path(resource_tmp_folder,resource)  
    
    if context.has_key('encoding'):
	_encoding = context['encoding']
    else:
	_encoding = 'utf-8'
    
    if _GDAL_DRIVER:
	_vector=vector.Vector(_GDAL_DRIVER,_file_path,_encoding, db_conn_params)
	layer_count=_vector.get_layer_count()
	
	for layer_idx in range(0,layer_count):
	     
	    _handle_vector(_vector, layer_idx, resource, context, geoserver_context)
	 
    #Delete temp folders created
    _delete_temp(resource_tmp_folder)
      
def _download_resource(resource):
  
    #Create temp folder for the resource
    resource_tmp_folder=settings.TMP_FOLDER+resource['id']+'/'
    os.makedirs(resource_tmp_folder)
    
    #Get resource URL and resource file name
    resource_url=urllib2.unquote(resource['url']).decode('utf8')
    url_parts=resource_url.split('/')
    resource_file_name= url_parts[len(url_parts)-1]
    
    #Download resource in the temp folder
    resource_download_request= urllib2.urlopen(resource_url)
    downloaded_resource = open(resource_tmp_folder+resource_file_name,'wb')
    downloaded_resource.write(resource_download_request.read())
    downloaded_resource.close()
    
    return resource_tmp_folder

def _get_tmp_file_path(resource_tmp_folder,resource):
    resource_url=urllib2.unquote(resource['url']).decode('utf8')
    url_parts=resource_url.split('/')
    resource_file_name= url_parts[len(url_parts)-1]
    file_path=resource_tmp_folder+resource_file_name
    return file_path
    
def _handle_vector(_vector, layer_idx, resource, context, geoserver_context):
    layer = _vector.get_layer(layer_idx)
    if layer and layer.GetFeatureCount()>0:
	layer_name=layer.GetName()
	geom_name=_vector.get_geometry_name(layer)
	
	created_db_table_resource=_add_db_table_resource(context,resource,geom_name,layer_name)
	
	layer = _vector.get_layer(layer_idx)
	_vector.handle_layer(layer, geom_name, created_db_table_resource['id'].lower())

	wms_server,wms_layer=_publish_layer(geoserver_context, created_db_table_resource)

	_add_wms_resource(context,layer_name, created_db_table_resource, wms_server, wms_layer)
  

def _add_db_table_resource(context,resource,geom_name,layer_name):
    db_table_resource=DBTableResource(context['package_id'], layer_name,resource['description'], resource['id'], resource['url'], geom_name)
    db_res_as_dict=db_table_resource.get_as_dict()
    created_db_table_resource=_api_resource_action(context,db_res_as_dict,RESOURCE_CREATE_ACTION)
    return created_db_table_resource

def _add_wms_resource(context, layer_name, parent_resource, wms_server, wms_layer):
    wms_resource=WMSResource(context['package_id'], layer_name, parent_resource['description'], parent_resource['id'], wms_server,wms_layer)
    wms_res_as_dict=wms_resource.get_as_dict()
    created_wms_resource=_api_resource_action(context,wms_res_as_dict,RESOURCE_CREATE_ACTION)
    return created_wms_resource
    
def _delete_temp(res_tmp_folder):
    shutil.rmtree(res_tmp_folder)

def _is_shapefile(res_folder_path):
    shp_exists=False
    shx_exists=False
    dbf_exists=False
    prj_exists=False
    for file in os.listdir(res_folder_path):
	if file.lower().endswith(".shp"):
	    shapefile_path=res_folder_path+file
	    shp_exists=True
	elif file.lower().endswith(".shx"):
	    shx_exists=True 
	elif file.lower().endswith(".dbf"):
	    dbf_exists=True
    if shp_exists and shx_exists and dbf_exists:
	return True,shapefile_path
    else:
	return False,None

     
      
         
def _publish_layer(geoserver_context,resource):
    geoserver_url= geoserver_context['geoserver_url']
    geoserver_workspace= geoserver_context['geoserver_workspace']
    geoserver_admin= geoserver_context['geoserver_admin']
    geoserver_password= geoserver_context['geoserver_password']
    geoserver_ckan_datastore= geoserver_context['geoserver_ckan_datastore']
    
    resource_id=resource['id'].lower()
    resource_name=resource['name']
    
    if DBTableResource.name_extention in resource_name:
	resource_name=resource_name.replace(DBTableResource.name_extention,'')
    resource_description=resource['description']
    
    url=geoserver_url+"/rest/workspaces/"+geoserver_workspace+"/datastores/"+geoserver_ckan_datastore+"/featuretypes"
    req = urllib2.Request(url)
    req.add_header("Content-type", "text/xml")
    req.add_data("<featureType><name>%s</name><title>%s</title><abstract>%s</abstract></featureType>"%(resource_id,resource_name,resource_description))
    req.add_header('Authorization',"Basic " + (geoserver_admin+':'+geoserver_password).encode("base64").rstrip())
 
    res = urllib2.urlopen(req)
    wms_server=geoserver_url+"/wms"
    wms_layer=geoserver_workspace+":"+resource_id
    
    return wms_server,wms_layer

def _api_resource_action(context,resource,action):
    api_key=context['apikey'].encode('utf8')
    site_url=context['site_url']
    data_string = urllib.quote(json.dumps(resource))
    request = urllib2.Request(site_url+'api/action/'+action)
    request.add_header('Authorization', api_key)
    response=urllib2.urlopen(request, data_string)
    created_resource=json.loads(response.read())['result']
    return created_resource

def _update_resource_metadata(context,resource):
    api_key=context['apikey'].encode('utf8')
    site_url=context['site_url']
    resource['vectorstorer_resource']=True
    data_string = urllib.quote(json.dumps(resource))
    request = urllib2.Request(site_url+'api/action/resource_update')
    request.add_header('Authorization', api_key)
    urllib2.urlopen(request, data_string)
    

    
@celery.task(name="vectorstorer.update", max_retries=24 * 7,
             default_retry_delay=3600)
def vectorstorer_update( geoserver_cont,cont,data):
    
    resource = json.loads(data)
    context=json.loads(cont)
    geoserver_context=json.loads(geoserver_cont)
    db_conn_params=context['db_params']
    
    resource_ids=context['resource_list_to_delete']
    
    '''If resource has child resources (WMS) unpublish from geoserver.Else the 
    WMS resource has already been deleted and the layer unpublished from geoserver'''
    
    if len(resource_ids)>0:
	for res_id in resource_ids:
	    res = {
	    "id":res_id}
	    try:
		_api_resource_action(context,res,RESOURCE_DELETE_ACTION)
	    except urllib2.HTTPError as e:
		print e.reason 
    
    _handle_resource(resource,db_conn_params,context,geoserver_context)
    
@celery.task(name="vectorstorer.delete", max_retries=24 * 7,
             default_retry_delay=3600)
def vectorstorer_delete( geoserver_cont,cont,data):
    
    resource = json.loads(data)
    context=json.loads(cont)
    geoserver_context=json.loads(geoserver_cont)
    db_conn_params=context['db_params']
    
    if resource.has_key('format'):
	if resource['format']==settings.DB_TABLE_FORMAT :
	    _delete_from_datastore(resource['id'],db_conn_params,context)
	  
	elif resource['format']==settings.WMS_FORMAT :
	    _unpublish_from_geoserver(resource['parent_resource_id'],geoserver_context)
    resource_ids=context['resource_list_to_delete']  
    if resource_ids:
	resource_ids=context['resource_list_to_delete']
	
	for res_id in resource_ids:
	    res = {
	    "id":res_id}
	    _api_resource_action(context,res,RESOURCE_DELETE_ACTION)

def _delete_from_datastore(resource_id,db_conn_params,context):
  
    _db=DB(db_conn_params)
    _db.drop_table(resource_id)
    _db.commit_and_close()
    
def _unpublish_from_geoserver(resource_id,geoserver_context):
    geoserver_url= geoserver_context['geoserver_url']
    geoserver_admin= geoserver_context['geoserver_admin']
    geoserver_password= geoserver_context['geoserver_password']
    cat = Catalog(geoserver_url+"/rest", username=geoserver_admin, password=geoserver_password)
    layer = cat.get_layer(resource_id.lower())
    cat.delete(layer)
    cat.reload()
    
def _delete_vectorstorer_resources(resource,context):
    resources_ids_to_delete=context['vector_storer_resources_ids']
    api_key=context['apikey'].encode('utf8')
    site_url=context['site_url']
    for res_id in resources_ids_to_delete:
     
	resource = {
	    "id":res_id}
	data_string = urllib.quote(json.dumps(resource))
	request = urllib2.Request(site_url+'api/action/resource_delete')
	request.add_header('Authorization', api_key)
	urllib2.urlopen(request, data_string)
      