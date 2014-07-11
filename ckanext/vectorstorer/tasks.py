import zipfile
import os
import urllib2 
import urllib
from ckan.lib.celery_app import celery
import json
import vector
import shutil
from db_helpers import DB
from settings import TMP_FOLDER, ARCHIVE_FORMATS
from pyunpack import Archive
from geoserver.catalog import Catalog
    
@celery.task(name="vectorstorer.upload", max_retries=24 * 7,
             default_retry_delay=3600)
def vectorstorer_upload( geoserver_cont,cont,data):
    
    resource = json.loads(data)
    context=json.loads(cont)
    geoserver_context=json.loads(geoserver_cont)
    db_conn_params=context['db_params']
    _handle_resource(resource,db_conn_params,context,geoserver_context)

	

def _handle_resource(resource,db_conn_params,context,geoserver_context):
    resource_tmp_folder=_download_resource(resource)
    resource_format=resource['format'].lower()
    if resource_format in ARCHIVE_FORMATS:
	try:
	    tmp_archive=_get_tmp_file_path(resource_tmp_folder,resource)
	    Archive(tmp_archive).extractall(resource_tmp_folder)
	    
	    _handle_vector(vector.SHAPEFILE,resource_tmp_folder,resource,db_conn_params,context,geoserver_context)
	except Exception,error:
	    raise error
	    pass
	
    elif resource_format=='kml':
	try:
	    _handle_vector(vector.KML,resource_tmp_folder,resource,db_conn_params,context,geoserver_context)
	except Exception,error:
	    raise error
    
    elif resource_format=='gml':
	try:
	    _handle_vector(vector.GML,resource_tmp_folder,resource,db_conn_params,context,geoserver_context)
	except Exception,error:
	    raise error
    elif resource_format=='geojson':
	try:
	    _handle_vector(vector.GEOJSON,resource_tmp_folder,resource,db_conn_params,context,geoserver_context)
	except Exception,error:
	    raise error
	    pass
    
    #Delete temp folders created
    _delete_temp(resource_tmp_folder)
      
      
      
def _download_resource(resource):
  
    #Create temp folder for the resource
    resource_tmp_folder=TMP_FOLDER+resource['id']+'/'
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
    
def _handle_vector(gdal_driver,resource_tmp_folder,resource,db_conn_params,context,geoserver_context):
    
    #For ESRI Shapefile
    if gdal_driver==vector.SHAPEFILE:
	is_shp,shp_path=_is_shapefile(resource_tmp_folder)
	if is_shp:
	    vector_layer=vector.Vector()
	    vector_layer.open_file(gdal_driver,shp_path,resource['id'],db_conn_params)
    
    #For KML, GML and GeoJSON
    else:
	#Get the file path 
	file_path=_get_tmp_file_path(resource_tmp_folder,resource)
	vector_layer=vector.Vector()
	vector_layer.open_file(gdal_driver,file_path,resource['id'],db_conn_params)
    
    wms_server,wms_layer=_publish_layer(geoserver_context, resource)
    _update_resource_metadata(context,resource)
    _add_wms_resource(context, resource, wms_server, wms_layer)


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
    resource_name=resource['name'].split('.')[0]
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

def _update_resource_metadata(context,resource):
    api_key=context['apikey'].encode('utf8')
    site_url=context['site_url']
    resource['vectorstorer_resource']=True
    data_string = urllib.quote(json.dumps(resource))
    request = urllib2.Request(site_url+'api/action/resource_update')
    request.add_header('Authorization', api_key)
    urllib2.urlopen(request, data_string)
    
def _add_wms_resource(context, parent_resource, wms_server, wms_layer):
    
    api_key=context['apikey'].encode('utf8')
    site_url=context['site_url']
    package_id=context['package_id']
     
    resource = {
	  "package_id":unicode(package_id),
	  "url":wms_server+"?service=WMS&request=GetCapabilities",
	  "format":u'WMS',
	  "from_uuid":unicode(parent_resource['id']),
	  'vectorstorer_resource':True,
	  "wms_server":unicode(wms_server) ,
	  "wms_layer":unicode(wms_layer),
	  "name":parent_resource['name'].split('.')[0]+" WMS Layer",
	  "description":parent_resource['description']}
    
    data_string = urllib.quote(json.dumps(resource))
    request = urllib2.Request(site_url+'api/action/resource_create')
    request.add_header('Authorization', api_key)
    urllib2.urlopen(request, data_string)
    
    
@celery.task(name="vectorstorer.delete", max_retries=24 * 7,
             default_retry_delay=3600)
def vectorstorer_delete( geoserver_cont,cont,data):
    
    resource = json.loads(data)
    context=json.loads(cont)
    geoserver_context=json.loads(geoserver_cont)
    db_conn_params=context['db_params']
    if resource:
	_delete_from_datastore(resource,db_conn_params,context)
	_delete_vectorstorer_resources(resource,context)
    else:
       	_unpublish_from_geoserver(context['vector_storer_resources_ids'][0],geoserver_context)


def _delete_from_datastore(resource,db_conn_params,context):
  
    Database=DB()
    Database.setup_connection(db_conn_params)
    Database.drop_table(resource['id'])
    Database.commit_and_close()
    
def _unpublish_from_geoserver(resource_id,geoserver_context):
    geoserver_url= geoserver_context['geoserver_url']
    geoserver_admin= geoserver_context['geoserver_admin']
    geoserver_password= geoserver_context['geoserver_password']
    cat = Catalog(geoserver_url+"/rest", username=geoserver_admin, password=geoserver_password)
    layer = cat.get_layer(resource_id.lower())
    cat.delete(layer)
    
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
      