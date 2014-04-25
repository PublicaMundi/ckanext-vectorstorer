import logging
import zipfile
import os
import ckan.lib.helpers as h
from ckan.lib.celery_app import celery 
import urllib2 
import urllib
from ckan.model.types import make_uuid
from  ckan import model
import datetime
from ckan.logic import get_action
from ckan.model import package
from ckan.lib.celery_app import celery

import json

from ckan.logic import action
from vector import Shapefile
import magic
import shutil
from db_helpers import DB
 
 
shapefile_file=''

    
@celery.task(name="vectorstorer.upload", max_retries=24 * 7,
             default_retry_delay=3600)
def vectorstorer_upload( geoserver_cont,cont,data):
    entity = json.loads(data)
    context=json.loads(cont)
    geoserver_context=json.loads(geoserver_cont)
    db_conn_params=context['db_params']
    resource_id= entity['id']
    resource_name=entity['name']
    resource_url= entity['url']
    resource_url=urllib2.unquote(resource_url).decode('utf8')
    url_parts=resource_url.split('/')
     
    resource_file_name= url_parts[len(url_parts)-1]
    
    tmp_folder='/tmp/vectorstorer/'
    resource_tmp_folder=download_and_unzip(resource_url,resource_id,resource_file_name,tmp_folder)
    is_shp,shp_path=is_shapefile(resource_tmp_folder)
    if is_shp:
	shp=Shapefile()
	shp.open_shapefile(shp_path,resource_id,db_conn_params)
	wms_server,wms_layer=publish_layer(geoserver_context,resource_id)
	add_wms_resource(context,resource_id,resource_name,wms_server,wms_layer)
	delete_temp(resource_tmp_folder)
    else:
	return
	

def download_and_unzip(res_url,res_id,res_file_name,tmp_folder):
    resource_tmp_folder=tmp_folder+res_id+'/'
    os.makedirs(resource_tmp_folder)
    zip_url = urllib2.urlopen(res_url)
    downloaded_zip = open(resource_tmp_folder+res_file_name,'wb')
    downloaded_zip.write(zip_url.read())
    downloaded_zip.close()
    zfile = zipfile.ZipFile(resource_tmp_folder+res_file_name)
    zfile.extractall(resource_tmp_folder)
    return resource_tmp_folder

def delete_temp(res_tmp_folder):
    shutil.rmtree(res_tmp_folder)

def is_shapefile(res_folder_path):
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

     
      
         
def publish_layer(geoserver_context,table_name):
    geoserver_public_url= geoserver_context['geoserver_public_url']
    geoserver_local_url= geoserver_context['geoserver_local_url']
    geoserver_workspace= geoserver_context['geoserver_workspace']
    geoserver_admin= geoserver_context['geoserver_admin']
    geoserver_password= geoserver_context['geoserver_password']
    
    url=geoserver_local_url+"/rest/workspaces/"+geoserver_workspace+"/datastores/ckan_datastore_default/featuretypes"
    req = urllib2.Request(url)
    req.add_header("Content-type", "text/xml")
    req.add_data("<featureType><name>"+table_name.lower()+"</name></featureType>")
    req.add_header('Authorization',"Basic " + (geoserver_admin+':'+geoserver_password).encode("base64").rstrip())
 
    res = urllib2.urlopen(req)
    wms_server=geoserver_public_url+"/wms"
    wms_layer=geoserver_workspace+":"+table_name.lower()
    return wms_server,wms_layer
    
def add_wms_resource(context,resource_id,resource_name,wms_server,wms_layer):
    
    api_key=context['apikey'].encode('utf8')
    package_id=context['package_id']
     
    resource = {
	  "package_id":unicode(package_id),
	  "url":wms_server+"?service=WMS&request=GetCapabilities",
	  "format":u'WMS',
	  "from_uuid":unicode(resource_id),
	  "wms_server":unicode(wms_server) ,
	  "wms_layer":unicode(wms_layer),
	  "name":resource_name.split('.')[0]+" WMS Layer",}
    data_string = urllib.quote(json.dumps(resource))
    request = urllib2.Request('http://localhost:5000/api/action/resource_create')
    request.add_header('Authorization', api_key)
    urllib2.urlopen(request, data_string)
    
    


   
