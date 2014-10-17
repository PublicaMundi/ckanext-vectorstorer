import ckan.lib.helpers as h
from ckan.lib.dictization.model_dictize import resource_dictize
from ckan.logic import get_action
from ckan.lib.celery_app import celery
from ckan.model.types import make_uuid
from ckan import model,logic
from ckan.lib.base import abort
from ckan.common import _
import json
import ckan
from pylons import config
from ckanext.vectorstorer import settings


def _get_site_url():
	try:
	    return h.url_for_static('/', qualified=True)
	except AttributeError:
	    return config.get('ckan.site_url', '')
    
def _get_site_user():
	user = get_action('get_site_user')({'model': model,
					    'ignore_auth': True,
					    'defer_commit': True}, {})
	return user
      
def _get_geoserver_context():
	geoserver_context = json.dumps({
	    'geoserver_url': config['ckanext-vectorstorer.geoserver_url'],
	    'geoserver_workspace': config['ckanext-vectorstorer.geoserver_workspace'],
	    'geoserver_admin': config['ckanext-vectorstorer.geoserver_admin'],
	    'geoserver_password': config['ckanext-vectorstorer.geoserver_password'],
	    'geoserver_ckan_datastore':config['ckanext-vectorstorer.geoserver_ckan_datastore']
	})
	return geoserver_context
    
def create_vector_storer_task(resource):
	user = _get_site_user()
	resource_package_id=resource.as_dict()['package_id']
	
	context = json.dumps({
	    'package_id': resource_package_id,
	    'site_url': _get_site_url(),
	    'apikey': user.get('apikey'),
	    'site_user_apikey': user.get('apikey'),
	    'user': user.get('name'),
	    'db_params':config['ckan.datastore.write_url']
	    
	})
	geoserver_context=_get_geoserver_context()
	data = json.dumps(resource_dictize(resource, {'model': model}))

	task_id = make_uuid()
    
	
	
	celery.send_task("vectorstorer.upload",
			args = [geoserver_context,context, data],
			task_id = task_id)
    
def update_vector_storer_task(resource):
	user = _get_site_user()
	resource_package_id = resource.as_dict()['package_id']
	
	resource_list_to_delete = _get_child_resources(resource.as_dict())
	  
	context = json.dumps({
	    'resource_list_to_delete':resource_list_to_delete,
	    'package_id': resource_package_id,
	    'site_url': _get_site_url(),
	    'apikey': user.get('apikey'),
	    'site_user_apikey': user.get('apikey'),
	    'user': user.get('name'),
	    'db_params':config['ckan.datastore.write_url']
	    
	})
	
	geoserver_context = _get_geoserver_context()
	data = json.dumps(resource_dictize (resource, {'model': model}) )

	task_id = make_uuid()
    
	
	
	celery.send_task("vectorstorer.update",
			args=[geoserver_context,context, data],
			task_id=task_id)
	
def delete_vector_storer_task(resource, pkg_delete=False):
	
	user = _get_site_user()
	
	data=None
	
	resource_list_to_delete=None
	if (resource['format'] == settings.WMS_FORMAT or resource['format'] == settings.DB_TABLE_FORMAT) and resource.has_key('vectorstorer_resource'):
	    '''A WMS resource which was created from vectorstorer was deleted, so it will only be unpublished from geoserver'''
	    
	    data = json.dumps(resource)

	    if pkg_delete:
	      resource_list_to_delete = _get_child_resources(resource)
	    
	    
	else:
	  
	    '''A resource was deleted. Check if this resource has vectorstorer child resources in order to be deleted from datastore 
	    and unpublished from geoserver.'''
	  
	    data = json.dumps(resource)
	    resource_list_to_delete = _get_child_resources(resource)
	    
	
	context = json.dumps({
	    'resource_list_to_delete': resource_list_to_delete,
	    'site_url': _get_site_url(),
	    'apikey': user.get('apikey'),
	    'site_user_apikey': user.get('apikey'),
	    'user': user.get('name'),
	    'db_params': config['ckan.datastore.write_url']
	})
	
	geoserver_context=_get_geoserver_context()
      

	task_id = make_uuid()
	
	celery.send_task("vectorstorer.delete",
			    args=[geoserver_context,context, data],
			    task_id=task_id)
	
	if resource.has_key('vectorstorer_resource') and not pkg_delete:
	    _delete_child_resources(resource)   
    
def _delete_child_resources(parent_resource):

	user = _get_site_user()
	
	
	temp_context = {'model': ckan.model,'user': user.get('name')}
	current_package=get_action('package_show')(temp_context,{'id':parent_resource['package_id']})
	resources= current_package['resources']
	    
	#Check if the deleted vectorstorer resource has child resources 
	for child_resource in resources:
	    if child_resource.has_key('parent_resource_id'):
		if child_resource['parent_resource_id']==parent_resource['id']:
		      action_result=logic._actions['resource_delete'](temp_context,{'id':child_resource['id']})
		      
    
def _get_child_resources(parent_resource ):
	child_resources=[]
	user = _get_site_user()
	
	temp_context = {'model': ckan.model,'user': user.get('name')}
	current_package=get_action('package_show')(temp_context,{'id':parent_resource['package_id']})
	resources= current_package['resources']
	    
	#Check if the deleted vectorstorer resource has child resources 
	for child_resource in resources:
	    if child_resource.has_key('parent_resource_id'):
		if child_resource['parent_resource_id']==parent_resource['id']:
		      child_resources.append(child_resource['id'])
	return child_resources
      
def pkg_delete_vector_storer_task(package):
	user = _get_site_user()
	context = {'model': ckan.model, 'session': ckan.model.Session,'user': user.get('name')}
	resources = package.as_dict()['resources']
	
	'''Get all vector resources in the deleted dataset which are related to vectorstorer in order to delete them from datastore and unpublish from geoserver.'''
	for res in resources:
	    
	    if res.has_key('vectorstorer_resource') and res['format'] == settings.DB_TABLE_FORMAT:
		res['package_id'] = package.as_dict()['id']
		delete_vector_storer_task(res, True)
