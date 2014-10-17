import ckan.lib.helpers as h
from ckan.plugins import SingletonPlugin, implements, IDomainObjectModification,  IConfigurable, toolkit, IResourceUrlChange, IRoutes, IConfigurer
from ckan.lib.dictization.model_dictize import resource_dictize
from ckan.logic import get_action
from ckan.lib.celery_app import celery
from ckan.model.types import make_uuid
from ckan import model,logic
from ckan.lib.base import abort
from ckan.common import _
import json
import ckan
from ckanext.vectorstorer import settings
from db_helpers import DB
from pylons import config

class VectorStorer(SingletonPlugin):
    STATE_DELETED='deleted'
    WMS_FORMAT='wms'
    VECTORSTORER_FORMATS=[WMS_FORMAT]
    
    resource_delete_action= None
    resource_update_action=None
    
    implements(IRoutes, inherit=True)
    implements(IConfigurer, inherit=True)
    implements(IConfigurable, inherit=True)
    implements(IResourceUrlChange)
    implements(IDomainObjectModification, inherit=True)
    

    def configure(self, config):
        
        ''' Extend the resource_delete action in order to get notification of deleted resources'''
        if self.resource_delete_action is None:
            
	    resource_delete = toolkit.get_action('resource_delete')

            @logic.side_effect_free
            def new_resource_delete(context, data_dict):
		resource=ckan.model.Session.query(model.Resource).get(data_dict['id'])
		self.notify(resource,model.domain_object.DomainObjectOperation.deleted)
                res_delete = resource_delete(context, data_dict)
               
                return res_delete
	    logic._actions['resource_delete'] = new_resource_delete
            self.resource_delete_action=new_resource_delete
        
        ''' Extend the resource_update action in order to pass the extra keys to vectorstorer resources
        when they are being updated'''
        if self.resource_update_action is None:
            
	    resource_update = toolkit.get_action('resource_update')

            @logic.side_effect_free
            def new_resource_update(context, data_dict):
		resource=ckan.model.Session.query(model.Resource).get(data_dict['id']).as_dict()
		if resource.has_key('vectorstorer_resource'):
		    if resource['format'].lower()==settings.WMS_FORMAT:
			data_dict['parent_resource_id']=resource['parent_resource_id']
			data_dict['vectorstorer_resource']=resource['vectorstorer_resource']
			data_dict['wms_server']=resource['wms_server']
			data_dict['wms_layer']=resource['wms_layer']
		    if resource['format'].lower()==settings.DB_TABLE_FORMAT:
			data_dict['vectorstorer_resource']=resource['vectorstorer_resource']
			data_dict['parent_resource_id']=resource['parent_resource_id']
			data_dict['geometry']=resource['geometry']
		    
		    if  not data_dict['url']==resource['url']:
			abort(400 , _('You cant upload a file to a '+resource['format']+' resource.'))
                res_update = resource_update(context, data_dict)
               
                return res_update
	    logic._actions['resource_update'] = new_resource_update
            self.resource_update_action=new_resource_update
    
    def before_map(self, map):
	map.connect('{action}', '/dataset/{id}/resource/{resource_id}/{action}/{operation}/',
            controller='ckanext.vectorstorer.controllers.style:StyleController',
            action='{action}')
	map.connect('{action}', '/dataset/{id}/resource/{resource_id}/{action}/{operation}',
            controller='ckanext.vectorstorer.controllers.export:ExportController',
            action='{action}',operation='{operation}')
	map.connect('{action}', '/api/search_epsg',
            controller='ckanext.vectorstorer.controllers.export:ExportController',
            action='search_epsg')
	return map

    def update_config(self, config):

        toolkit.add_public_directory(config, 'public')
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_resource('public', 'ckanext-vectorstorer')    
        
    def notify(self, entity, operation=None):

        if isinstance(entity, model.resource.Resource):
	    
	    if operation==model.domain_object.DomainObjectOperation.new and entity.format.lower() in settings.SUPPORTED_DATA_FORMATS:
		#A new vector resource has been created
		self._create_vector_storer_task(entity)
	    
	    elif operation==model.domain_object.DomainObjectOperation.deleted:
		#A vectorstorer resource has been deleted
		self._delete_vector_storer_task(entity.as_dict())
	    
	    elif operation is None:
		#Resource Url has changed
		
		if entity.format.lower() in settings.SUPPORTED_DATA_FORMATS:
		    #Vector file was updated
		    
		    self._update_vector_storer_task(entity)
		    
		else :
		    #Resource File updated but not in supported formats
		 
		    self._delete_vector_storer_task(entity.as_dict())
		    
	elif isinstance(entity, model.Package):
	    
	    if entity.state==self.STATE_DELETED:
		
		self._pkg_delete_vector_storer_task(entity)
	
    def _get_site_url(self):
	    try:
		return h.url_for_static('/', qualified=True)
	    except AttributeError:
		return config.get('ckan.site_url', '')
	
    def _get_site_user(self):
	    user = get_action('get_site_user')({'model': model,
						'ignore_auth': True,
						'defer_commit': True}, {})
	    return user
	  
    def _get_geoserver_context(self):
	    geoserver_context = json.dumps({
		'geoserver_url': config['ckanext-vectorstorer.geoserver_url'],
		'geoserver_workspace': config['ckanext-vectorstorer.geoserver_workspace'],
		'geoserver_admin': config['ckanext-vectorstorer.geoserver_admin'],
		'geoserver_password': config['ckanext-vectorstorer.geoserver_password'],
		'geoserver_ckan_datastore':config['ckanext-vectorstorer.geoserver_ckan_datastore']
	    })
	    return geoserver_context
	
    def _create_vector_storer_task(self, resource):
	    user=self._get_site_user()
	    resource_package_id=resource.as_dict()['package_id']
	    
	    context = json.dumps({
		'package_id': resource_package_id,
		'site_url': self._get_site_url(),
		'apikey': user.get('apikey'),
		'site_user_apikey': user.get('apikey'),
		'user': user.get('name'),
		'db_params':config['ckan.datastore.write_url']
		
	    })
	    geoserver_context=self._get_geoserver_context()
	    data = json.dumps(resource_dictize(resource, {'model': model}))

	    task_id = make_uuid()
	
	    
	    
	    celery.send_task("vectorstorer.upload",
			    args=[geoserver_context,context, data],
			    task_id=task_id)
	
    def _update_vector_storer_task(self, resource):
	    user=self._get_site_user()
	    resource_package_id=resource.as_dict()['package_id']
	    
	    resource_list_to_delete=self._get_child_resources(resource.as_dict())
	      
	    context = json.dumps({
		'resource_list_to_delete':resource_list_to_delete,
		'package_id': resource_package_id,
		'site_url': self._get_site_url(),
		'apikey': user.get('apikey'),
		'site_user_apikey': user.get('apikey'),
		'user': user.get('name'),
		'db_params':config['ckan.datastore.write_url']
		
	    })
	    
	    geoserver_context = self._get_geoserver_context()
	    data = json.dumps(resource_dictize(resource, {'model': model}))

	    task_id = make_uuid()
	
	    
	    
	    celery.send_task("vectorstorer.update",
			    args=[geoserver_context,context, data],
			    task_id=task_id)
	    
    def _delete_vector_storer_task(self, resource, pkg_delete=False):
	    
	    user=self._get_site_user()
	    
	    data=None
	    
	    resource_list_to_delete=None
	    if (resource['format']==settings.WMS_FORMAT or resource['format']==settings.DB_TABLE_FORMAT) and resource.has_key('vectorstorer_resource'):
		'''A WMS resource which was created from vectorstorer was deleted, so it will only be unpublished from geoserver'''
		
		data = json.dumps(resource)

		if pkg_delete:
		  resource_list_to_delete=self._get_child_resources(resource)
		
		
	    else:
	      
		'''A resource was deleted. Check if this resource has vectorstorer child resources in order to be deleted from datastore 
		and unpublished from geoserver.'''
	      
		data = json.dumps(resource)
		resource_list_to_delete=self._get_child_resources(resource)
		
	    
	    context = json.dumps({
		'resource_list_to_delete':resource_list_to_delete,
		'site_url':self._get_site_url(),
		'apikey': user.get('apikey'),
		'site_user_apikey': user.get('apikey'),
		'user': user.get('name'),
		'db_params':config['ckan.datastore.write_url']
	    })
	    
	    geoserver_context=self._get_geoserver_context()
	  

	    task_id = make_uuid()
	    
	    celery.send_task("vectorstorer.delete",
				args=[geoserver_context,context, data],
				task_id=task_id)
	    
	    if resource.has_key('vectorstorer_resource') and not pkg_delete:
		self._delete_child_resources(resource)   
	
    def _delete_child_resources(self, parent_resource ):

	    user=self._get_site_user()
	    
	    
	    temp_context = {'model': ckan.model,'user': user.get('name')}
	    current_package=get_action('package_show')(temp_context,{'id':parent_resource['package_id']})
	    resources= current_package['resources']
		
	    #Check if the deleted vectorstorer resource has child resources 
	    for child_resource in resources:
		if child_resource.has_key('parent_resource_id'):
		    if child_resource['parent_resource_id']==parent_resource['id']:
			  action_result=logic._actions['resource_delete'](temp_context,{'id':child_resource['id']})
			  
	
    def _get_child_resources(self, parent_resource ):
	    child_resources=[]
	    user=self._get_site_user()
	    
	    temp_context = {'model': ckan.model,'user': user.get('name')}
	    current_package=get_action('package_show')(temp_context,{'id':parent_resource['package_id']})
	    resources= current_package['resources']
		
	    #Check if the deleted vectorstorer resource has child resources 
	    for child_resource in resources:
		if child_resource.has_key('parent_resource_id'):
		    if child_resource['parent_resource_id']==parent_resource['id']:
			  child_resources.append(child_resource['id'])
	    return child_resources
	  
    def _pkg_delete_vector_storer_task(self, package):
	    user= self._get_site_user()
	    context = {'model': ckan.model, 'session': ckan.model.Session,'user': user.get('name')}
	    resources= package.as_dict()['resources']
	    
	    '''Get all vector resources in the deleted dataset which are related to vectorstorer in order to delete them from datastore and unpublish from geoserver.'''
	    for res in resources:
		
		if res.has_key('vectorstorer_resource') and res['format']==settings.DB_TABLE_FORMAT:
		    res['package_id']=package.as_dict()['id']
		    self._delete_vector_storer_task(res, True)
