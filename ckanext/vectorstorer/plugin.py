import ckan.lib.helpers as h
from ckan.plugins import SingletonPlugin, implements, IDomainObjectModification,  IConfigurable, toolkit, IResourceUrlChange
from ckan.lib.dictization.model_dictize import resource_dictize
from ckan.logic import get_action
from ckan.lib.celery_app import celery
from ckan.model.types import make_uuid
from ckan import model,logic
import json
import ckan
from settings import SUPPORTED_DATA_FORMATS
from db_helpers import DB



class VectorStorer(SingletonPlugin):
    
    STATE_DELETED='deleted'
    WMS_FORMAT='wms'
    VECTORSTORER_FORMATS=[WMS_FORMAT]
    
    resource_delete_action= None
    resource_update_action=None
    
    
    implements(IConfigurable, inherit=True)
    implements(IResourceUrlChange)
    implements(IDomainObjectModification, inherit=True)
    

    def configure(self, config):
        self.config=config
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
		if resource.has_key('vectorstorer_resource') and resource['format'].lower()==self.WMS_FORMAT:
		    data_dict['from_uuid']=resource['from_uuid']
		    data_dict['vectorstorer_resource']=resource['vectorstorer_resource']
		    data_dict['wms_server']=resource['wms_server']
		    data_dict['wms_layer']=resource['wms_layer']
		elif resource.has_key('vectorstorer_resource') and resource['format'].lower() in SUPPORTED_DATA_FORMATS:
		    data_dict['vectorstorer_resource']=resource['vectorstorer_resource']
		 
                res_update = resource_update(context, data_dict)
               
                return res_update
	    logic._actions['resource_update'] = new_resource_update
            self.resource_update_action=new_resource_update
        
    def notify(self, entity, operation=None):

        if isinstance(entity, model.resource.Resource):
	    
	    if operation==model.domain_object.DomainObjectOperation.new and entity.format.lower() in SUPPORTED_DATA_FORMATS:
		#A new vector resource has been created
		self._create_vector_storer_task(entity)
	    
	    elif operation==model.domain_object.DomainObjectOperation.deleted and entity.extras.has_key('vectorstorer_resource'):
		#A vectorstorer resource has been deleted
		self._delete_vector_storer_task(entity.as_dict())
	    
	    elif operation is None:
		#Resource Url has changed
		
		if entity.format.lower() in SUPPORTED_DATA_FORMATS:
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
            return self.config.get('ckan.site_url', '')
    
    def _get_site_user(self):
	user = get_action('get_site_user')({'model': model,
                                            'ignore_auth': True,
                                            'defer_commit': True}, {})
	return user
      
    def _get_geoserver_context(self):
	geoserver_context = json.dumps({
	    'geoserver_url': self.config['ckanext-vectorstorer.geoserver_url'],
            'geoserver_workspace': self.config['ckanext-vectorstorer.geoserver_workspace'],
            'geoserver_admin': self.config['ckanext-vectorstorer.geoserver_admin'],
            'geoserver_password': self.config['ckanext-vectorstorer.geoserver_password'],
	    'geoserver_ckan_datastore': self.config['ckanext-vectorstorer.geoserver_ckan_datastore']
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
            'db_params':self.config['ckan.datastore.write_url']
            
        })
	geoserver_context=self._get_geoserver_context()
        data = json.dumps(resource_dictize(resource, {'model': model}))

        task_id = make_uuid()
     
	
        
        celery.send_task("vectorstorer.upload",
                         args=[geoserver_context,context, data],
                         task_id=task_id)
    
    def _update_vector_storer_task(self, resource):
	
	#First deleted from datastorer the current vector resource and delete-unpublish child resources
        self._delete_vector_storer_task(resource.as_dict())
        
        #Secondly handle the new vector resource (create datastore table), create child resources and publish it to geoserver
	self._create_vector_storer_task(resource)
	
    def _delete_vector_storer_task(self, resource):
	
	user=self._get_site_user()
	
	vector_storer_resources_ids=[]
	
	if resource['format']==self.WMS_FORMAT:
	    '''A WMS resource which was created from vectorstorer was deleted, so it will only be unpublished from geoserver'''
	    
	    vector_storer_resources_ids.append(resource['from_uuid'])
	    parent_vector_resource=None
	
	else:
	    '''A Vector resource which was created from vectorstorer was deleted, so it will be deleted from datastore and unpublished from geoserver.
	    Child resources will also be deleted form CKAN '''
	    
	    parent_vector_resource=resource
	    #Get current package resources
	    resource_package_id=resource['package_id']
	    temp_context = {'model': ckan.model,'user': user.get('name')}
	    current_package=get_action('package_show')(temp_context,{'id':resource_package_id})
	    resources= current_package['resources']
	    
	    #Check if the deleted vectorstorer resource has child resources 
	    for res in resources:
		if res['format']==self.WMS_FORMAT and res['from_uuid']==resource['id']:
		    
		    #Child resource was found so append it to a list, in order to be deleted
		    vector_storer_resources_ids.append(res['id'])
	 
        

	context = json.dumps({
	    'vector_storer_resources_ids': vector_storer_resources_ids,
            'site_url': self._get_site_url(),
            'apikey': user.get('apikey'),
            'site_user_apikey': user.get('apikey'),
            'user': user.get('name'),
            'db_params':self.config['ckan.datastore.write_url']
            
        })
	
	geoserver_context=self._get_geoserver_context()
        data = json.dumps(parent_vector_resource)

        task_id = make_uuid()
     
	
        
        celery.send_task("vectorstorer.delete",
                         args=[geoserver_context,context, data],
                         task_id=task_id)

    def _pkg_delete_vector_storer_task(self, package):
	
	user= self._get_site_user()
	context = {'model': ckan.model, 'session': ckan.model.Session,'user': user.get('name')}
	resources= package.as_dict()['resources']
	
	'''Get all vector resources in the deleted dataset which are related to vectorstorer in order to delete them from datastore and unpublish from geoserver.'''
	for res in resources:
	         if res['format'].lower() in SUPPORTED_DATA_FORMATS and res.has_key('vectorstorer_resource'):
		      self._delete_vector_storer_task(res)
