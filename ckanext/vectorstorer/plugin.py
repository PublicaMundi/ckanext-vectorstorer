from ckan.plugins import SingletonPlugin, implements, IDomainObjectModification,  IConfigurable, toolkit, IResourceUrlChange, IRoutes, IConfigurer
from ckan import model,logic
from ckan.lib.base import abort
from ckan.common import _
import ckan
from ckanext.vectorstorer import settings
from ckanext.vectorstorer import resource_actions
from pylons import config


class VectorStorer(SingletonPlugin):
    STATE_DELETED='deleted'
    
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
		resource_actions.create_vector_storer_task(entity)
	    
	    elif operation==model.domain_object.DomainObjectOperation.deleted:
		#A vectorstorer resource has been deleted
		resource_actions.delete_vector_storer_task(entity.as_dict())
	    
	    elif operation is None:
		#Resource Url has changed
		
		if entity.format.lower() in settings.SUPPORTED_DATA_FORMATS:
		    #Vector file was updated
		    
		    resource_actions.update_vector_storer_task(entity)
		    
		else :
		    #Resource File updated but not in supported formats
		 
		    resource_actions.delete_vector_storer_task(entity.as_dict())
		    
	elif isinstance(entity, model.Package):
	    
	    if entity.state==self.STATE_DELETED:
		
		resource_actions.pkg_delete_vector_storer_task(entity)