import os
import zipfile
import uuid
import codecs

from pylons import config, Response
from ckan.lib.base import BaseController, c, g, request, \
    response, session, render, config, abort

from ckan.logic import get_action ,check_access, model,NotFound,NotAuthorized
from ckanext.vectorstorer import settings
from ckanext.vectorstorer import resource_actions
import json
import urllib2
import ckan
from ckan.common import _
from ckanext.vectorstorer.settings import osr

_check_access = check_access
  
class VectorController(BaseController):
    '''VectorController will be used to publish vector data at postgis and geoserver'''
    
    def publish(self):
	resource_id = request.params.get('resource_id',u'')
	self._get_context(resource_id)
	
	
        encoding = self._get_encoding()
        projection = self._get_projection()
        geometry_column = request.params.get('geometry_column',u'')
        layer_idxs = self._get_selected_layers()
        
        
        
       
	    
        resource=ckan.model.Session.query(model.Resource).get(resource_id)
        extra_params={
		      "encoding":encoding,
		      "projection":projection,
		      "geometry_column":geometry_column,
		      "layer_idxs":layer_idxs
		      }
        resource_actions.create_vector_storer_task(resource,extra_params)
        return "OK"
        
    def _get_context(self,resource_id):
	context = {'model': model, 'session': model.Session,
                   'user': c.user}
	  
        try:
	    _check_access('resource_update',context, {'id':resource_id })
            resource = get_action('resource_show')(context,
                                                     {'id': resource_id})
	    return (resource)
            
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Unauthorized to read resource %s') % resource_id)
            
            
    def _get_encoding(self):
	_encoding=request.params.get('encoding',u'utf-8')
	if len(_encoding)==0:
	    _encoding=u'utf-8'
	    return _encoding
	else:
	    if self._encoding_exists(_encoding):
		return _encoding
	    else:
		abort(400, _('Bad Encoding : %s') % _encoding)
	      
    def _encoding_exists(self, encoding):
	try:
	    codecs.lookup(encoding)
	except LookupError:
	    return False
	return True
      
    def _get_projection(self):
	try:

	    proj_param=request.params.get('projection',u'')
	    _projection=int(proj_param)
	    _spatial_ref = osr.SpatialReference()
	    _spatial_ref.ImportFromEPSG(_projection)
	   
	except ValueError:
	    abort(400, _('Bad EPSG code : %s') % proj_param)
	except RuntimeError:
	    abort(400, _('Bad EPSG code : %s') % proj_param)
	except OverflowError:
	    abort(400, _('Bad EPSG code : %s') % proj_param)
	
    def _get_selected_layers(self):
	_selected_layers = request.params.get('layers',u'*')
	if len(_selected_layers)==0:
	    _selected_layers=u'*'
	    return _selected_layers
	else:
	    for let in _selected_layers:
		if not let.isnumeric() and not let in [',','*']:
		    abort(400, _('Not Valid character : %s') % let)
	    return _selected_layers
