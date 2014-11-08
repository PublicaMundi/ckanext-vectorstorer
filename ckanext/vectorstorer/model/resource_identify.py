from logging import getLogger
from sqlalchemy import types, Column, Table
from ckan.lib.base import config
from ckan import model
from ckan.model import Session
from ckan.model import meta
from ckan.model.domain_object import DomainObject

from ckanext.vectorstorer import settings
from ckan.lib.celery_app import celery
log = getLogger(__name__)

resource_identify_table = None

class TaskNotReady(Exception):
    pass

def setup():
 
    if resource_identify_table is None:
        define_resource_identify_table()
 
	if not resource_identify_table.exists():
	    log.debug('Table resource_identification does not exist')
	    resource_identify_table.create()
	    log.debug('Table resource_identification was created')

class ResourceIdentify(DomainObject):
    celery_task_id = None
    resource_id = None
    
    def __init__(self, celery_task_id = None , resource_id = None):
	
        self.celery_task_id = celery_task_id
        self.resource_id = resource_id
	    
        
    def get_task_result(self):
	result = celery.AsyncResult(self.celery_task_id)
	if result.ready():
	    return result.get()
	else:
	    raise TaskNotReady
      

    
      
def define_resource_identify_table():

    global resource_identify_table


    resource_identify_table = Table('resource_identification', meta.metadata,
                    Column('resource_id', types.UnicodeText, primary_key=True),
                   Column('celery_task_id', types.UnicodeText))


    meta.mapper(ResourceIdentify, resource_identify_table)




