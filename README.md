CKAN Vector Storer Extension
=======================


Overview
--------
Vector Storer is a CKAN extension that allows users to upload vector geospatial data, store and publish through OGC services.



Installation
------------

    $ pip install -e git+http://github.com/PublicaMundi/ckanext-vectorstorer.git#egg=ckanext-vectorstorer

Requirements:

    * Python-GDAL


Configuration
-------------

**1.  Enabling Vector Storer**

  To enable the Vector Storer Extension add this to ckan plugins in the config file :
 
        ckan.plugins = vectorstorer

    
**2.  Vector Storer config options**

  The following should be set in the CKAN config :

        #ckanext-vectorstorer Settings
        ckanext-vectorstorer.geoserver_url= (e.g. http://ckan_services_server/geoserver)
        ckanext-vectorstorer.geoserver_workspace= (e.g. CKAN)
        ckanext-vectorstorer.geoserver_admin= (e.g. admin)
        ckanext-vectorstorer.geoserver_password= (e.g. geoserver)
