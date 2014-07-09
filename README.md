CKAN Vector Storer Extension
=======================


Overview
--------
Vector Storer is a CKAN extension that allows users to upload vector geospatial data, store and publish through OGC services.



Installation
------------
**1.  Extension installation**

    $ pip install -e git+http://github.com/PublicaMundi/ckanext-vectorstorer.git#egg=ckanext-vectorstorer
    $ pip install -r ./pyenv/src/ckan/requirements.txt

**2.  Install required packages**

    $ sudo apt-get install unzip unrar p7zip-full

Other Requirements:

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
        ckanext-vectorstorer.geoserver_ckan_datastore=(e.g. ckan_datastore_default)

  Geoserver workspace and datastore have to be created in advance. The datastore must be connected to the CKAN datastore database.

**3.  Datastore configuration**

  Enable the postgis extension in the Datastorer database
