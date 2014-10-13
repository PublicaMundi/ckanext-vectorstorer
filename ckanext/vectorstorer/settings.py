from pylons import config

import sys
sys.path.append(config.get('ckanext-vectorstorer.gdal_folder', '/usr/lib/python2.7/dist-packages'))
from osgeo import ogr, osr



db_encoding='utf-8'
TMP_FOLDER='/tmp/vectorstorer/'

SUPPORTED_DATA_FORMATS = [ 
    'zip',
    'rar',
    'tar',
    'gz',
    '7z',
    'kml',
    'gml',
    'gpx',
    'csv',
    'geojson',
    'sqlite',
    'geopackage',
    'gpkg',
    'db_table'
]

ARCHIVE_FORMATS=[
    'zip',
    'rar',
    'tar',
    'gz',
    '7z'
]

WMS_VECTORSTORER_RESOURCE=u'vectorstorer_wms'
DB_TABLE_RESOURCE=u'vectorstorer_db'
WMS_FORMAT=u'wms'
DB_TABLE_FORMAT=u'db_table'