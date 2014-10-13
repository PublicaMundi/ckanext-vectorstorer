from setuptools import setup, find_packages
import sys, os

version = '1.0'

setup(
	name='ckanext-vectorstorer',
	version=version,
	description="",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='',
	author_email='',
	url='',
	license='',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.vectorstorer'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
	],
	entry_points=\
	"""
        [ckan.plugins]
	 
	vectorstorer=ckanext.vectorstorer.plugin:VectorStorer
	
	[ckan.celery_task]
	tasks = ckanext.vectorstorer.celery_import:task_imports
	""",
)
