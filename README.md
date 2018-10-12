[![Build Status](https://travis-ci.org/xchem/pipeline.svg?branch=master)](https://travis-ci.org/xchem/pipeline)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/xchem/pipeline.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/xchem/pipeline/context:python)


XChemDB
=====

This is a simple Django app to get a RESTFul API of XChem data.

Detailed documentation is in the "docs" directory.

Quick start
-----------

1. Add "xchem_db" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'xchem_db',
    ]

2. Run `python manage.py migrate` to create the xchem_db models.

