=====
XChem
=====

Polls is a simple Django app to get a RESTFul API of XChem data.

Detailed documentation is in the "docs" directory.

Quick start
-----------

1. Add "polls" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'xchem',
    ]

2. Include the polls URLconf in your project urls.py like this::

    path('xchem/', include('xchem.urls')),

3. Run `python manage.py migrate` to create the polls models.

