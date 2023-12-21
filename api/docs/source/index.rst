.. Explorer API documentation master file, created by
   sphinx-quickstart on Mon Jul  3 17:21:30 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Explorer API
======================================================

.. toctree::
   :maxdepth: 2

   version
   quota
   mapping
   execute

======================================================

Overview
--------

This is the documentation for the API of the explorer.

Available API endpoints are pretty limited right now. If you have ideas and
specific needs, please provide feedback `here <https://explorer.hamp.app/feedback>`_.

Base Endpoint
^^^^^^^^^^^^^

The base endpoint is ``https://explorer.hamp.app``.

Authentication
^^^^^^^^^^^^^^

The explorer API doesn't need any authentication now.

Rate limiting
^^^^^^^^^^^^^

The explorer API uses a custom quota system. Read more about it at :doc:`quota`.

In the future, there might be paid plans with higher quotas.

