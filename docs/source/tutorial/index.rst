.. _user_guide:

*********************
GFW Common User Guide
*********************

.. currentmodule:: gfw.common

.. sectionauthor:: Tomás J. Link

The Global Fishing Watch (GFW) common package provides reusable utilities, abstractions,
and helpers for data processing, validation, and orchestration across GFW pipelines and projects.

Installation
------------

You can install the ``gfw-common`` package directly from PyPI:

.. code-block:: bash

   pip install gfw-common

Once installed, you can import it in your Python code:

.. code-block:: python

   from gfw.common.version import __version__

   # Example: check the package version
   print(__version__)

Subpackages and User Guides
---------------------------

GFW Common more complex functionality is organized into subpackages.
These are summarized in the following table, with
their user guide linked in the `Description and User Guide` column (if available):

==================    ========================================
Subpackage            Description and User Guide
==================    ========================================
``beam``              :doc:`./beam`
``bigquery``          BigQuery utilities.
``cli``               CLI framework.
``pipeline``          :doc:`./pipeline`
==================    ========================================

.. toctree::
   :caption: User guide
   :maxdepth: 1
   :hidden:

   beam
   bigquery
   cli
   pipeline

Modules
-------

Simpler and miscleneous functionality is organized into modules of the main package.
Go directly to the :ref:`API Reference <api>` to check available modules.
