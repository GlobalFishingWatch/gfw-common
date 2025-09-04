.. gfw-common documentation master file, created by
   sphinx-quickstart on Tue Sep  2 16:22:36 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

:html_theme.sidebar_secondary.remove: true

Welcome to gfw-common’s documentation!
======================================

The **gfw-common** package provides shared utilities and abstractions used across
Global Fishing Watch (GFW) projects. It is designed as a lightweight library of
reusable components that simplify working with data pipelines, command-line
interfaces, and integrations with external systems.

This package follows a **"common code, single source"** principle: functionality
that is used by more than one service is factored into gfw-common, ensuring
consistency, easier maintenance, and reduced duplication.

**Date**: |today| **Version**: |version|

.. toctree::
   :maxdepth: 1
   :hidden:

   User Guide <tutorial/index>
   API reference <reference/index>
   Release notes <release>


.. grid:: 1 1 2 2
    :gutter: 2 3 4 4

    .. grid-item-card::
        :img-top: _static/index_user_guide.svg
        :text-align: center

        **User guide**
        ^^^

        The user guide provides useful examples on how to use the subpackages.

        +++

        .. button-ref:: user_guide
            :color: secondary
            :click-parent:

            To the user guide

    .. grid-item-card::
        :img-top: _static/index_api.svg
        :text-align: center

        **API reference**
        ^^^

        The reference guide contains a detailed description of the GFW Common API.

        +++

        .. button-ref:: api
            :color: secondary
            :click-parent:

            To the reference guide
