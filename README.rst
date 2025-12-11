DVX
===

**Minimal Data Version Control** - content-addressable storage for data files.

DVX is a fork of `DVC`_ focused on the core data versioning functionality:
version large files alongside your Git repository, store them in remote storage,
and share them with your team.

|CI| |Python Version| |PyPI|

Features
--------

- **Version data files**: Track large files with small `.dvc` metafiles in Git
- **Content-addressable storage**: Files are hashed and deduplicated automatically
- **Remote storage**: Push/pull data to S3, GCS, Azure, SSH, and more
- **Parallel pipelines**: Execute ``dvc.yaml`` stages in parallel with ``dvx run``
- **Git integration**: Works seamlessly with your existing Git workflow

What's Different from DVC?
--------------------------

DVX removes DVC's ML-specific features to provide a simpler, focused tool:

**Removed:**

- Experiments (``dvc exp``, metrics, params, plots)
- Studio integration and telemetry

**Simplified:**

- ``dvx run`` replaces ``dvc repro`` with parallel execution by default

**Retained:**

- ``dvx add`` / ``dvx remove`` - Track/untrack files
- ``dvx push`` / ``dvx pull`` / ``dvx fetch`` - Sync with remote storage
- ``dvx checkout`` - Restore files from cache
- ``dvx status`` / ``dvx diff`` - Check file status
- ``dvx gc`` - Garbage collect unused cache
- ``dvx remote`` / ``dvx config`` - Configure remotes and settings
- ``dvx import`` / ``dvx get`` - Import files from external repos
- ``dvx run`` - Execute pipeline stages in parallel

Quick Start
-----------

.. code-block:: bash

   # Install
   pip install dvx

   # Initialize in a Git repo
   dvx init

   # Track a large file
   dvx add data/dataset.csv

   # Commit the .dvc file to Git
   git add data/dataset.csv.dvc data/.gitignore
   git commit -m "Track dataset"

   # Configure remote storage
   dvx remote add -d myremote s3://mybucket/data

   # Push data to remote
   dvx push

Pipeline Execution
------------------

DVX can execute ``dvc.yaml`` pipelines with parallel stage execution:

.. code-block:: bash

   # Run all stages (parallel by default)
   dvx run

   # Limit to 4 parallel workers
   dvx run -j 4

   # Show execution plan without running
   dvx run --dry-run

   # Run specific stage and its dependencies
   dvx run process_data

   # Export DAG visualization
   dvx run --dot pipeline.dot

Installation
------------

.. code-block:: bash

   pip install dvx

For specific remote backends:

.. code-block:: bash

   pip install 'dvx[s3]'      # Amazon S3
   pip install 'dvx[gs]'      # Google Cloud Storage
   pip install 'dvx[azure]'   # Azure Blob Storage
   pip install 'dvx[ssh]'     # SSH/SFTP
   pip install 'dvx[all]'     # All backends

License
-------

Apache 2.0 - see LICENSE file.

DVX is a fork of `DVC`_ by `Iterative`_.

.. _DVC: https://github.com/iterative/dvc
.. _Iterative: https://iterative.ai

.. |CI| image:: https://github.com/runsascoded/dvx/actions/workflows/tests.yaml/badge.svg
   :target: https://github.com/runsascoded/dvx/actions/workflows/tests.yaml
   :alt: CI

.. |Python Version| image:: https://img.shields.io/pypi/pyversions/dvx
   :target: https://pypi.org/project/dvx
   :alt: Python Version

.. |PyPI| image:: https://img.shields.io/pypi/v/dvx.svg?label=pip&logo=PyPI&logoColor=white
   :target: https://pypi.org/project/dvx
   :alt: PyPI
