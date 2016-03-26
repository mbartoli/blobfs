blobfs
======
FUSE-based file system backed by Microsoft Azure's blob storage

1. `Examples & Usage <#examples--usage>`_
2. `Installation <#installation-setup>`_

|Python Version| |License Type|

Examples & Usage
================
Start blobfs where ``mnt`` is the folder where you want to mount Azure

.. code:: bash

    python blobfs.py mnt 

Installation & Setup
====================

1. Install the dependencies via pip

.. code:: bash 

    pip install azure-storage

2. Add your storage account name and key to ``config.py``

3. Test Azure connectivity 

.. code:: bash 

    python -W ignore verify-azure.py

----

.. |Python Version| image:: https://img.shields.io/badge/python-2.7-yellow.svg
    :target: https://www.python.org/

.. |License Type| image:: https://img.shields.io/badge/license-APLv2-blue.svg
    :target: https://github.com/mbartoli/blobfs/blob/master/LICENSE
