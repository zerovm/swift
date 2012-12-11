=============
Object Append
=============

--------
Overview
--------

Swift allows users to append data to objects. Data can be appended to any object anytime.
You can do as many appends as you want. Append is implemented by sending ``X-Append-To`` header
in **POST** request and setting the data to be appended as a **POST** request body.
On the filesystem level appending data involves only file append operation, the old data is never read
or otherwise touched. The md5 checksum is maintained *online* for this purpose, you can always add
more chunks and still get a correct checksum each time.

---------
Revisions
---------

To make append manageable a concept of *Revisions* was introduced. Each revision is a running
positive number, starting from **0** for the original un-appended object,
and incremented by **1** for each appended data chunk.
Each revision indicates that a chunk of data was appended to the end of an object
(essentially to the end of previous revision data).
Each revision therefore represents a state of the whole object after the append.
Revision number is returned in ``X-Revision`` header inside **GET** or **HEAD** response.
You will get ``X-Revision: 0`` for original object  ``X-Revision: 1`` when appended once
``X-Revision: 2`` for second append and so on. Only a successful **POST** request
with correct ``X-Append-To`` header will increment revision number.
To get ``X-Revision`` header in response you MUST set it in **GET** or **HEAD** request.
If you set ``X-Revision`` to any integer >= 0 you will either get the object at that revision
or an HTTP 404 error if that revision does not exist.
In the ``X-Append-To`` header you will need to set a current revision number (it's 0 for original object,
so it will be ``X-Append-To: 0`` for first append operation). After successful append the revision number
will increment (``X-Revision: 1`` for the case above, and ``X-Append-To: 1`` if you need to append another chunk)
There is a special meaning to ``X-Revision: -1`` or any other *negative* number, it means
"get me the newest revision", i.e. the latest object revision after applying all appends.
This in turn means that you can get a total number of revisions by setting ``X-Revision: -1``
in a request and adding 1 to the ``X-Revision`` number in response.

---------
Appending
---------

To append data to an object you **must** send a **POST** request with ``X-Append-To`` header set.
If you set ``X-Append-To`` to any integer >=0 the data will be appended to that revision.
If that exact revision is not the latest one you will get an HTTP 400 error.
If you send ``X-Append-To: -1`` the data will be appended to the latest revision.
Therefore, there are two modes of operation for append:

:1: Client and server are synchronized, client sends exact revision with each update to make
    sure that the data is appended in a correct order. If client gets out of sync with server
    it will receive HTTP 400 and needs to recover from it. Example for this mode of operation
    would be sending tar archive updates for differential backup.

:2: Client does not care what revision is server at, it just needs to append the data to the end.
    Client will send all its updates with revision -1 and they will never fail. This is an example
    for logging application that writes log chunks to some object.

If you perform a **PUT** on the existing appended object, the object will be overwritten with the new data
from the request body. Revision number will reset to 0 for this object. ``X-Append-To`` is ignored on **PUT**.

.. attention::

    Do not try to append from several clients in parallel without synchronizing between them.
    Trying ``X-Append-To: -1`` in parallel will lead to *undefined behavior*.
    It's a good practice to append from only one client at a time.

--------
Metadata
--------

Append operation will preserve metadata. Append operations will treat the data chunk as
``Content-Type: application/octet-stream``. The original ``Content-Type``
(the one that you have set when creating this object) will retain as-is on the object.
If you **POST** a new metadata and ``post_as_copy = true`` is set in the config file
the object will be copied as a whole, using the latest revision, and all other revision
information will be dropped. It will be essentially equal to: **GET** latest revision object,
**PUT** it as a new object.

----------
Expiration
----------

If you set expiration for the object at the creation time it will be retained after append.
You can set different expiration date by setting ``X-Delete-After`` header when appending,
this will reset the expiration date to the new value.

-----------
Replication
-----------

Replication of appended objects will retain the extended attributes and hence all the revision data.
Objects will be replicated based on last modification time, which means that the newest object will be
replicated to all other nodes regardless of the revision data that exists there or on any other replica
of the same object.

--------
Examples
--------

See below::

    # First, upload the file
    curl -X PUT -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject --data-binary 'The quick'

    # Next, append to file
    curl -X POST -H 'X-Auth-Token: <token>' \
        -H 'X-Append-To: 0' \
        http://<storage_url>/container/myobject --data-binary ' brown fox'

    # Append some more
    curl -X POST -H 'X-Auth-Token: <token>' \
        -H 'X-Append-To: -1' \
        http://<storage_url>/container/myobject --data-binary ' jumps over'

    # And even more
    curl -X POST -H 'X-Auth-Token: <token>' \
        -H 'X-Append-To: 2' \
        http://<storage_url>/container/myobject --data-binary ' the lazy dog'

    # Now, let's read the whole object
    curl -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject

    # Another way to read it as a whole
    curl -H 'X-Auth-Token: <token>' \
        -H 'X-Revision: -1' \
        http://<storage_url>/container/myobject

    # Now we'll read only "The quick brown fox"
    curl -H 'X-Auth-Token: <token>' \
        -H 'X-Revision: 1' \
        http://<storage_url>/container/myobject


----------------
Additional Notes
----------------

* Storing revision data requires some space in the extended attributes extents. Each revision has an overhead
  of 116 bytes of data and each 32 revisions or part of them require additional 256 bytes
  in the extended attributes storage space. This means that append feature will be much more usable
  on xfs filesystem (it supports virtually unlimited number of extended attributes), than on most other fs
  (ext3/4, reiserfs) where extended attributes are limited to one filesystem block each.

* Looking up extended attribute data incurs overhead in any filesystem, it will lead to slightly longer response times.
  Issuing a **GET** without ``X-Revision`` will not lead to attribute lookup, and won't increase latency.