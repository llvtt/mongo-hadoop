``pymongo-spark`` integrates `PyMongo`_, the Python driver for MongoDB, with
`PySpark`_, the Python front-end for `Apache Spark`_. This package relies
*heavily* on the `mongo-hadoop`_ project, which must be installed prior to
using this package.

.. _PyMongo: https://pypi.python.org/pypi/pymongo
.. _PySpark: http://spark.apache.org/docs/latest/api/python/pyspark.html
.. _Apache Spark: https://spark.apache.org
.. _mongo-hadoop: https://github.com/mongodb/mongo-hadoop

Installation
------------

The easiest way to install pymongo-spark is with `pip
<https://pypi.python.org/pypi/pip>`__::

   pip install pymongo-spark

You'll also need to put the ``mongo-hadoop-spark.jar`` from the `mongo-hadoop`_
project somewhere on Spark's ``CLASSPATH`` prior to using this package.

Usage
-----

``pymongo-spark`` works by monkey-patching PySpark's RDD and SparkContext
classes. All you need to do is call the ``activate()`` function in
``pymongo-spark``::

   import pymongo_spark
   pymongo_spark.activate()

   # You are now ready to use BSON and MongoDB with PySpark.

**Make sure to set the appropriate options to put mongo-hadoop-spark.jar on
Spark's CLASSPATH**. For example::

   bin/pyspark --jars mongo-hadoop-spark.jar \
               --driver-class-path mongo-hadoop-spark.jar

You might also need to add ``pymongo-spark`` and/or ``PyMongo`` to Spark's
``PYTHONPATH`` explicitly::

   bin/pyspark --py-files /path/to/pymongo_spark.py,/path/to/pymongo.egg

Examples
--------

Read from MongoDB
.................

::

   >>> mongo_rdd = sc.mongoRDD('mongodb://localhost:27017/db.collection')
   >>> print(mongo_rdd.first())
   {u'_id': ObjectId('55cd069c6e32abacca39da2b'),
    u'hello': u'from MongoDB!'}

Write to MongoDB
................

::

   >>> some_rdd.saveToMongoDB('mongodb://localhost:27017/db.output_collection')

Reading from a BSON File
........................

::

   >>> file_path = 'my_bson_files/dump.bson'
   >>> rdd = sc.BSONFileRDD(file_path)
   >>> rdd.first()
   {u'_id': ObjectId('55cd071e6e32abacca39da2c'),
    u'hello': u'from BSON!'}

Write to a BSON File
....................

::

   >>> some_rdd.saveToBSON('my_bson_files/output')
