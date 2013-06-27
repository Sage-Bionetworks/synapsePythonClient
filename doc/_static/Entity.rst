**************
Synapse Entity
**************

The Entity class is the base class for all entities. It has a few
special characteristics. It is a dictionary-like object in which
both object and dictionary notation (entity.foo or entity['foo'])
can be used interchangeably.

In Synapse, entities have both properties and annotations. This has
come to be viewed as awkward, so we try to hide it. Furthermore,
because we're getting tricky with the dot notation, there are three
distinct namespaces to consider when accessing variables that are
part of the entity: the members of the object, properties defined by
Synapse, and Synapse annotations, which are open-ended and user-
defined.

The rule, for either getting or setting is: first look in the object
then look in properties, then look in annotations. If the key is not
found in any of these three, a get results in a ``KeyError`` and a set
results in a new annotation being created. Thus, the following results
in a new annotation that will be persisted in Synapse::

    entity.foo = 'bar'

To create an object member variable, which will *not* be persisted in
Synapse, this unfortunate notation is required::

    entity.__dict__['foo'] = 'bar'

Between the three namespaces, name collisions are entirely possible.
Keys in the three namespaces can be referred to unambiguously like so::

    entity.__dict__['key']
    
    entity.properties.key
    entity.properties['key']
    
    entity.annotations.key
    entity.annotations['key']

Alternate implementations include:
- a naming convention to tag object members
- keeping a list of 'transient' variables (the object members)
- giving up on the dot notation (implemented in Entity2.py in commit e441fcf5a6963118bcf2b5286c67fc66c004f2b5 in the entity_object branch)
- giving up on hiding the difference between properties and annotations

.. autoclass:: synapseclient.entity.Entity
   :members:
   
.. automethod:: synapseclient.entity.is_locationable
.. automethod:: synapseclient.entity.is_versionable
.. automethod:: synapseclient.entity.split_entity_namespaces
   
~~~~~~~
Project
~~~~~~~

.. autoclass:: synapseclient.entity.Project

~~~~~~
Folder
~~~~~~

.. autoclass:: synapseclient.entity.Folder

~~~~
File
~~~~

.. autoclass:: synapseclient.entity.File