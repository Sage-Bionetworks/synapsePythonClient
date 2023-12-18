# Properties and annotations, implementation details

In Synapse, entities have both properties and annotations. Properties are used by the system, whereas annotations are completely user defined. In the Python client, we try to present this situation as a normal object, with one set of properties.

Printing an entity will show the division between properties and annotations.

```python
print(entity)
```

Under the covers, an Entity object has two dictionaries, one for properties and one for annotations. These two namespaces are distinct, so there is a possibility of collisions. It is recommended to avoid defining annotations with names that collide with properties, but this is not enforced.

```python
## don't do this!
entity.properties['description'] = 'One thing'
entity.annotations['description'] = 'A different thing'
```

In case of conflict, properties will take precedence.

```python
print(entity.description)
#> One thing
```

Some additional ambiguity is entailed in the use of dot notation. Entity objects have their own internal properties which are not persisted to Synapse. As in all Python objects, these properties are held in object.__dict__. For example, this dictionary holds the keys 'properties' and 'annotations' whose values are both dictionaries themselves.

The rule, for either getting or setting is: first look in the object then look in properties, then look in annotations. If the key is not found in any of these three, a get results in a `KeyError` and a set results in a new annotation being created. Thus, the following results in a new annotation that will be persisted in Synapse:

```python
entity.foo = 'bar'
```

To create an object member variable, which will *not* be persisted in Synapse, this unfortunate notation is required:

```python
entity.__dict__['foo'] = 'bar'
```

As mentioned previously, name collisions are entirely possible. Keys in the three namespaces can be referred to unambiguously like so:

```python
entity.__dict__['key']

entity.properties.key
entity.properties['key']

entity.annotations.key
entity.annotations['key']
```

Most of the time, users should be able to ignore these distinctions and treat Entities like normal Python objects. End users should never need to manipulate items in __dict__.

See also:

- [synapseclient.annotations][]
