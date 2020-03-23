"""
******
Entity
******

The Entity class is the base class for all entities, including Project, Folder, File, and Link.

Entities are dictionary-like objects in which both object and dictionary notation (entity.foo or entity['foo']) can be
used interchangeably.

Imports::

    from synapseclient import Project, Folder, File, Link

.. autoclass:: synapseclient.entity.Entity

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

Changing File Names
-------------------

A Synapse File Entity has a name separate from the name of the actual file it represents. When a file is uploaded to
Synapse, its filename is fixed, even though the name of the entity can be changed at any time. Synapse provides a way
to change this filename and the content-type of the file for future downloads by creating a new version of the file
with a modified copy of itself.  This can be done with the synapseutils.copy_functions.changeFileMetaData function.

>>> import synapseutils
>>> e = syn.get(synid)
>>> print(os.path.basename(e.path))  ## prints, e.g., "my_file.txt"
>>> e = synapseutils.changeFileMetaData(syn, e, "my_newname_file.txt")

Setting *fileNameOverride* will **not** change the name of a copy of the
file that's already downloaded into your local cache. Either rename the
local copy manually or remove it from the cache and re-download.:

>>> syn.cache.remove(e.dataFileHandleId)
>>> e = syn.get(e)
>>> print(os.path.basename(e.path))  ## prints "my_newname_file.txt"

~~~~
Link
~~~~

.. autoclass:: synapseclient.entity.Link

~~~~~~~~~~~~
Table Schema
~~~~~~~~~~~~

.. autoclass:: synapseclient.table.Schema


~~~~~~~~~~~~~~~~~~
Entity View Schema
~~~~~~~~~~~~~~~~~~

.. autoclass:: synapseclient.table.EntityViewSchema


~~~~~~~~~~~~~~~~
DockerRepository
~~~~~~~~~~~~~~~~

.. autoclass:: synapseclient.entity.DockerRepository

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Properties and annotations, implementation details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In Synapse, entities have both properties and annotations. Properties are used by the system, whereas annotations are
completely user defined. In the Python client, we try to present this situation as a normal object, with one set of
properties.

Printing an entity will show the division between properties and annotations.::

    print(entity)

Under the covers, an Entity object has two dictionaries, one for properties and one for annotations. These two
namespaces are distinct, so there is a possibility of collisions. It is recommended to avoid defining annotations with
names that collide with properties, but this is not enforced.::

    ## don't do this!
    entity.properties['description'] = 'One thing'
    entity.annotations['description'] = 'A different thing'

In case of conflict, properties will take precedence.::

    print(entity.description)
    #> One thing

Some additional ambiguity is entailed in the use of dot notation. Entity objects have their own internal properties
which are not persisted to Synapse. As in all Python objects, these properties are held in object.__dict__. For
example, this dictionary holds the keys 'properties' and 'annotations' whose values are both dictionaries themselves.

The rule, for either getting or setting is: first look in the object then look in properties, then look in annotations.
If the key is not found in any of these three, a get results in a ``KeyError`` and a set results in a new annotation
being created. Thus, the following results in a new annotation that will be persisted in Synapse::

    entity.foo = 'bar'

To create an object member variable, which will *not* be persisted in Synapse, this unfortunate notation is required::

    entity.__dict__['foo'] = 'bar'

As mentioned previously, name collisions are entirely possible.
Keys in the three namespaces can be referred to unambiguously like so::

    entity.__dict__['key']

    entity.properties.key
    entity.properties['key']

    entity.annotations.key
    entity.annotations['key']

Most of the time, users should be able to ignore these distinctions and treat Entities like normal Python objects.
End users should never need to manipulate items in __dict__.

See also:

- :py:mod:`synapseclient.annotations`

"""

import collections
import itertools
import io
import os
import inspect
import urllib.parse as urllib_parse

from synapseclient.core.models.dict_object import DictObject
from synapseclient.core.utils import id_of, itersubclasses
from synapseclient.core.exceptions import *


class Versionable(object):
    """An entity for which Synapse will store a version history."""

    _synapse_entity_type = 'org.sagebionetworks.repo.model.Versionable'
    _property_keys = ['versionNumber', 'versionLabel', 'versionComment', 'versionUrl', 'versions']


# TODO: inherit from UserDict.DictMixin?
#       http://docs.python.org/2/library/userdict.html#UserDict.DictMixin

# Alternate implementations include:
# - a naming convention to tag object members
# - keeping a list of 'transient' variables (the object members)
# - giving up on the dot notation (implemented in Entity2.py in commit e441fcf5a6963118bcf2b5286c67fc66c004f2b5 in the
# entity_object branch)
# - giving up on hiding the difference between properties and annotations

class Entity(collections.MutableMapping):
    """
    A Synapse entity is an object that has metadata, access control, and potentially a file. It can represent data,
    source code, or a folder that contains other entities.

    Entities should typically be created using the constructors for specific subclasses such as Project, Folder or File.
    """

    _synapse_entity_type = 'org.sagebionetworks.repo.model.Entity'
    _property_keys = ['id', 'name', 'description', 'parentId',
                      'entityType', 'concreteType',
                      'uri', 'etag', 'annotations', 'accessControlList',
                      'createdOn', 'createdBy', 'modifiedOn', 'modifiedBy']
    _local_keys = []

    @classmethod
    def create(cls, properties=None, annotations=None, local_state=None):
        """
        Create an Entity or a subclass given dictionaries of properties and annotations, as might be received from the
        Synapse Repository.

        :param properties:  A map of Synapse properties

            If 'concreteType' is defined in properties, we create the proper subclass of Entity. If not, give back the
            type whose constructor was called:

            If passed an Entity as input, create a new Entity using the input entity as a prototype.

        :param annotations: A map of user defined annotations
        :param local_state: Internal use only
        """

        # Create a new Entity using an existing Entity as a prototype
        if isinstance(properties, Entity):
            if annotations is None:
                annotations = {}
            if local_state is None:
                local_state = {}
            annotations.update(properties.annotations)
            local_state.update(properties.local_state())
            properties = properties.properties
            if 'id' in properties:
                del properties['id']

        if cls == Entity \
                and 'concreteType' in properties \
                and properties['concreteType'] in entity_type_to_class:
            cls = entity_type_to_class[properties['concreteType']]
        return cls(properties=properties, annotations=annotations, local_state=local_state)

    @classmethod
    def getURI(cls, id):
        return '/entity/%s' % id

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls)

        # Make really sure that properties and annotations exist before
        # any object methods get invoked. This is important because the
        # dot operator magic methods have been overridden and depend on
        # properties and annotations existing.
        obj.__dict__['properties'] = DictObject()
        obj.__dict__['annotations'] = DictObject()
        return obj

    def __init__(self, properties=None, annotations=None, local_state=None, parent=None, **kwargs):

        if properties:
            if isinstance(properties, collections.Mapping):
                if 'annotations' in properties and isinstance(properties['annotations'], collections.Mapping):
                    annotations.update(properties['annotations'])
                    del properties['annotations']
                self.__dict__['properties'].update(properties)
            else:
                raise SynapseMalformedEntityError('Unknown argument type: properties is a %s' % str(type(properties)))

        if annotations:
            if isinstance(annotations, collections.Mapping):
                self.__dict__['annotations'].update(annotations)
            elif isinstance(annotations, str):
                self.properties['annotations'] = annotations
            else:
                raise SynapseMalformedEntityError('Unknown argument type: annotations is a %s' % str(type(annotations)))

        if local_state:
            if isinstance(local_state, collections.Mapping):
                self.local_state(local_state)
            else:
                raise SynapseMalformedEntityError('Unknown argument type: local_state is a %s' % str(type(local_state)))

        for key in self.__class__._local_keys:
            if key not in self.__dict__:
                self.__dict__[key] = None

        # Extract parentId from parent
        if 'parentId' not in kwargs:
            if parent:
                try:
                    kwargs['parentId'] = id_of(parent)
                except Exception:
                    if isinstance(parent, Entity) and 'id' not in parent:
                        raise SynapseMalformedEntityError("Couldn't find 'id' of parent."
                                                          " Has it been stored in Synapse?")
                    else:
                        raise SynapseMalformedEntityError("Couldn't find 'id' of parent.")

        # Note: that this will work properly if derived classes declare their internal state variable *before* invoking
        # super(...).__init__(...)
        for key, value in kwargs.items():
            self.__setitem__(key, value)

        if 'concreteType' not in self:
            self['concreteType'] = self.__class__._synapse_entity_type

        # Only project can be top-level. All other entity types require parentId don't enforce this for generic Entity
        if 'parentId' not in self \
                and not isinstance(self, Project) \
                and not type(self) == Entity:
            raise SynapseMalformedEntityError("Entities of type %s must have a parentId." % type(self))

    def postURI(self):
        return '/entity'

    def putURI(self):
        return '/entity/%s' % self.id

    def deleteURI(self, versionNumber=None):
        if versionNumber:
            return '/entity/%s/version/%s' % (self.id, versionNumber)
        else:
            return '/entity/%s' % self.id

    def local_state(self, state=None):
        """
        Set or get the object's internal state, excluding properties, or annotations.

        :param state: A dictionary
        """
        if state:
            for key, value in state.items():
                if key not in ['annotations', 'properties']:
                    self.__dict__[key] = value
        result = {}
        for key, value in self.__dict__.items():
            if key not in ['annotations', 'properties'] and not key.startswith('__'):
                result[key] = value
        return result

    def __setattr__(self, key, value):
        return self.__setitem__(key, value)

    def __setitem__(self, key, value):
        if key in self.__dict__ or key in self.__class__._local_keys:
            # If we assign like so:
            #   entity.annotations = {'foo';123, 'bar':'bat'}
            # Wrap the dictionary in a DictObject so we can
            # later do:
            #   entity.annotations.foo = 'bar'
            if (key == 'annotations' or key == 'properties') and not isinstance(value, DictObject):
                value = DictObject(value)
            self.__dict__[key] = value
        elif key in self.__class__._property_keys:
            self.properties[key] = value
        else:
            self.annotations[key] = value

    # TODO: def __delattr__

    def __getattr__(self, key):
        # Note: that __getattr__ is only called after an attempt to
        # look the key up in the object's dictionary has failed.
        try:
            return self.__getitem__(key)
        except KeyError:
            # Note that hasattr in Python2 is more permissive than Python3
            # about what exceptions it catches. In Python3, hasattr catches
            # only AttributeError
            raise AttributeError(key)

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        elif key in self.properties:
            return self.properties[key]
        elif key in self.annotations:
            return self.annotations[key]
        else:
            raise KeyError(key)

    def __delitem__(self, key):
        if key in self.properties:
            del self.properties[key]
        elif key in self.annotations:
            del self.annotations[key]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self.keys())

    # TODO shouldn't these include local_state as well? -jcb
    def keys(self):
        """Returns a set of property and annotation keys"""
        return set(self.properties.keys()) | set(self.annotations.keys())

    def has_key(self, key):
        """Is the given key a property or annotation?"""

        return key in self.properties or key in self.annotations

    def _write_kvps(self, f, dictionary, key_filter=None, key_aliases=None):
        for key in sorted(dictionary.keys()):
            if (not key_filter) or key_filter(key):
                f.write('  ')
                f.write(str(key) if not key_aliases else key_aliases[key])
                f.write('=')
                f.write(str(dictionary[key]))
                f.write('\n')

    def __str__(self):
        f = io.StringIO()

        f.write('%s: %s (%s)\n' % (self.__class__.__name__, self.properties.get('name', 'None'),
                                   self['id'] if 'id' in self else '-',))

        self._str_localstate(f)

        f.write('properties:\n')
        self._write_kvps(f, self.properties)

        f.write('annotations:\n')
        self._write_kvps(f, self.annotations)

        return f.getvalue()

    def _str_localstate(self, f):  # type: (StringIO) -> None
        """
        Helper method for writing the string representation of the local state to a StringIO object
        :param f: a StringIO object to which the local state string will be written
        """
        self._write_kvps(f, self.__dict__,
                         lambda key: not (key in ['properties', 'annotations'] or key.startswith('__')))

    def __repr__(self):
        """Returns an eval-able representation of the Entity."""

        f = io.StringIO()
        f.write(self.__class__.__name__)
        f.write("(")
        f.write(", ".join(
            {"%s=%s" % (str(key), value.__repr__(),) for key, value in
                itertools.chain(
                    list([k_v for k_v in self.__dict__.items()
                          if not (k_v[0] in ['properties', 'annotations'] or k_v[0].startswith('__'))]),
                    self.properties.items(),
                    self.annotations.items())}))
        f.write(")")
        return f.getvalue()


class Project(Entity):
    """
    Represents a project in Synapse.

    Projects in Synapse must be uniquely named. Trying to create a project with a name that's already taken, say
    'My project', will result in an error

    :param name:            The name of the project
    :param properties:      A map of Synapse properties
    :param annotations:     A map of user defined annotations
    :param local_state:     Internal use only
    
    Example::

        project = Project('Foobarbat project')
        project = syn.store(project)
    """

    _synapse_entity_type = 'org.sagebionetworks.repo.model.Project'

    def __init__(self, name=None, properties=None, annotations=None, local_state=None, **kwargs):
        if name:
            kwargs['name'] = name
        super(Project, self).__init__(concreteType=Project._synapse_entity_type, properties=properties,
                                      annotations=annotations, local_state=local_state, **kwargs)


class Folder(Entity):
    """
    Represents a folder in Synapse.

    Folders must have a name and a parent and can optionally have annotations.
    
    :param name:            The name of the folder
    :param parent:          The parent project or folder
    :param properties:      A map of Synapse properties
    :param annotations:     A map of user defined annotations
    :param local_state:     Internal use only

    Example::

        folder = Folder('my data', parent=project)
        folder = syn.store(folder)
    """

    _synapse_entity_type = 'org.sagebionetworks.repo.model.Folder'

    def __init__(self, name=None, parent=None, properties=None, annotations=None, local_state=None, **kwargs):
        if name:
            kwargs['name'] = name
        super(Folder, self).__init__(concreteType=Folder._synapse_entity_type, properties=properties,
                                     annotations=annotations, local_state=local_state, parent=parent, **kwargs)


class Link(Entity):
    """
    Represents a link in Synapse.

    Links must have a target ID and a parent. When you do :py:func:`synapseclient.Synapse.get` on a Link object,
    the Link object is returned. If the target is desired, specify followLink=True in synapseclient.Synapse.get.

    :param targetId:        The ID of the entity to be linked
    :param targetVersion:   The version of the entity to be linked
    :param parent:          The parent project or folder
    :param properties:      A map of Synapse properties
    :param annotations:     A map of user defined annotations
    :param local_state:     Internal use only
    
    Example::

        link = Link('targetID', parent=folder)
        link = syn.store(link)
    """
    _property_keys = Entity._property_keys + ['linksTo', 'linksToClassName']
    _local_keys = Entity._local_keys
    _synapse_entity_type = 'org.sagebionetworks.repo.model.Link'

    def __init__(self, targetId=None, targetVersion=None, parent=None, properties=None, annotations=None,
                 local_state=None, **kwargs):
        if targetId is not None and targetVersion is not None:
            kwargs['linksTo'] = dict(targetId=utils.id_of(targetId), targetVersionNumber=targetVersion)
        elif targetId is not None and targetVersion is None:
            kwargs['linksTo'] = dict(targetId=utils.id_of(targetId))
        elif properties is not None and 'linksTo' in properties:
            pass
        else:
            raise SynapseMalformedEntityError("Must provide a target id")
        super(Link, self).__init__(concreteType=Link._synapse_entity_type, properties=properties,
                                   annotations=annotations, local_state=local_state, parent=parent, **kwargs)


class File(Entity, Versionable):
    """
    Represents a file in Synapse.

    When a File object is stored, the associated local file or its URL will be stored in Synapse. A File must have a
    path (or URL) and a parent.

    :param path:                Location to be represented by this File
    :param name:                Name of the file in Synapse, not to be confused with the name within the path
    :param parent:              Project or Folder where this File is stored
    :param synapseStore:        Whether the File should be uploaded or if only the path should be stored when
                                :py:func:`synapseclient.Synapse.store` is called on the File object.
                                Defaults to True (file should be uploaded)
    :param contentType:         Manually specify Content-type header, for example "application/png" or
                                "application/json; charset=UTF-8"
    :param dataFileHandleId:    Defining an existing dataFileHandleId will use the existing dataFileHandleId
                                The creator of the file must also be the owner of the dataFileHandleId to have
                                permission to store the file
    :param properties:          A map of Synapse properties
    :param annotations:         A map of user defined annotations
    :param local_state:         Internal use only
    
    Example::

        data = File('/path/to/file/data.xyz', parent=folder)
        data = syn.store(data)
    """
    # Note: externalURL technically should not be in the keys since it's only a field/member variable of
    # ExternalFileHandle, but for backwards compatibility it's included
    _file_handle_keys = ["createdOn", "id", "concreteType", "contentSize", "createdBy", "etag", "fileName",
                         "contentType", "contentMd5", "storageLocationId", 'externalURL']
    # Used for backwards compatability. The keys found below used to located in the entity's local_state
    # (i.e. __dict__).
    _file_handle_aliases = {'md5': 'contentMd5', 'externalURL': 'externalURL', 'fileSize': 'contentSize',
                            'contentType': 'contentType'}
    _file_handle_aliases_inverse = {v: k for k, v in _file_handle_aliases.items()}

    _property_keys = Entity._property_keys + Versionable._property_keys + ['dataFileHandleId']
    _local_keys = Entity._local_keys + ['path', 'cacheDir', 'files', 'synapseStore', '_file_handle']
    _synapse_entity_type = 'org.sagebionetworks.repo.model.FileEntity'

    # TODO: File(path="/path/to/file", synapseStore=True, parentId="syn101")
    def __init__(self, path=None, parent=None, synapseStore=True, properties=None,
                 annotations=None, local_state=None, **kwargs):
        if path and 'name' not in kwargs:
            kwargs['name'] = utils.guess_file_name(path)
        self.__dict__['path'] = path
        if path:
            cacheDir, basename = os.path.split(path)
            self.__dict__['cacheDir'] = cacheDir
            self.__dict__['files'] = [basename]
        else:
            self.__dict__['cacheDir'] = None
            self.__dict__['files'] = []
        self.__dict__['synapseStore'] = synapseStore

        # pop the _file_handle from local properties because it is handled differently from other local_state
        self._update_file_handle(local_state.pop('_file_handle', None) if (local_state is not None) else None)

        super(File, self).__init__(concreteType=File._synapse_entity_type, properties=properties,
                                   annotations=annotations, local_state=local_state, parent=parent, **kwargs)

    def _update_file_handle(self, file_handle_update_dict=None):
        """
        Sets the file handle
        
        Should not need to be called by users
        """

        # replace the file handle dict
        fh_dict = DictObject(file_handle_update_dict) if file_handle_update_dict is not None else DictObject()
        self.__dict__['_file_handle'] = fh_dict

        if file_handle_update_dict is not None \
                and file_handle_update_dict.get('concreteType') == "org.sagebionetworks.repo.model.file.ExternalFileHandle"\
                and urllib_parse.urlparse(file_handle_update_dict.get('externalURL')).scheme != 'sftp':
            self.__dict__['synapseStore'] = False

        # initialize all nonexistent keys to have value of None
        for key in self.__class__._file_handle_keys:
            if key not in fh_dict:
                fh_dict[key] = None

    def __setitem__(self, key, value):
        if key == '_file_handle':
            self._update_file_handle(value)
        elif key in self.__class__._file_handle_aliases:
            self._file_handle[self.__class__._file_handle_aliases[key]] = value
        else:
            def expand_and_convert_to_URL(path): return utils.as_url(os.path.expandvars(os.path.expanduser(path)))
            # hacky solution to allowing immediate switching into a ExternalFileHandle pointing to the current path
            # yes, there is boolean zen but I feel like it is easier to read/understand this way
            if key == 'synapseStore' and value is False and self['synapseStore'] is True \
                    and utils.caller_module_name(inspect.currentframe()) != 'client':
                self['externalURL'] = expand_and_convert_to_URL(self['path'])

            # hacky solution because we historically allowed modifying 'path' to indicate wanting to change to a new
            # ExternalFileHandle
            # don't change exernalURL if it's just the synapseclient setting metadata after a function call such as
            # syn.get()
            if key == 'path' and not self['synapseStore'] \
                    and utils.caller_module_name(inspect.currentframe()) != 'client':
                self['externalURL'] = expand_and_convert_to_URL(value)
                self['contentMd5'] = None
                self['contentSize'] = None
            super(File, self).__setitem__(key, value)

    def __getitem__(self, item):
        if item in self.__class__._file_handle_aliases:
            return self._file_handle[self.__class__._file_handle_aliases[item]]
        else:
            return super(File, self).__getitem__(item)

    def _str_localstate(self, f):
        self._write_kvps(f, self._file_handle, lambda key: key in ['externalURL', 'contentMd5', 'contentSize',
                                                                   'contentType'], self._file_handle_aliases_inverse)
        self._write_kvps(f, self.__dict__, lambda key: not (key in ['properties', 'annotations', '_file_handle']
                                                            or key.startswith('__')))


class DockerRepository(Entity):
    """
    A Docker repository is a lightweight virtual machine image.
    
    NOTE: store()-ing a DockerRepository created in the Python client will always result in it being treated as a 
    reference to an external Docker repository that is not managed by synapse. 
    To upload a docker image that is managed by Synapse please use the official Docker client and read
     http://docs.synapse.org/articles/docker.html for instructions on uploading a Docker Image to Synapse
    
    :param repositoryName: the name of the Docker Repository. Usually in the format: [host[:port]/]path.
     If host is not set, it will default to that of DockerHub. port can only be specified if the host is also specified.
    :param parent: the parent project for the Docker repository
    :param properties:      A map of Synapse properties
    :param annotations:     A map of user defined annotations
    :param local_state:     Internal use only
    
    :return:  an object of type :py:class:`synapseclient.entity.DockerRepository`
    """
    _synapse_entity_type = 'org.sagebionetworks.repo.model.docker.DockerRepository'

    _property_keys = Entity._property_keys + ['repositoryName']

    def __init__(self, repositoryName=None, parent=None, properties=None, annotations=None, local_state=None, **kwargs):
        if repositoryName:
            kwargs['repositoryName'] = repositoryName
        super(DockerRepository, self).__init__(properties=properties, annotations=annotations, local_state=local_state,
                                               parent=parent, **kwargs)
        if 'repositoryName' not in self:
            raise SynapseMalformedEntityError("DockerRepository must have a repositoryName.")


# Create a mapping from Synapse class (as a string) to the equivalent Python class.
entity_type_to_class = {}
for cls in itersubclasses(Entity):
    entity_type_to_class[cls._synapse_entity_type] = cls

_entity_types = ["project", "folder", "file", "table", "link", "entityview", "dockerrepo"]


def split_entity_namespaces(entity):
    """
    Given a plain dictionary or an Entity object, splits the object into properties, annotations and local state.
    A dictionary will be processed as a specific type of Entity if it has a valid 'concreteType' field, otherwise it is
    treated as a generic Entity.

    :returns: a 3-tuple (properties, annotations, local_state).
    """
    if isinstance(entity, Entity):
        # Defensive programming: return copies
        return entity.properties.copy(), entity.annotations.copy(), entity.local_state()

    if not isinstance(entity, collections.Mapping):
        raise SynapseMalformedEntityError("Can't split a %s object." % entity.__class__.__name__)

    if 'concreteType' in entity and entity['concreteType'] in entity_type_to_class:
        entity_class = entity_type_to_class[entity['concreteType']]
    else:
        entity_class = Entity

    properties = DictObject()
    annotations = DictObject()
    local_state = DictObject()

    property_keys = entity_class._property_keys
    local_keys = entity_class._local_keys
    for key, value in entity.items():
        if key in property_keys:
            properties[key] = value
        elif key in local_keys:
            local_state[key] = value
        else:
            annotations[key] = value

    return properties, annotations, local_state


ENTITY_TYPES = [
    'org.sagebionetworks.repo.model.FileEntity',
    'org.sagebionetworks.repo.model.Folder',
    'org.sagebionetworks.repo.model.Link',
    'org.sagebionetworks.repo.model.Project',
    'org.sagebionetworks.repo.model.table.TableEntity'
]


def is_synapse_entity(entity):
    if isinstance(entity, Entity):
        return True
    if isinstance(entity, collections.Mapping):
        return entity.get('concreteType', None) in ENTITY_TYPES
    return False


def is_versionable(entity):
    """Return True if the given entity's concreteType is one that is Versionable."""

    if isinstance(entity, Versionable):
        return True

    try:
        entity_class = entity_type_to_class[entity['concreteType']]
        return issubclass(entity_class, Versionable)
    except (KeyError, TypeError):
        # the dict input is not an entity
        raise ValueError("Input is not an entity.")


def is_container(entity):
    """Test if an entity is a container (ie, a Project or a Folder)"""
    if 'concreteType' in entity:
        concreteType = entity['concreteType']
    elif 'type' in entity:
        concreteType = entity['type']
    elif isinstance(entity, collections.Mapping):
        prefix = utils.extract_prefix(entity.keys())
        if prefix+'concreteType' in entity:
            concreteType = entity[prefix+'concreteType'][0]
        elif prefix+'nodeType' in entity:
            return entity[prefix+'nodeType'] in ['project', 'folder']
        else:
            return False
    else:
        return False
    return concreteType in (Project._synapse_entity_type, Folder._synapse_entity_type)

