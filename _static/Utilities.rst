*************************
Synapse Utility Functions
*************************
TODO_Sphinx (There's probably some way to generally describe these functions)
   
~~~~~~~~~~~~~
File Handling
~~~~~~~~~~~~~

.. automethod:: synapseclient.utils.md5_for_file
.. automethod:: synapseclient.utils.download_file
.. automethod:: synapseclient.utils.extract_filename
.. automethod:: synapseclient.utils.file_url_to_path
.. automethod:: synapseclient.utils.normalize_whitespace

~~~~~~~~~~~~~~~~~
Property Juggling
~~~~~~~~~~~~~~~~~

.. automethod:: synapseclient.utils.guess_object_type
.. automethod:: synapseclient.utils.id_of
.. automethod:: synapseclient.utils.class_of
.. automethod:: synapseclient.utils.get_properties
.. automethod:: synapseclient.utils.get_entity_type
.. automethod:: synapseclient.utils.is_url
.. automethod:: synapseclient.utils.as_url
.. automethod:: synapseclient.utils.is_synapse_entity
.. automethod:: synapseclient.utils.is_synapse_id
.. automethod:: synapseclient.utils.to_unix_epoch_time
.. automethod:: synapseclient.utils.from_unix_epoch_time
.. automethod:: synapseclient.utils.format_time_interval

~~~~~~~~
Chunking
~~~~~~~~

.. autoclass:: synapseclient.utils.Chunk
.. automethod:: synapseclient.utils.chunks
   
~~~~~~~
Testing
~~~~~~~

.. automethod:: synapseclient.utils.make_bogus_data_file
.. automethod:: synapseclient.utils.make_bogus_binary_file
.. automethod:: synapseclient.utils.synapse_error_msg
.. automethod:: synapseclient.utils.debug_response
.. automethod:: synapseclient.version_check.version_check
   
~~~~~~~~~~~~~~
I have no idea
~~~~~~~~~~~~~~

.. automethod:: synapseclient.utils.itersubclasses