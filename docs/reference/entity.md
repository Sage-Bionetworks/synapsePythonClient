#Entity
The Entity class is the base class for all entities, including Project, Folder, File, and Link.

Entities are dictionary-like objects in which both object and dictionary notation (`entity.foo` or `entity['foo']`) can be
used interchangeably.

!!! info "v5.0.0 uses the object-oriented models"
    This page documents the legacy entity base classes. New code should use the
    object-oriented models such as [Project][project-reference-sync],
    [Folder][folder-reference-sync], [File][file-reference-sync], and
    [Link][link-sync].

::: synapseclient.entity.Entity
::: synapseclient.entity.Versionable
